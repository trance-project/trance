package uk.ac.ox.cs.trance

import framework.common._
import framework.plans.NRCTranslator
import org.apache.spark.sql.types.{ArrayType => SparkArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, types}

import scala.collection.immutable.{Map => IMap}

object NRCConverter extends NRCTranslator {

  /**
   * This function recursively translates the intermediary Rep object created by [[Wrapper]] into an NRC expression.
   * The rep will enter this function as a nested structure of operations that contains a Wrapper object at its core.
   * We recursively unnest these layers of nesting, building the NRC expression as we go.
   * The innermost layer will be the [[Wrapper]] containing a Dataframe which is represented as a BagVarRef in NRC.
   */
  def toNRC(rep: Rep, env: IMap[Rep, Any]): Expr = rep match {
    //TODO - Map
    //case Map() =>
    //
    //ForEachUnion(tvr, expr.asBag, expr2.asTuple)
    //TODO - Filter
    //Case Filter()
    case FlatMap(e, Fun(in, out)) =>
      val expr1 = toNRC(e, env)
      val tvr = TupleVarRef(utilities.Symbol.fresh(), expr1.asInstanceOf[BagExpr].tp.tp)
      val expr2 = toNRC(out, env + (in -> tvr))
      ForeachUnion(tvr, expr1.asInstanceOf[BagExpr], expr2.asInstanceOf[BagExpr])
    case Join(e1, e2, joinCond) =>
      val c1 = toNRC(e1, env).asBag
      val c2 = toNRC(e2, env).asBag
      prepareJoinOutput(c1, c2, joinCond)
    case Merge(e1, e2) =>
      val c1 = toNRC(e1, env)
      val c2 = toNRC(e2, env)
      Union(c1.asInstanceOf[BagExpr], c2.asInstanceOf[BagExpr])
    case DropDuplicates(e1) =>
      val c1 = toNRC(e1, env).asInstanceOf[BagExpr]
      DeDup(c1)
    case Select(e, cols) =>
      val expr = toNRC(e, env).asInstanceOf[BagVarRef]
      val tvr = TupleVarRef(utilities.Symbol.fresh(), expr.tp.tp)
      val map: IMap[String, TupleVarRef] = IMap(expr.name -> tvr)
      val pq = cols.asInstanceOf[Seq[Col]]
      val k = pq.map(z => sparkAliasing(z) -> translateColumn(z, map).asPrimitive)

      ForeachUnion(tvr, expr, prepareSelectOutput(k))
    //TODO - Reimplementation needed
    //    case Drop(e, cols) =>
    //      DropContext.addField(cols: _*)
    //      val expr = toNRC(e, env).asInstanceOf[BagExpr]
    //      val tvr = TupleVarRef(utilities.Symbol.fresh(), expr.tp.tp)
    //      ForeachUnion(tvr, expr, Singleton(Tuple(unnestBagExpr(expr).diff(DropContext.getDropFields).map(f => f -> PrimitiveProject(tvr, f)).toMap)))
    case Reduce(e, cols, fields) =>
      val expr = toNRC(e, env).asInstanceOf[BagExpr]
      ReduceByKey(expr, cols, fields)
    case Sng(x) =>
      val e = env(x).asInstanceOf[TupleExpr]
      Singleton(e)
    case Wrapper(in, s) =>
      val ds: DataFrame = in.asInstanceOf[DataFrame]
      ds.printSchema()
      val nrcTypeMap: IMap[String, TupleAttributeType] = ds.schema.fields.map {
        case StructField(name, SparkArrayType(dataType, _), _, _) => name -> ArrayType(typeToNRCType(dataType))
//          name -> BagType(TupleType(name -> typeToNRCType(dataType)))
        case StructField(name, dataType, _, _) => name -> typeToNRCType(dataType)
      }.toMap
      BagVarRef(s, BagType(TupleType(nrcTypeMap)))
    case s@Sym(_) =>
      val e = env(s)
      e.asInstanceOf[Expr]
    case s@_ =>
      sys.error("Unsupported: " + s)
  }

  //  TODO - Need new way of getting unnested BagExpr after restructure
  //  @tailrec
  //  private def unnestBagExpr(b: BagExpr): Array[String] = b match {x
  //    case bd: DeDup => unnestBagExpr(bd.e)
  //    case u: Union => unnestBagExpr(u.e1)
  //    case f: ForeachUnion => unnestBagExpr(f.e1)
  //    case bvr: BagVarRef => sys.error("Drop error")
  //      getCtx()(bvr.name).dtypes.map { case (str, _) => str }
  //    case s@_ => sys.error("Unnesting invalid: " + s)
  //  }

  /**
   * This function translates from Spark DataType to NRC/Plan type
   */
  private def typeToNRCType(s: DataType): TupleAttributeType = s match {
    case structType: StructType => BagType(TupleType(structType.fields.map(f => f.name -> typeToNRCType(f.dataType)).toMap))
    case types.StringType => StringType
    case types.IntegerType => IntType
    case types.LongType => LongType
    case types.DoubleType => DoubleType
    case types.BooleanType => BoolType
    case _ => null
  }

  /**
   * This function translates scala type to NRC/Plan type
   */
  private def getPrimitiveType(e: Any): PrimitiveType = e match {
    case true | false => BoolType
    case _: Integer => IntType
    case Long => LongType
    case Double => DoubleType
    case _: String => StringType
    case s@_ => sys.error("Unsupported primitive type: " + s)
  }


  /**
   * This method takes a Rep column definition and recursively translates it to an NRC Expr.
   * It takes in a ctx which contains a mapping from input expression names to the corresponding TupleVarRefs created in toNRC()
   * eg.
   * Spark:
   * df.select(df("column") * df("column2") =>
   * Rep:
   * Mult(BaseCol(id, column), BaseCol(id, column2)) =>
   * NRC:
   * ArithmeticExpr(*,
   * NumericProject(TupleVarRef(id,TupleType(Map(column -> IntType, column2 -> IntType))), column),
   * NumericProject(TupleVarRef(id,TupleType(Map(column -> IntType, column2 -> IntType))),userCount2)
   * )
   */
  private def translateColumn(c: Rep, ctx: IMap[String, TupleVarRef]): Expr = c match {
    case BaseCol(df, n) => Project(ctx(df), n)
    case Literal(e) => Const(e, getPrimitiveType(e))
    case EquiJoinCol(dfId, dfId2, n: String) =>
      Cmp(OpEq, Project(ctx(dfId), n), Project(ctx(dfId2), s"${n}_${extractSuffixCount(dfId2)}"))
    case EquiJoinCol(dfId, dfId2, n@ _*) =>
      n.map(f => Cmp(OpEq, Project(ctx(dfId), f), Project(ctx(dfId2), s"${f}_${extractSuffixCount(dfId2)}"))).reduce(And)
    case Equality(e1, e2) => Cmp(OpEq, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case Inequality(e1, e2) => Cmp(OpNe, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case GreaterThan(e1, e2) => Cmp(OpGt, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case GreaterThanOrEqual(e1, e2) => Cmp(OpGe, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case LessThan(e1, e2) => Cmp(OpGt, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case LessThanOrEqual(e1, e2) => Cmp(OpGe, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case Mult(e1, e2) => ArithmeticExpr(OpMultiply, translateColumn(e1, ctx).asNumeric, translateColumn(e2, ctx).asNumeric)
    case Add(e1, e2) => ArithmeticExpr(OpPlus, translateColumn(e1, ctx).asNumeric, translateColumn(e2, ctx).asNumeric)
    case Sub(e1, e2) => ArithmeticExpr(OpMinus, translateColumn(e1, ctx).asNumeric, translateColumn(e2, ctx).asNumeric)
    case Divide(e1, e2) => ArithmeticExpr(OpDivide, translateColumn(e1, ctx).asNumeric, translateColumn(e2, ctx).asNumeric)
    case Mod(e1, e2) => ArithmeticExpr(OpMod, translateColumn(e1, ctx).asNumeric, translateColumn(e2, ctx).asNumeric)
    case OrRep(e1, e2) => Or(translateColumn(e1, ctx).asCond, translateColumn(e2, ctx).asCond)
    case AndRep(e1, e2) => And(translateColumn(e1, ctx).asCond, translateColumn(e2, ctx).asCond)
  }

  /**
   * Figures out what to call the columns in the output schema,
   * handles auto aliasing in select queries where a new column is being created.
   */
  private def sparkAliasing(col: Rep): String = col match {
    case BaseCol(_, n) => n
    case Mult(e1, e2) => s"(${sparkAliasing(e1)} * ${sparkAliasing(e2)})"
    case Sub(e1, e2) => s"(${sparkAliasing(e1)} - ${sparkAliasing(e2)})"
    case Divide(e1, e2) => s"(${sparkAliasing(e1)} / ${sparkAliasing(e2)})"
    case Mod(e1, e2) => s"(${sparkAliasing(e1)} % ${sparkAliasing(e2)})"
    case Equality(e1, e2) => s"(${sparkAliasing(e1)} = ${sparkAliasing(e2)})"
    case Inequality(e1, e2) => s"(NOT (${sparkAliasing(e1)} = ${sparkAliasing(e2)}))"
    case LessThan(e1, e2) => s"(${sparkAliasing(e2)} < ${sparkAliasing(e1)})"
    case LessThanOrEqual(e1, e2) => s"(${sparkAliasing(e2)} <= ${sparkAliasing(e1)})"
    case GreaterThan(e1, e2) => s"(${sparkAliasing(e1)} > ${sparkAliasing(e2)})"
    case GreaterThanOrEqual(e1, e2) => s"(${sparkAliasing(e1)} >= ${sparkAliasing(e2)})"
    case Literal(e) => String.valueOf(e)
  }


  /**
   * This function is to catch boolean conditions in select operations.
   * eg. df.select(df("column") === 1)
   * +------------+
   * |(column = 1)|
   * +------------|
   * |      false |
   * |      true  |
   * |      false |
   * +------------+
   * We need to check if any of the projection fields contain a Cmp as their PrimitiveExpr,
   * if so we'll update them to project a PrimitiveIfThenElse containing two Constants (for each outcome of the condition),
   * otherwise we leave the projection as is.
   */
  private def prepareSelectOutput(fields: Seq[(String, PrimitiveExpr)]): BagExpr = {
    Singleton(Tuple(fields.map {
      case (s, cmp: PrimitiveCmp) =>
        s -> PrimitiveIfThenElse(cmp.asCond, PrimitiveConst(true, BoolType), PrimitiveConst(false, BoolType))
      case (s, expr) =>
        s -> expr
    }.toMap))
  }

  /**
   * Creates the NRC Expression for a Join Operation.
   * Handles self joins in the check c1Name == c2Name
   * Converts joinCond from a Rep -> NRC Cond Expression
   */
  private def prepareJoinOutput(c1: BagExpr, c2: BagExpr, joinCond: Option[Rep]): ForeachUnion = {
    val c1Name = unnestJoinExpr(c1)
    val c2Name = unnestJoinExpr(c2)
    val tvr = TupleVarRef(utilities.Symbol.fresh(), c1.tp.tp)
    val tvr2 = TupleVarRef(utilities.Symbol.fresh(), c2.tp.tp)
    val getJoinCond = joinCond.orNull
    getJoinCond match {
      case null =>
        val projectionMap = tvr.tp.attrTps.keys.toSeq.map(f => f -> Project(tvr, f)).toMap
        val combinedColumnMap = projectionMap ++ tvr2.tp.attrTps.keys.toSeq.map(f => f -> Project(tvr2, f))
        ForeachUnion(tvr, c1, ForeachUnion(tvr2, c2, Singleton(Tuple(combinedColumnMap))))
      case e:EquiJoinCol =>
        val bagToTupleMap: IMap[String, TupleVarRef] = IMap(c1Name -> tvr) ++ IMap(c2Name -> tvr2)
        val projectionMap = tvr.tp.attrTps.keys.toSeq.map(f => f -> Project(tvr, f)).toMap
        val combinedColumnMap = projectionMap ++ tvr2.tp.attrTps.keys.toSeq.map(f => f -> Project(tvr2, f))-- e.n.map(f => f + "_" + extractSuffixCount(c2Name))
        val nrcCond = translateColumn(getJoinCond, bagToTupleMap).asCond
        ForeachUnion(tvr, c1, ForeachUnion(tvr2, c2, IfThenElse(nrcCond, Singleton(Tuple(combinedColumnMap)))))
      case _ if c1Name == c2Name =>
        val c3 = BagVarRef(c1Name, BagType(TupleType(c1.tp.tp.attrTps ++ c2.tp.tp.attrTps.map { case (key, valueType) => (key + "_2", valueType) })))
        val tvr3 = TupleVarRef(utilities.Symbol.fresh(), c3.tp.tp)
        val colMap: IMap[String, TupleVarRef] = IMap(c1Name -> tvr3)
        val combinedColumnMap: IMap[String, TupleAttributeExpr] = tvr3.tp.attrTps.keys.toSeq.map(f => f -> Project(tvr3, f)).toMap
        val nrcCond = translateColumn(getJoinCond, colMap).asCond
        ForeachUnion(tvr, c1, ForeachUnion(tvr3, c3, IfThenElse(nrcCond, Singleton(Tuple(combinedColumnMap)))))
      case _ =>
        val bagToTupleMap: IMap[String, TupleVarRef] = IMap(c1Name -> tvr) ++ IMap(c2Name -> tvr2)
        val projectionMap = tvr.tp.attrTps.keys.toSeq.map(f => f -> Project(tvr, f)).toMap
        val combinedColumnMap = projectionMap ++ tvr2.tp.attrTps.keys.toSeq.map(f => f -> Project(tvr2, f))
        val nrcCond = translateColumn(getJoinCond, bagToTupleMap).asCond
        ForeachUnion(tvr, c1, ForeachUnion(tvr2, c2, IfThenElse(nrcCond, Singleton(Tuple(combinedColumnMap)))))
    }
  }

  /**
   * Gets the Dataset Identifier from the BagExpr created during a Join
   *
   * @param e
   * @return
   */
  private def unnestJoinExpr(e: BagExpr): String = e match {
    case b: BagVarRef => b.name
    case f: ForeachUnion => unnestJoinExpr(f.e1)
  }

  private def extractSuffixCount(s: String): String = {
    val pattern = "s(\\d+)".r
    s match {
      case pattern(number) => number
      case _ => sys.error("String doesn't match convention")
    }
  }
}
