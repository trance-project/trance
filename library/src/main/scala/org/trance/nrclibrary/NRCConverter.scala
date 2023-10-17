package org.trance.nrclibrary

import framework.common._
import framework.plans.NRCTranslator
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, types}
import org.trance.nrclibrary.utilities.DropContext

object NRCConverter extends NRCTranslator {

  /*
  This function recursively translates the intermediary Rep object created by Wrapper.scala into an NRC expression.
  The rep will enter this function as a nested structure of operations that contains a Wrapper object at its core.
  We recursively unnest these layers of nesting, building the NRC expression as we go.
  The innermost layer will be the Wrapper containing a Dataframe which is represented as a BagVarRef in NRC.
   */
  def toNRC[T, S](rep: Rep[S], env: Map[Rep[_], Any]): Expr = rep match {
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
    case Join(e1, e2, joinCond, joinType) =>
      val c1 = toNRC(e1, env).asInstanceOf[BagVarRef]
      val c2 = toNRC(e2, env).asInstanceOf[BagVarRef]
      val tvr = TupleVarRef(utilities.Symbol.fresh(), c1.tp.tp)
      val tvr2 = TupleVarRef(utilities.Symbol.fresh(), c2.tp.tp)
      // If there are duplicate columns add suffix
      val map: Map[String, TupleVarRef] = Map(c1.name -> tvr) ++ Map(c2.name -> tvr2)

      val combinedColumnMap = tvr.tp.attrTps.keys.toSeq.map(f => f -> PrimitiveProject(tvr, f)).toMap ++
        tvr2.tp.attrTps.keys.toSeq.map { f =>
          val newKey = if (tvr.tp.attrTps.contains(f)) f + "_2" else f
          newKey -> PrimitiveProject(tvr2, f)
        }.toMap
      val col = translateColumn(joinCond.asInstanceOf[Rep[T]], map).asInstanceOf[CondExpr]
      ForeachUnion(tvr, c1, ForeachUnion(tvr2, c2, IfThenElse(col, Singleton(Tuple(combinedColumnMap)))))
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
      val map: Map[String, TupleVarRef] = Map(expr.name -> tvr)
      val pq = cols.asInstanceOf[Seq[Col[T]]]
      val k = pq.map(z => sparkAliasing(z) -> translateColumn(z, map))

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
      val nrcTypeMap: Map[String, TupleAttributeType] = ds.schema.fields.map {
        case StructField(name, ArrayType(dataType, _), _, _) => "array" -> BagType(TupleType(name -> typeToNRCType(dataType)))
        case StructField(name, dataType, _, _) => name -> typeToNRCType(dataType)
      }.toMap
      BagVarRef(s, BagType(TupleType(nrcTypeMap)))
    case s@Sym(_) =>
      val e = env(s)
      e.asInstanceOf[Expr]
    case s@_ =>
      sys.error("Unsupported: " + s)
  }


  //  @tailrec
  //  private def unnestBagExpr[T](b: BagExpr): Array[String] = b match {
  //    case bd: DeDup => unnestBagExpr(bd.e)
  //    case u: Union => unnestBagExpr(u.e1)
  //    case f: ForeachUnion => unnestBagExpr(f.e1)
  //    case bvr: BagVarRef => sys.error("Drop error")
  //      getCtx()(bvr.name).dtypes.map { case (str, _) => str }
  //    case s@_ => sys.error("Unnesting invalid: " + s)
  //  }

  /*
  This function translates from Spark DataType to NRC/Plan type
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

  /*
  This function translates scala type to NRC/Plan type
   */
  private def getPrimitiveType(e: Any): PrimitiveType = e match {
    case true | false => BoolType
    case _: Integer => IntType
    case Long => LongType
    case Double => DoubleType
    case _: String => StringType
    case s@_ => sys.error("Unsupported primitive type: " + s)
  }


  /*
  This method takes a Rep column definition and recursively translates it to an NRC Expr.
  It takes in a ctx which contains a mapping from input expression names to the corresponding TupleVarRefs created in toNRC()

  eg.

  Spark:
  df.select(df("column") * df("column2") =>

  Rep:
  Mult(BaseCol(id, column), BaseCol(id, column2)) =>

  NRC:
  ArithmeticExpr(*,
    NumericProject(TupleVarRef(id,TupleType(Map(column -> IntType, column2 -> IntType))), column),
    NumericProject(TupleVarRef(id,TupleType(Map(column -> IntType, column2 -> IntType))),userCount2)
  )
     */
  private def translateColumn[T](c: Rep[T], ctx: Map[String, TupleVarRef]): PrimitiveExpr = c match {
    case BaseCol(df, n) => Project(ctx(df), n).asPrimitive
    case Literal(e) => PrimitiveConst(e, getPrimitiveType(e))
    case Equality(e1, e2) => PrimitiveCmp(OpEq, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case Inequality(e1, e2) => PrimitiveCmp(OpNe, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case GreaterThan(e1, e2) => PrimitiveCmp(OpGt, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case GreaterThanOrEqual(e1, e2) => PrimitiveCmp(OpGe, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case LessThan(e1, e2) => PrimitiveCmp(OpGt, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case LessThanOrEqual(e1, e2) => PrimitiveCmp(OpGe, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case Mult(e1, e2) => ArithmeticExpr(OpMultiply, translateColumn(e1, ctx).asNumeric, translateColumn(e2, ctx).asNumeric)
    case Add(e1, e2) => ArithmeticExpr(OpPlus, translateColumn(e1, ctx).asNumeric, translateColumn(e2, ctx).asNumeric)
    case Sub(e1, e2) => ArithmeticExpr(OpMinus, translateColumn(e1, ctx).asNumeric, translateColumn(e2, ctx).asNumeric)
    case Divide(e1, e2) => ArithmeticExpr(OpDivide, translateColumn(e1, ctx).asNumeric, translateColumn(e2, ctx).asNumeric)
    case Mod(e1, e2) => ArithmeticExpr(OpMod, translateColumn(e1, ctx).asNumeric, translateColumn(e2, ctx).asNumeric)
    case OrRep(e1, e2) => Or(translateColumn(e1, ctx).asCond, translateColumn(e2, ctx).asCond)
    case AndRep(e1, e2) => And(translateColumn(e1, ctx).asCond, translateColumn(e2, ctx).asCond)
  }

  /*
  Figures out what to call the columns in the output schema,
  handles auto aliasing in select queries where a new column is being created.
  */
  private def sparkAliasing(col: Rep[_]): String = col match {
    case BaseCol(_, n) => n
    case Mult(e1, e2) => s"(${sparkAliasing(e1)} * ${sparkAliasing(e2)})"
    case Equality(e1, e2) => s"(${sparkAliasing(e1)} = ${sparkAliasing(e2)})"
    case Literal(e) => String.valueOf(e)
  }


  /* This function is to catch boolean conditions in select operations.
  eg. df.select(df("column") === 1)
      +------------+
      |(column = 1)|
      +------------|
      |      false |
      |      true  |
      |      false |
      +------------+

  We need to check if any of the projection fields contain a Cmp as their PrimitiveExpr,
  if so we'll update them to project a PrimitiveIfThenElse containing two Constants (for each outcome of the condition),
  otherwise we leave the projection as is.
  */
  private def prepareSelectOutput(fields: Seq[(String, PrimitiveExpr)]): BagExpr = {
    Singleton(Tuple(fields.map{
      case (s, cmp: PrimitiveCmp) =>
        s -> PrimitiveIfThenElse(cmp.asCond, PrimitiveConst(true, BoolType), PrimitiveConst(false, BoolType))
      case (s, expr) =>
        s -> expr
    }.toMap))
  }
}
