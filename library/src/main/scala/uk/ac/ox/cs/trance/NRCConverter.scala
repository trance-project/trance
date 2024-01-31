package uk.ac.ox.cs.trance

import framework.common._
import framework.nrc.NRC
import framework.plans.NRCTranslator
import org.apache.spark.sql.types.{DataType, StructField, StructType, ArrayType => SparkArrayType}
import org.apache.spark.sql.{DataFrame, types}

import scala.collection.immutable.{Map => IMap}

object NRCConverter extends NRCTranslator {

  /**
   * This function recursively translates the intermediary Rep object created by [[Wrapper]] into an NRC expression.
   * The rep will enter this function as a nested structure of operations that contains a Wrapper object at its core.
   * We recursively unnest these layers of nesting, building the NRC expression as we go.
   * The innermost layer will be the [[Wrapper]] containing a Dataframe which is represented as a BagVarRef in NRC.
   */
  def toNRC(rep: Rep, env: IMap[String, Expr]): Expr = rep match {
    case Map(e, Fun(in, out)) =>
      val c1 = toNRC(e, env).asBag
      val tvr = TupleVarRef(utilities.Symbol.fresh(), c1.tp.tp)

      val k = in.asInstanceOf[NewSym].symID -> tvr
      val c2 = toNRC(out, env + k).asTuple


      ForeachUnion(tvr, c1, Singleton(c2))
    case FlatMap(e, Fun(in, out)) =>
      val c1 = toNRC(e, env).asBag
      val tvr = TupleVarRef(utilities.Symbol.fresh(), c1.tp.tp)

      val k = in.asInstanceOf[NewSym].symID -> tvr

//      val k = in.asInstanceOf[Sym].rows.map(f => f.id -> tvr).toMap
      val output = toNRC(out, env + k)
      output match {
        case t: TupleExpr => ForeachUnion(tvr, c1, Singleton(t))
        case b: BagExpr => ForeachUnion(tvr, c1, b)
        case s@_ => s
      }
    case Filter(e1, cond) =>
      val c1 = toNRC(e1, env).asBag
      val c1Name = unnestExprId(c1)
      val tvr = TupleVarRef(utilities.Symbol.fresh(), c1.tp.tp)
      val colMap: IMap[String, TupleVarRef] = c1Name.map(name => name -> tvr).toMap
      val nrcCond = translateColumn(cond, colMap).asCond
      val outputMap = tvr.tp.attrTps.keys.toSeq.map(f => f -> Project(tvr, f)).toMap
      ForeachUnion(tvr, c1, IfThenElse(nrcCond, Singleton(Tuple(outputMap))))
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
      val c1 = toNRC(e, env).asBag
      val c1Name = unnestExprId(c1)
      val tvr = TupleVarRef(utilities.Symbol.fresh(), c1.tp.tp)
      val map: IMap[String, TupleVarRef] = c1Name.map(name => name -> tvr).toMap
      val k = cols.map(z => sparkAliasing(z) -> translateColumn(z, map))
      val out = Tuple(k.map(f => f._1 -> prepareSelectOutput(f._2).asInstanceOf[TupleAttributeExpr]).toMap)
      ForeachUnion(tvr, c1, Singleton(out))
    //TODO - Reimplementation needed
    //    case Drop(e, cols) =>
    //      DropContext.addField(cols: _*)
    //      val expr = toNRC(e, env).asInstanceOf[BagExpr]
    //      val tvr = TupleVarRef(utilities.Symbol.fresh(), expr.tp.tp)
    //      ForeachUnion(tvr, expr, Singleton(Tuple(unnestBagExpr(expr).diff(DropContext.getDropFields).map(f => f -> PrimitiveProject(tvr, f)).toMap)))
    case Reduce(e, cols, fields) =>
      val expr = toNRC(e, env).asInstanceOf[BagExpr]
      ReduceByKey(expr, cols, fields)
    case Concat(e1, e2) =>
      val c1 = toNRC(e1, env)
      val c2 = toNRC(e2, env).asInstanceOf[PrimitiveConst]
      Udf(c2.v.asInstanceOf[String], c1.asPrimitive, StringType)
    case Transform(e1, e2) =>
      val c1 = toNRC(e1, env)
      val c2 = toNRC(e2, env).asInstanceOf[PrimitiveConst]
      Udf("Transform", PrimitiveConst(c2.v.asInstanceOf[String], StringType), StringType)
    case Sng(x) =>
      val k = toNRC(x, env).asTuple
      Singleton(k)
    case Wrapper(in, s) =>
      val ds: DataFrame = in
      ds.printSchema()
      val nrcTypeMap = ds.schema.fields.map { case StructField(name, dataType, _, _) => name -> typeToNRCType(dataType) }.toMap
      BagVarRef(s, BagType(TupleType(nrcTypeMap)))
    case Add(e1, e2) =>
      val c1 = toNRC(e1, env).asInstanceOf[NumericExpr]
      val c2 = toNRC(e2, env).asInstanceOf[NumericExpr]
      ArithmeticExpr(OpPlus, c1, c2)
    case Divide(e1, e2) =>
      val c1 = toNRC(e1, env).asInstanceOf[NumericExpr]
      val c2 = toNRC(e2, env).asInstanceOf[NumericExpr]
      ArithmeticExpr(OpDivide, c1, c2)
    case Mult(e1, e2) =>
      val c1 = toNRC(e1, env).asInstanceOf[NumericExpr]
      val c2 = toNRC(e2, env).asInstanceOf[NumericExpr]
      ArithmeticExpr(OpMultiply, c1, c2)
    case Sub(e1, e2) =>
      val c1 = toNRC(e1, env).asInstanceOf[NumericExpr]
      val c2 = toNRC(e2, env).asInstanceOf[NumericExpr]
      ArithmeticExpr(OpMinus, c1, c2)
    case Mod(e1, e2) =>
      val c1 = toNRC(e1, env).asInstanceOf[NumericExpr]
      val c2 = toNRC(e2, env).asInstanceOf[NumericExpr]
      ArithmeticExpr(OpMod, c1, c2)
    case Literal(e) => Const(e, getPrimitiveType(e))
    case RowLiteral(e) => Const(e, getPrimitiveType(e))
    case If(condition, thenBranch, elseBranch) =>
      val c1 = toNRC(condition, env).asCond
      toNRC(thenBranch, env) match {
        case t: Tuple => IfThenElse(c1, Singleton(t))
        case p: PrimitiveExpr =>  val c3 = toNRC(elseBranch, env)
          IfThenElse(c1, p, c3)
        case i: IfThenElse => i
      }
    case GreaterThan(lhs, rhs) =>
      val c1 = toNRC(lhs, env)
      val c2 = toNRC(rhs, env)
      Cmp(OpGt, c1, c2)
    case Equality(lhs, rhs) =>
      val c1 = toNRC(lhs, env)
      val c2 = toNRC(rhs, env)
      Cmp(OpEq, c1, c2)
    case RepRowInst(vals) =>
//      val exprs = vals.map(f => f.name -> toNRC(f.r, env).asInstanceOf[TupleAttributeExpr]).toMap
//      Tuple(exprs)
      val result = repRowResolver(vals, env)
      Tuple(result)
//    case Sym(vals) =>
//      val result = repRowResolver(vals, env)
//      Tuple(result)
    case s: NewSym => TupleVarRef(s.symID, repToNRCType(s.schema))
    case rp: RepProjection =>
      val expr = toNRC(rp.r, env).asTuple
      try {
        Project(expr, rp.name)
      } catch {
        case e: NoSuchElementException => sys.error("Cannot Project Field " + rp.name + " from TupleVarRef " + expr)
      }
    case RepElem(name, id) =>
      val tvr = env(id).asInstanceOf[TupleVarRef]
      try {
        Project(tvr, name)
      } catch {
        case e: NoSuchElementException => sys.error("Cannot Project Field " + name + " from TupleVarRef " + tvr)
      }
    case RepSeq(reps@_*) =>
      val out = reps.map { f =>
        val t = toNRC(f, env)
        t
      }.asInstanceOf[Seq[Tuple]]

     val k = out.map(Singleton).reduce(Union)
      k
    //TODO - need discussion on UDF
    case Alias(e1, output) =>
      val c1 = toNRC(e1, env)
      Udf("Transform", PrimitiveConst(output, StringType), StringType)
    case RepSeq(elems) =>
      val o = toNRC(elems, env)
      o
    case As(e1, alias) =>
      val o = toNRC(e1, env)
      Tuple(alias -> o.asInstanceOf[TupleAttributeExpr])
    case s@_ =>
      sys.error("Unsupported: " + s)
  }

  private def repRowResolver(r: Seq[Rep], env: IMap[String, Expr], as: Option[String] = None): IMap[String, TupleAttributeExpr] = {
    val res = r.flatMap {
      case f: FlatMap =>
        val str = as.getOrElse("nested")
        Seq(str -> toNRC(f, env).asInstanceOf[TupleAttributeExpr])
      case m: Map => Seq("mapTest" -> toNRC(m, env).asInstanceOf[TupleAttributeExpr])
      case a: As => repRowResolver(Seq(a.in), env, Some(a.name))
      case s: Sym => repRowResolver(s.rows, env)
      case elem@_ =>
        Seq(getRepElemName(elem) -> (toNRC(elem, env) match {
          case u: Udf => u
          case n: NumericExpr => n
          case p: PrimitiveExpr => p
          case c: PrimitiveConst => Udf(c.v.asInstanceOf[String], c, StringType)
          case bp: BagProject => bp
        }))
    }.toMap

    res
  }

  private def repToNRCType(s: StructType): TupleType = {
    val tt = s.map{f =>
      f.name -> typeToNRCType(f.dataType)
    }.toMap
    TupleType(tt)
  }

  /**
   * This method is called during flatMap & map when the output schema has different column names from the input schema
   */
  private def updateTupleCols(t: Expr, r: RepRowEncoder): TupleExpr = t match {
    case tuple: Tuple =>
      val updatedFields = tuple.fields.zip(r.output.fields).map {
        case ((_, tupleAttrExpr: TupleAttributeExpr), structField) =>
          structField.name -> tupleAttrExpr
      }
      Tuple(updatedFields)
    case tvr: TupleVarRef => Tuple(tvr.tp.attrTps.zip(r.output.fields).map(f => f._2.name -> tvr(f._1._1)))
    case s@_ => sys.error("Unhandled: " + s)
  }

  /**
   * getRepElemName returns the column name that a RepElem belongs to.
   * This is called when creating the output Tuple
   */
  private def getRepElemName(rep: Rep): String = rep match {
    case RepElem(name, _) => name
    case RepProjection(name, _) => name
    case Add(e1, e2) => getRepElemName(e1)
    case If(e1, e2, e3) => getRepElemName(e2)
    case GreaterThan(e1, e2) => getRepElemName(e1)
    case Mod(e1, e2) => getRepElemName(e1)
    case Sub(e1, e2) => getRepElemName(e1)
    case Mult(e1, e2) => getRepElemName(e1)
    case Divide(e1, e2) => getRepElemName(e1)
    case Concat(e1, e2) => getRepElemName(e1)
    case Transform(e1, e2) => getRepElemName(e1)
    case Alias(e1, _) => getRepElemName(e1)
    case RowLiteral(v) => v.toString
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
    case SparkArrayType(e, _) => ArrayType(typeToNRCType(e))
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
    case _: Long => LongType
    case _: Double => DoubleType
    case _: String => StringType
    case RepElem(id, _) => StringType // this is hacky
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
    case BaseCol(id, n) => Project(ctx(id), n)
    case Literal(e) => Const(e, getPrimitiveType(e))
    case EquiJoinCol(dfId, dfId2, n: String) =>
      Cmp(OpEq, Project(ctx(dfId), n), Project(ctx(dfId2), s"${n}_${extractSuffixCount(dfId2)}"))
    case EquiJoinCol(dfId, dfId2, n@_*) =>
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
    case Add(e1, e2) => s"(${sparkAliasing(e1)} + ${sparkAliasing(e2)})"
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
    case OrRep(e1, e2) => s"(${sparkAliasing(e1)} OR ${sparkAliasing(e2)})"
    case AndRep(e1, e2) => s"(${sparkAliasing(e1)} AND ${sparkAliasing(e2)})"
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
  private def prepareSelectOutput(fields: Expr): Expr = {
    fields match {
      case cmp: PrimitiveCmp =>
        PrimitiveIfThenElse(cmp.asCond, PrimitiveConst(true, BoolType), PrimitiveConst(false, BoolType))
      case or: Or =>
        val lhs = or.e1.asInstanceOf[PrimitiveCmp]
        val rhs = or.e2.asInstanceOf[PrimitiveCmp]
        PrimitiveIfThenElse(Or(lhs.asCond, rhs.asCond), PrimitiveConst(true, BoolType), PrimitiveConst(false, BoolType))
      case and: And =>
        val lhs = and.e1.asInstanceOf[PrimitiveCmp]
        val rhs = and.e2.asInstanceOf[PrimitiveCmp]
        PrimitiveIfThenElse(And(lhs.asCond, rhs.asCond), PrimitiveConst(true, BoolType), PrimitiveConst(false, BoolType))
      case and: And =>
        val lhs = and.e1.asInstanceOf[PrimitiveCmp]
        val rhs = and.e2.asInstanceOf[PrimitiveCmp]
        PrimitiveIfThenElse(And(lhs.asCond, rhs.asCond), PrimitiveConst(true, BoolType), PrimitiveConst(false, BoolType))
      case expr => expr
    }
  }

  /**
   * Creates the NRC Expression for a Join Operation.
   * Handles self joins in the check c1Name == c2Name
   * Converts joinCond from a Rep -> NRC Cond Expression
   */
  private def prepareJoinOutput(c1: BagExpr, c2: BagExpr, joinCond: Option[Rep]): ForeachUnion = {
    val c1Name = unnestExprId(c1)
    val c2Name = unnestExprId(c2)
    val tvr = TupleVarRef(utilities.Symbol.fresh(), c1.tp.tp)
    val tvr2 = TupleVarRef(utilities.Symbol.fresh(), c2.tp.tp)
    val getJoinCond = joinCond.orNull
    getJoinCond match {
      case null =>
        val projectionMap = tvr.tp.attrTps.keys.toSeq.map(f => f -> Project(tvr, f)).toMap
        val combinedColumnMap = projectionMap ++ tvr2.tp.attrTps.keys.toSeq.map(f => f -> Project(tvr2, f))
        ForeachUnion(tvr, c1, ForeachUnion(tvr2, c2, Singleton(Tuple(combinedColumnMap))))
      case e: EquiJoinCol =>
        val bagToTupleMap: IMap[String, TupleVarRef] = c1Name.map(name => name -> tvr).toMap ++ c2Name.map(name => name -> tvr2)
        val projectionMap = tvr.tp.attrTps.keys.toSeq.map(f => f -> Project(tvr, f)).toMap
        val combinedColumnMap = projectionMap ++ tvr2.tp.attrTps.keys.toSeq.map(f => f -> Project(tvr2, f)) -- e.n.map(f => f + "_" + extractSuffixCount(c2Name.head))
        val nrcCond = translateColumn(getJoinCond, bagToTupleMap).asCond
        ForeachUnion(tvr, c1, ForeachUnion(tvr2, c2, IfThenElse(nrcCond, Singleton(Tuple(combinedColumnMap)))))
      case _ if c1Name == c2Name =>
        val c3 = BagVarRef(c1Name.head, BagType(TupleType(c1.tp.tp.attrTps ++ c2.tp.tp.attrTps.map { case (key, valueType) => (key + "_2", valueType) })))
        val tvr3 = TupleVarRef(utilities.Symbol.fresh(), c3.tp.tp)
        val colMap: IMap[String, TupleVarRef] = c1Name.map(name => name -> tvr3).toMap
        val combinedColumnMap: IMap[String, TupleAttributeExpr] = tvr3.tp.attrTps.keys.toSeq.map(f => f -> Project(tvr3, f)).toMap
        val nrcCond = translateColumn(getJoinCond, colMap).asCond
        ForeachUnion(tvr, c1, ForeachUnion(tvr3, c3, IfThenElse(nrcCond, Singleton(Tuple(combinedColumnMap)))))
      case _ =>
        val bagToTupleMap: IMap[String, TupleVarRef] = c1Name.map(name => name -> tvr).toMap ++ c2Name.map(name => name -> tvr2)
        val projectionMap = tvr.tp.attrTps.keys.toSeq.map(f => f -> Project(tvr, f)).toMap
        val combinedColumnMap = projectionMap ++ tvr2.tp.attrTps.keys.toSeq.map(f => f -> Project(tvr2, f))
        val nrcCond = translateColumn(getJoinCond, bagToTupleMap).asCond
        ForeachUnion(tvr, c1, ForeachUnion(tvr2, c2, IfThenElse(nrcCond, Singleton(Tuple(combinedColumnMap)))))
    }
  }

  /**
   * Gets the Dataset Identifier from the lhs of a nested BagExpr (eg. ForEachUnion)
   */
  private def unnestExprId(e: BagExpr): Seq[String] = e match {
    case b: BagVarRef => Seq(b.name)
    case f: ForeachUnion => unnestExprId(f.e1) ++ unnestExprId(f.e2)
    case _ => Seq.empty[String]
  }


  private def extractSuffixCount(s: String): String = {
    val pattern = "s(\\d+)".r
    s match {
      case pattern(number) => number
      case _ => sys.error("String doesn't match convention")
    }
  }
}
