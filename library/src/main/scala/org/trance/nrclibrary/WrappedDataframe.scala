package org.trance.nrclibrary

import org.trance.nrclibrary.utilities.SparkUtil.getSparkSession
import framework.common.{BagType, BoolType, DoubleType, IntType, LongType, OpCmp, OpEq, OpGe, OpGt, OpMultiply, OpNe, StringType, TupleAttributeType, TupleType}
import framework.plans.{AddIndex, BaseNormalizer, CDeDup, CExpr, Comprehension, Constant, EmptySng, Finalizer, InputRef, MathOp, NRCTranslator, Nest, Printer, Projection, Record, Unnester, Variable, Join => CJoin, Merge => CMerge, Project => CProject, Reduce => CReduce, Select => CSelect, Sng => CSng}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{expr, monotonically_increasing_id}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, functions, types}
import org.trance.nrclibrary.utilities.DropContext

import scala.annotation.tailrec

trait WrappedDataframe[T] extends Rep[DataFrame] with NRCTranslator {

  val spark = getSparkSession

  def apply(colName: String): Col[T] = {
    BaseCol(getCtx(this).keys.head, colName)
  }

  def flatMap[S](f: Rep[T] => WrappedDataframe[S]): WrappedDataframe[S] = {
    val sym = Sym[T](utilities.Symbol.fresh())
    val out = f(sym)
    val fun = Fun(sym, out)
    FlatMap(this, fun)
  }

  def union[S](df: WrappedDataframe[S]): WrappedDataframe[S] = {
    Merge(this, df)
  }

  //  //TODO no join cond
  //    def join[S](df: WrappedDataframe[S], joinType: String): WrappedDataframe[S] = {
  //      Join(this, df, null, joinType, this.ctx ++ df.ctx)
  //    }

  def join[S](df: WrappedDataframe[S], joinCond: Rep[T], joinType: String): WrappedDataframe[S] = {
    Join(this, df, joinCond, joinType)
  }

  def dropDuplicates[S]: WrappedDataframe[T] = {
    DropDuplicates(this)
  }


  //TODO - string
//  def select(col: String, cols: String*): WrappedDataframe[T] = {
//    Select(this, col +: cols)
//  }

  def select(cols: Rep[T]*): WrappedDataframe[T] = {
    Select(this, cols)
  }

  def groupBy(cols: String*): GroupBy[T] = {
    GroupBy(this, cols.toList)
  }

  def drop(col: String, cols: String*): WrappedDataframe[T] = {
    Drop(this, col +: cols)
  }

  def drop(col: Column, cols: Column*): WrappedDataframe[T] = {
    Drop(this, col.toString() +: cols.map(_.toString))
  }

  def leaveNRC(): DataFrame = {

    println("This: " + this)
    val nrcExpr = toNRC(this, Map())
    println("nrcExpression: " + nrcExpr)
    //    println("NRC Expr: " + quote(nrcExpr))
    val cExpr = translate(nrcExpr)
    println("initial cExpr: " + cExpr)

    println("Initial cExpr Quote: " + Printer.quote(cExpr))
    val normalizer = new Finalizer(new BaseNormalizer())
    val normalized = normalizer.finalize(cExpr).asInstanceOf[CExpr]
    println("nested and normalized cExpr: " + normalized)
    println("corresponding quote: " + Printer.quote(normalized))

    // Unnesting transforms comprehension calculus to plan language
    val unnested = Unnester.unnest(normalized)(Map(), Map(), None, "_2")
    println("unnested and normalized cExpr: " + unnested)
    println("corresponding quote: " + Printer.quote(unnested))

    val stringified = Printer.quote(unnested)
    println("Printer quote: " + stringified)

    planToDF(unnested, Map()).asInstanceOf[DataFrame]
  }


  private def toNRC[S](rep: Rep[S], env: Map[Rep[_], Any]): Expr = rep match {
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
      val tvr3 = TupleVarRef(utilities.Symbol.fresh(), TupleType(attrTps = c1.tp.tp.attrTps ++ c2.tp.tp.attrTps))
      val combinedColumnMap = tvr3.tp.attrTps.keys.toSeq.map(f => f -> PrimitiveProject(tvr3, f)).toMap
      val map: Map[String, TupleVarRef] = Map(c1.name -> tvr) ++ Map(c2.name -> tvr2)
      val p = translateColumn(joinCond.asInstanceOf[Rep[T]], map).asInstanceOf[CondExpr]
      ForeachUnion(tvr, c1, ForeachUnion(tvr2, c2, IfThenElse(p, Singleton(Tuple(combinedColumnMap)))))
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
      val k = pq.map{
        case compCol: CompCol[T] if compCol.lhs.isInstanceOf[BaseCol[T]] =>
          val baseCol = compCol.lhs.asInstanceOf[BaseCol[T]]
          baseCol.n -> translateColumn(compCol, map)
        case baseCol: BaseCol[T] => baseCol.n -> translateColumn(baseCol, map)
      }
      ForeachUnion(tvr, expr, Singleton(Tuple(k.toMap)))
    case Drop(e, cols) =>
      DropContext.addField(cols: _*)
      val expr = toNRC(e, env).asInstanceOf[BagExpr]
      val tvr = TupleVarRef(utilities.Symbol.fresh(), expr.tp.tp)
      ForeachUnion(tvr, expr, Singleton(Tuple(unnestBagExpr(expr).diff(DropContext.getDropFields).map(f => f -> PrimitiveProject(tvr, f)).toMap)))
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

  // TODO - Remove from WrappedDataframe
  private def planToDF(cExpr: CExpr, ctx: Map[String, Any]): T = cExpr match {
    case Projection(e1, v, p, fields) =>
      val vName = e1.asInstanceOf[CSelect].v.name
      val c1 = planToDF(e1, ctx).asInstanceOf[DataFrame]
      val inputSchema = c1.schema
      val projectionFields = p.asInstanceOf[Record].fields.keys.toSeq
      val updatedFields = projectionFields.flatMap { fieldName =>
        inputSchema.fields.find(_.name == fieldName)
      }
      val outputSchema = StructType(updatedFields)
      val c = c1.flatMap(z => Seq(planToDF(p, ctx + (vName -> z)).asInstanceOf[Row]))(RowEncoder.apply(outputSchema))
      c.asInstanceOf[T]
    case CJoin(left, v, right, v2, cond, fields) =>
      val i1 = planToDF(left, ctx).asInstanceOf[DataFrame]
      val i2 = planToDF(right, ctx).asInstanceOf[DataFrame]
      val c = toSparkCond(cond)
      val col = expr(c)
      i1.join(i2, col).asInstanceOf[T]
    case CMerge(e1, e2) =>
      planToDF(e1, ctx).asInstanceOf[DataFrame].union(planToDF(e2, ctx).asInstanceOf[DataFrame]).asInstanceOf[T]
    case CSelect(x, v, p) =>
      if (p == Constant(true)) {
        return planToDF(x, ctx)
      }
      getSparkSession.emptyDataFrame.asInstanceOf[T]
    case CDeDup(in) =>
      val i1 = planToDF(in, ctx).asInstanceOf[DataFrame]
      i1.dropDuplicates().asInstanceOf[T]
    case InputRef(data, tp) =>
      val x = getCtx(this)
      val df = x.getOrElse(data, ctx(data))
      df.asInstanceOf[T]
    case Variable(name, tp) =>
      val out = ctx(name).asInstanceOf[T]
      out
    case AddIndex(e, name) =>
      val df = planToDF(e, ctx).asInstanceOf[DataFrame].withColumn(name, monotonically_increasing_id)
      df.asInstanceOf[T]
    case CProject(e1, field) =>
      val genericRowWithSchema = planToDF(e1, ctx).asInstanceOf[Row]
      val schema: StructType = genericRowWithSchema.schema
      val columnIndex = schema.fieldIndex(field)
      val columnValue = genericRowWithSchema.get(columnIndex)
      val updatedDf = Row.fromSeq(Array(columnValue))
      updatedDf.asInstanceOf[T]
    case CSng(e1) =>
      planToDF(e1, ctx)
    case Comprehension(e1, v, p, e) =>
      val c1 = planToDF(e1, ctx).asInstanceOf[DataFrame]
      // TODO - take schema from e for RowEncoder
      val k = c1.flatMap(z => Seq(planToDF(e, ctx + (v.name -> z)).asInstanceOf[Row]))(RowEncoder.apply(c1.schema))
      k.asInstanceOf[T]
    case Record(fields) =>
      val t = fields.map { x =>
        planToDF(x._2, ctx).asInstanceOf[Row]
      }.toArray
      val combinedRow = t.flatMap(row => row.toSeq)
      val row = Row.fromSeq(combinedRow).asInstanceOf[T]
      row
    case CReduce(in, v, keys, values) =>
      val d1 = planToDF(in, ctx).asInstanceOf[DataFrame]
      d1.groupBy(keys.head, keys.tail: _*).sum(values: _*).asInstanceOf[T]
    case MathOp(op, e1, e2) =>
      val x = planToDF(e1, ctx).asInstanceOf[Row].getInt(0)
      val y = planToDF(e2, ctx).asInstanceOf[Row].getInt(0)
      op match {
      case OpMultiply => {
        val multipliedValue = x * y
        val row = Row.fromSeq(Seq(multipliedValue)).asInstanceOf[T]
        row
      }
    }

    case EmptySng => getSparkSession.emptyDataFrame.asInstanceOf[T]
    case s@_ =>
      sys.error("Unsupported: " + s)
  }

  @tailrec
  private def unnestBagExpr[T](b: BagExpr): Array[String] = b match {
    case bd: DeDup => unnestBagExpr(bd.e)
    case u: Union => unnestBagExpr(u.e1)
    case f: ForeachUnion => unnestBagExpr(f.e1)
    case bvr: BagVarRef =>
      getCtx(this)(bvr.name).dtypes.map { case (str, _) => str }
    case s@_ => sys.error("Unnesting invalid: " + s)
  }

  private def typeToNRCType(s: DataType): TupleAttributeType = s match {
    case structType: StructType => BagType(TupleType(structType.fields.map(f => f.name -> typeToNRCType(f.dataType)).toMap))
    case types.StringType => StringType
    case types.IntegerType => IntType
    case types.LongType => LongType
    case types.DoubleType => DoubleType
    case types.BooleanType => BoolType
    case _ => null
  }

  private def translateColumn(c: Rep[T], ctx: Map[String, TupleVarRef]): PrimitiveExpr = c match {
    case BaseCol(df, n) => Project(ctx(df), n).asPrimitive
    case Equality(e1: BaseCol[_], e2: BaseCol[_]) => PrimitiveCmp(OpEq, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case Inequality(e1: BaseCol[_], e2: BaseCol[_]) => PrimitiveCmp(OpNe, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case GreaterThan(e1: BaseCol[_], e2: BaseCol[_]) => PrimitiveCmp(OpGt, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case GreaterThanOrEqual(e1: BaseCol[_], e2: BaseCol[_]) => PrimitiveCmp(OpGe, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case LessThan(e1: BaseCol[_], e2: BaseCol[_]) => PrimitiveCmp(OpGt, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case LessThanOrEqual(e1: BaseCol[_], e2: BaseCol[_]) => PrimitiveCmp(OpGe, translateColumn(e1, ctx), translateColumn(e2, ctx))
    case Mult(e1: BaseCol[_], e2: BaseCol[_]) => ArithmeticExpr(OpMultiply, translateColumn(e1, ctx).asNumeric, translateColumn(e2, ctx).asNumeric)
    case OrRep(e1: CompCol[_], e2: CompCol[_]) => Or(translateColumn(e1, ctx).asCond, translateColumn(e2, ctx).asCond)
    case compCol: CompCol[T] =>
      val left = compCol.lhs.asInstanceOf[CompCol[T]]
      val right = compCol.rhs
      val one_v2 = translateColumn(left, ctx).asInstanceOf[CondExpr]
      And(one_v2, PrimitiveCmp(getOp(compCol), translateColumn(left.rhs, ctx), translateColumn(right, ctx)))
  }
  private def getCtx(e: Rep[_]): Map[String, DataFrame] = e match {
    case Wrapper(in, e) => Map(e -> in.asInstanceOf[DataFrame])
    case Merge(e1, e2) => getCtx(e1) ++ getCtx(e2)
    case Join(e1, e2, _, _) => getCtx(e1) ++ getCtx(e2)
    case Drop(e1, _) => getCtx(e1)
    case DropDuplicates(e1) => getCtx(e1)
    case GroupBy(e1, _) => getCtx(e1)
    case Reduce(e1, _, _) => getCtx(e1)
    case Select(e1, _) => getCtx(e1)
    case FlatMap(e1, _) => getCtx(e1)
    case s@_ => sys.error("Error getting context for: " + s)
  }

  private def getOp(e: CompCol[_]): OpCmp = e match {
    case Equality(_, _) => OpEq
    case Inequality(_, _) => OpNe
    case GreaterThan(_, _) => OpGt
    case GreaterThanOrEqual(_, _) => OpGe
    case LessThan(_, _) => OpGt
    case LessThanOrEqual(_, _) => OpGe
  }

  private def toSparkCond(c: CExpr): String = {
    c.vstr.replaceAll("&&", " AND ").replaceAll("\\|\\|", " OR ")
  }
}
