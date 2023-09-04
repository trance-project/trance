package framework.library

import framework.common._
import framework.library.utilities.{Context, DropContext}
import framework.library.utilities.SparkUtil.getSparkSession
import framework.plans.{AddIndex, BaseNormalizer, CDeDup, CExpr, Comprehension, Constant, EmptySng, Finalizer, InputRef, NRCTranslator, Nest, Printer, Projection, Record, Unnester, Variable, Join => CJoin, Merge => CMerge, Project => CProject, Reduce => CReduce, Select => CSelect, Sng => CSng}
import org.apache.spark.sql.catalyst.expressions.{BinaryComparison, EqualTo, Expression, GreaterThan => GreaterThanExpr, GreaterThanOrEqual, Not => NotExpression}
import org.apache.spark.sql.functions.{col, expr, monotonically_increasing_id}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, functions, types}

import scala.annotation.tailrec

trait WrappedDataframe[T] extends Rep[DataFrame] with NRCTranslator {

  def apply(colName: String): Column = col(colName)

  def flatMap[S](f: Rep[T] => WrappedDataframe[S]): WrappedDataframe[S] = {
    val sym = Sym[T](utilities.Symbol.fresh())
    val out = f(sym)
    val fun = Fun(sym, out)
    FlatMap(this, fun)
  }

  def union[S](df: WrappedDataframe[S]): WrappedDataframe[S] = {
    Merge(this, df)
  }

  //TODO no join cond
//  def join[S](df: WrappedDataframe[S], joinType: String): WrappedDataframe[S] = {
//    Join(this, df, joinType)
//  }

  def join[S](df: WrappedDataframe[S], joinCond: Column, joinType: String = "inner"): WrappedDataframe[S] = joinCond.expr match {
      case EqualTo(left, right) =>
        Join(this, df, Col(Equals, left.sql, right.sql), joinType)
      case NotExpression(child) =>
        val c = child.asInstanceOf[EqualTo]
        Join(this, df, Col(NotEqual, c.left.sql, c.right.sql), joinType)
      case GreaterThanExpr(left, right) =>
        Join(this, df, Col(GreaterThan, left.sql, right.sql), joinType)
      case GreaterThanOrEqual(left, right) =>
        Join(this, df, Col(GreaterThanEqual, left.sql, right.sql), joinType)
      case s@_ => sys.error("Unhandled join condition expression: " + s)
  }

  def dropDuplicates[S]: WrappedDataframe[T] = {
    DropDuplicates(this)
  }

  def select(col: String, cols: String*): WrappedDataframe[T] = {
    Select(this, col +: cols)
  }
  def select(col: Column, cols: Column*): WrappedDataframe[T] = {
    Select(this, col.toString +: cols.map(_.toString))
  }


  def groupBy(col: String): GroupBy[T] = {
    GroupBy(this, List(col))
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
    println("NRC Expr: " + quote(nrcExpr))
    val cExpr = translate(nrcExpr)
    println("initial cExpr: " + cExpr)

    println("Initial cExpr Quote: " + Printer.quote(cExpr))
    val normalizer = new Finalizer(new BaseNormalizer())
    val normalized = normalizer.finalize(cExpr).asInstanceOf[CExpr]
    println("nested and normalized cExpr: " + normalized)
    println("corresponding quote: " + Printer.quote(normalized))

    val unnested = Unnester.unnest(normalized)(Map(), Map(), None, "_2")
    println("unnested and normalized cExpr: " + unnested)
    println("corresponding quote: " + Printer.quote(unnested))

    val stringified = Printer.quote(unnested)
    println("Printer quote: " + stringified)
    Context.ctx.foreach(x => println("Context name in context:" + x._1))


    planToDF(unnested).asInstanceOf[DataFrame]
  }


  private def toNRC[T](rep: Rep[T], env: Map[Rep[_], Any]): Expr = rep match {
    case FlatMap(e, Fun(in, out)) =>
      val expr1 = toNRC(e, env)
      val tvr = TupleVarRef(utilities.Symbol.fresh(), expr1.asInstanceOf[BagExpr].tp.tp)
      val expr2 = toNRC(out, env + (in -> tvr))
      ForeachUnion(tvr, expr1.asInstanceOf[BagExpr], expr2.asInstanceOf[BagExpr])
    case Join(e1, e2, joinCond, joinType) =>
      val c1 = toNRC(e1, env).asInstanceOf[BagExpr]
      val c2 = toNRC(e2, env).asInstanceOf[BagExpr]
      val tvr = TupleVarRef(utilities.Symbol.fresh(), c1.tp.tp)
      val tvr2 = TupleVarRef(utilities.Symbol.fresh(), c2.tp.tp)
      val tvr3 = TupleVarRef(utilities.Symbol.fresh(), TupleType(attrTps = c1.tp.tp.attrTps ++ c2.tp.tp.attrTps))
      val co = tvr3.tp.attrTps.keys.toSeq.map(f => f -> Project(tvr3 ,f)).toMap
      ForeachUnion(tvr, c1, ForeachUnion(tvr2, c2, IfThenElse(Cmp(colToJoinCond(joinCond.op), Project(tvr, joinCond.lhs), Project(tvr2, joinCond.rhs)), Singleton(Tuple(co))).asBag))
    case Merge(e1, e2) =>
      val c1 = toNRC(e1, env)
      val c2 = toNRC(e2, env)
      Union(c1.asInstanceOf[BagExpr], c2.asInstanceOf[BagExpr])
    case DropDuplicates(e1) =>
      val c1 = toNRC(e1, env).asInstanceOf[BagExpr]
      DeDup(c1)
    case Select(e, cols) =>
      val expr = toNRC(e, env).asInstanceOf[BagExpr]
      val tvr = TupleVarRef(utilities.Symbol.fresh(), expr.tp.tp)
      ForeachUnion(tvr, expr, Singleton(Tuple(cols.map(f => f -> Project(tvr,f)).toMap)))
    case Drop(e, cols) =>
      DropContext.addField(cols:_*)
      val expr = toNRC(e, env).asInstanceOf[BagExpr]
      val tvr = TupleVarRef(utilities.Symbol.fresh(), expr.tp.tp)
      ForeachUnion(tvr, expr, Singleton(Tuple(unnestBagExpr(expr).diff(DropContext.getDropFields).map(f => f -> Project(tvr, f)).toMap)))
    case Reduce(e, cols, fields) =>
      val expr = toNRC(e, env).asInstanceOf[BagExpr]
      ReduceByKey(expr, cols, fields)
    case Sng(x) =>
      val e = env(x).asInstanceOf[TupleExpr]
      Singleton(e)
    case Wrapper(in) =>
      val datasetIdentifier = utilities.Symbol.fresh()
      if (in.isInstanceOf[DataFrame]) {
        val ds: DataFrame = in.asInstanceOf[DataFrame]
        Context.addMapping(datasetIdentifier, ds)
        val nrcTypeMap: Map[String, TupleAttributeType] = ds.schema.fields.map {
          case StructField(name, ArrayType(dataType, _), _, _) => "array" -> BagType(TupleType(name -> typeToNRCType(dataType)))
          case StructField(name, dataType, _, _) => name -> typeToNRCType(dataType)
        }.toMap
        BagVarRef(datasetIdentifier, BagType(TupleType(nrcTypeMap)))
      }
        // TODO - Handle Wrapper not containing Dataframe?
      else if(in.isInstanceOf[Sym[T]]) {
        val e = env(in.asInstanceOf[Sym[T]])
        e.asInstanceOf[Expr]
      }
      else null
    case s@Sym(_) =>
      val e = env(s)
      e.asInstanceOf[Expr]
    case s@_ =>
      sys.error("Unsupported: " + s)
  }

  private def planToDF(cExpr: CExpr): T = cExpr match {
    case Projection(e1, v, p, fields) =>
      // e1 is the input, and what needs to be done to it, i.e. addIndex etc. We then use p and its fields to get what to query on e1.
      val i1 = planToDF(e1).asInstanceOf[DataFrame]
      //TODO - support nested Variables
      // Map through i1 like teams example (adding to context)

      val c = p.asInstanceOf[Record]
      val selectExpr = c.fields.map(_._1).toSeq
      val resultDF = i1.select(i1.columns.filter(selectExpr.contains).map(functions.col): _*)
      resultDF.asInstanceOf[T]
    case CJoin(left, v, right, v2, cond, fields) =>
      val i1 = planToDF(left).asInstanceOf[DataFrame]
      val i2 = planToDF(right).asInstanceOf[DataFrame]
      val col = expr(cond.vstr)
      i1.join(i2, col).asInstanceOf[T]
    case CMerge(e1, e2) =>
      planToDF(e1).asInstanceOf[DataFrame].union(planToDF(e2).asInstanceOf[DataFrame]).asInstanceOf[T]
    case CSelect(x, v, p) =>
      if(p == Constant(true)) {
        return planToDF(x)
      }
      getSparkSession.emptyDataFrame.asInstanceOf[T]
    case CDeDup(in) =>
      val i1 = planToDF(in).asInstanceOf[DataFrame]
      i1.dropDuplicates().asInstanceOf[T]
    case InputRef(data, tp) =>
      val df = Context.getMapping(data)
      df.asInstanceOf[T]
    case Variable(name, tp) =>

      //TODO - Pass env down to planToDF from toNRC. No global context.

      val df = Context.getMapping(name)
      df.asInstanceOf[T]
    case AddIndex(e, name) =>
      val df = planToDF(e).asInstanceOf[DataFrame].withColumn(name, monotonically_increasing_id)
      df.asInstanceOf[T]
    case CProject(e1, field) =>
      val df = planToDF(e1).asInstanceOf[DataFrame]
      df.select(field).asInstanceOf[T]
    case Nest(in, v, key, value, filter, nulls, ctag) =>
      // TODO - Currently hardcoded for groupBy & Sum
      val c1 = planToDF(in).asInstanceOf[DataFrame]
      val g = c1.groupBy(key.head)
      val v = value.vstr
      g.sum(v).asInstanceOf[T]
    case CSng(e1) =>
      planToDF(e1)
    case Comprehension(e1, v, p, e) =>
      //TODO - Handle comprehension properly
      val c1 = planToDF(e1).asInstanceOf[DataFrame]
      val d1 = planToDF(e)
      if (d1.isInstanceOf[Seq[String]]) {
        val d2 = d1.asInstanceOf[Seq[String]]
        c1.selectExpr(d2:_*).asInstanceOf[T]
      }
      else if (d1.isInstanceOf[DataFrame]) {
        d1
      }
      else
        sys.error("Comprehension error")
    case Record(fields) =>
      //TODO - Recursively handle nested Variable(s)
      val exprs = fields.map(x => x._1).toSeq
      exprs.asInstanceOf[T]
    case CReduce(in, v, keys, values) =>
      val d1 = planToDF(in).asInstanceOf[DataFrame]
      d1.groupBy(keys.head, keys.tail: _*).sum(values:_*).asInstanceOf[T]
    case EmptySng => getSparkSession.emptyDataFrame.asInstanceOf[T]
    case s@_ =>
      sys.error("Unsupported: " + s)
  }

  @tailrec
  private def unnestBagExpr[T](b: BagExpr): Array[String] = b match {
    case bd: DeDup => unnestBagExpr(bd.e)
    case u: Union => unnestBagExpr(u.e1)
    case f: ForeachUnion => unnestBagExpr(f.e1)
    case bvr: BagVarRef => {
      Context.getMapping(bvr.name).dtypes.map { case (str, _) => str }
    }
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

  private def colToJoinCond[T](c: Comparator): OpCmp = c match {
      case Equals => OpEq
      case NotEqual => OpNe
      case GreaterThan => OpGe
      case GreaterThanEqual => OpGt
      case p@_ => sys.error("Invalid join cond: " + p)
    }
}

