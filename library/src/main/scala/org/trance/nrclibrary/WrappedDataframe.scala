package org.trance.nrclibrary

import org.trance.nrclibrary.utilities.SparkUtil.getSparkSession
import framework.plans.{BaseNormalizer, CExpr, Finalizer, NRCTranslator, Printer, Unnester}
import org.apache.spark.sql.{Column, DataFrame}

trait WrappedDataframe[T] extends Rep[DataFrame] with NRCTranslator {

  val spark = getSparkSession

  def apply[S](colName: String): Col[T] = {
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
    val ctx = getCtx(this)
    println("Rep: " + this)

    val nrcExpr= NRCConverter.toNRC(this, Map())
    println("nrcExpression: " + nrcExpr)

    val cExpr: CExpr = NRCConverter.translate(nrcExpr)
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
    PlanConverter.planToDF(unnested, ctx).asInstanceOf[DataFrame]
  }

  private def getCtx(e: Rep[_] = this): Map[String, DataFrame] = e match {
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
}
