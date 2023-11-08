package uk.ac.ox.cs.trance

import framework.common.{StringType, TupleAttributeType}
import framework.plans.{BaseNormalizer, CExpr, Finalizer, NRCTranslator, Printer, Unnester}
import framework.utils.Utils.ind
import org.apache.spark.sql.{Column, DataFrame}
import uk.ac.ox.cs.trance.utilities.JoinCondContext

import scala.collection.immutable.{Map => IMap}

trait WrappedDataframe[T] extends Rep[DataFrame] with NRCTranslator {

  /**
   * The constructor for WrappedDataframe is made to mimic Spark's [[Column]] syntax. <br>
   * df("column") returns a column in this case a [[Col]]
   * that can be used as a way of specifying columns contained in [[WrappedDataframe]]s for use in [[Select]], [[Join]], [[Drop]]...
   */
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

  def join[S](df: WrappedDataframe[S], joinCond: Rep[T]): WrappedDataframe[S] = {
    val updatedWrapper = handleDupColumnNames(df.asInstanceOf[Wrapper[_]])
    val updatedJoinCond = handleDupColumnNames(joinCond)
    Join(this, updatedWrapper.asInstanceOf[WrappedDataframe[S]], updatedJoinCond)
  }

  def dropDuplicates: WrappedDataframe[T] = {
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

  /**
   * The pipeline controller function.
   * Intended to be called from Developer's environment.
   * Takes the [[Rep]] object and converts it to an NRC Expression then to [[CExpr]].
   * The [[CExpr]] undergoes normalization & unnesting before being converted to a Dataframe
   *
   * @return [[DataFrame]]
   */
  def leaveNRC(): DataFrame = {
    val ctx = getCtx()
    println("Rep: " + this)

    val nrcExpr = NRCConverter.toNRC(this, IMap())
    println("nrcExpression: " + nrcExpr)
    //    println("nrc quote: " + nrcquote(nrcExpr))

    val cExpr: CExpr = NRCConverter.translate(nrcExpr)
    println("initial cExpr: " + cExpr)
    println("Initial cExpr Quote: " + Printer.quote(cExpr))

    val normalizer = new Finalizer(new BaseNormalizer())
    val normalized = normalizer.finalize(cExpr).asInstanceOf[CExpr]
    println("nested and normalized cExpr: " + normalized)
    println("corresponding quote: " + Printer.quote(normalized))

    // Unnesting transforms comprehension calculus to plan language
    val unnested = Unnester.unnest(normalized)(IMap(), IMap(), None, "_2")
    println("unnested and normalized cExpr: " + unnested)
    println("corresponding quote: " + Printer.quote(unnested))

    val stringified = Printer.quote(unnested)
    println("Printer quote: " + stringified)
    PlanConverter.convert(unnested, ctx).asInstanceOf[DataFrame]
  }

  /**
   *
   * @param e The [[Rep]] contained in the this instance
   * @return The mapping from the Dataframe's identifier created in [[Wrapper]] and the corresponding Dataframe
   *         <br><br>
   *         This mapping is passed into the [[PlanConverter]] and will be referenced when an InputRef is encountered
   */
  private def getCtx(e: Rep[_] = this): IMap[String, DataFrame] = e match {
    case Wrapper(in, e) => IMap(e -> in.asInstanceOf[DataFrame]) //Maybe get from JoinCondCtx
    case Merge(e1, e2) => getCtx(e1) ++ getCtx(e2)
    case Join(e1, e2, _) => getCtx(e1) ++ getCtx(e2)
    case Drop(e1, _) => getCtx(e1)
    case DropDuplicates(e1) => getCtx(e1)
    case GroupBy(e1, _) => getCtx(e1)
    case Reduce(e1, _, _) => getCtx(e1)
    case Select(e1, _) => getCtx(e1)
    case FlatMap(e1, _) => getCtx(e1)
    case s@_ => sys.error("Error getting context for: " + s)
  }
}


