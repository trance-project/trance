package uk.ac.ox.cs.trance

import framework.common.{StringType, TupleAttributeType}
import framework.plans.{BaseNormalizer, CExpr, Finalizer, NRCTranslator, Printer, Unnester}
import framework.utils.Utils.ind
import org.apache.spark.sql.{Column, DataFrame}
import uk.ac.ox.cs.trance.utilities.JoinContext

import scala.collection.immutable.{Map => IMap}

trait WrappedDataframe extends Rep with NRCTranslator {

  /**
   * The constructor for WrappedDataframe is made to mimic Spark's [[Column]] syntax. <br>
   * df("column") returns a column in this case a [[Col]]
   * that can be used as a way of specifying columns contained in [[WrappedDataframe]]s for use in [[Select]], [[Join]], [[Drop]]...
   */
  def apply(colName: String): Col = {
    BaseCol(getCtx(this).keys.head, colName)
  }

  def flatMap(f: Rep => WrappedDataframe): WrappedDataframe = {
    val sym = Sym(utilities.Symbol.fresh())
    val out = f(sym)
    val fun = Fun(sym, out)
    FlatMap(this, fun)
  }

  def union(df: WrappedDataframe): WrappedDataframe = {
    Merge(this, df)
  }

  def join(df: Wrapper): WrappedDataframe = {
    Join(this, df, None)
  }

  def join(df: Wrapper, joinCond: Rep): WrappedDataframe = {
    val updatedWrapper = handleDupColumnNames(df)
    val updatedJoinCond = handleDupColumnNames(joinCond)
    Join(this, updatedWrapper.asInstanceOf[WrappedDataframe], Some(updatedJoinCond))
  }

  /**
   * Join in the format df.join(df2, "columnName").
   * Joins the datasets on equality of the given column name(s) present in each dataset and then drops the duplicate.
   * In this case the matched column from the second dataset.
   */
  def join(df: Wrapper, withColumns: String): WrappedDataframe = {
    val nestedDfId = getNestedWrapperId(this)
    val updatedWrapper = handleDupColumnNames(df)
    val joinColumns = EquiJoinCol(nestedDfId, df.str, withColumns)
    Join(this, updatedWrapper, Some(joinColumns))
  }

  /**
   * Just like join(df: Wrapper, withColumns: String) but its possible to specify multiple columns in the join condition.
   */
  def join(df: Wrapper, withColumns: Seq[String]): WrappedDataframe = {
    val nestedDfId = getNestedWrapperId(this)
    val updatedWrapper = handleDupColumnNames(df)
    val joinColumns = EquiJoinCol(nestedDfId, df.str, withColumns:_*)
    Join(this, updatedWrapper, Some(joinColumns))
  }


  def dropDuplicates: WrappedDataframe = {
    DropDuplicates(this)
  }


  //TODO - string
  //  def select(col: String, cols: String*): WrappedDataframe = {
  //    Select(this, col +: cols)
  //  }

  def select(cols: Rep*): WrappedDataframe = {
    Select(this, cols)
  }

  def groupBy(cols: String*): GroupBy = {
    GroupBy(this, cols.toList)
  }

  def drop(col: String, cols: String*): WrappedDataframe = {
    Drop(this, col +: cols)
  }

  def drop(col: Column, cols: Column*): WrappedDataframe = {
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
  private def getCtx(e: Rep = this): IMap[String, DataFrame] = e match {
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


  /**
   * This takes in the original join condition and, if column names have needed to be changed due to duplicates,
   * will change the join condition column to match those changes
   */
  private def handleDupColumnNames(joinCond: Rep): Rep = joinCond match {
    case Equality(lhs, rhs) => Equality(handleDupColumnNames(lhs), handleDupColumnNames(rhs))
    case GreaterThan(lhs, rhs) => GreaterThan(handleDupColumnNames(lhs), handleDupColumnNames(rhs))
    case GreaterThanOrEqual(lhs, rhs) => GreaterThanOrEqual(handleDupColumnNames(lhs), handleDupColumnNames(rhs))
    case LessThan(lhs, rhs) => LessThan(handleDupColumnNames(lhs), handleDupColumnNames(rhs))
    case LessThanOrEqual(lhs, rhs) => LessThanOrEqual(handleDupColumnNames(lhs), handleDupColumnNames(rhs))
    case Inequality(lhs, rhs) => Inequality(handleDupColumnNames(lhs), handleDupColumnNames(rhs))
    case v: Literal => v
    case BaseCol(dfId, str) =>
      val strs = JoinContext.getMappingsForStr(dfId)
      val matchingValueOption: String = strs.find(_.startsWith(str)).get
      BaseCol(dfId, matchingValueOption)
//    case EquiJoinCol(dfId, n) =>
//      val strs = JoinContext.getMappingsForStr(dfId)
//      val matchingValueOption: String = strs.find(_.startsWith(n)).get
//      EquiJoinCol(dfId, matchingValueOption)
  }

  /**
   * This takes the WrappedDataset as a Wrapper that is due to be joined and updates columns that may have been renamed during Wrapping process due to duplicate column names.
   * Currently only used in Joins.
   */
  private def handleDupColumnNames(w: Wrapper): Wrapper = {
    val columnNames = JoinContext.getMappingsForStr(w.str)
    val updatedNestedDf = columnNames.zip(w.in.asInstanceOf[DataFrame].columns).foldLeft(w.in.asInstanceOf[DataFrame]) {
      case (accDf, (newCol, oldCol)) =>
        accDf.withColumnRenamed(oldCol, newCol)
    }

    val updatedWrapper = Wrapper(updatedNestedDf, w.str)
    updatedWrapper.asInstanceOf[Wrapper]
  }

  /**
   * In the case of nested Equi-Joins its necessary to have the Dataset Identifier of the base nested Wrapper.
   * This recursively extracts that ID from the [[this]] object
   */
  private def getNestedWrapperId(df: WrappedDataframe): String = df match {
    case w: Wrapper => w.str
    case o: Operation => o match {
      case Join(lhs, _, _) => getNestedWrapperId(lhs)
    }
  }

}


