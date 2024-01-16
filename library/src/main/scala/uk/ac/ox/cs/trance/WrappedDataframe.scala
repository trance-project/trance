package uk.ac.ox.cs.trance

import framework.common.TupleType
import framework.plans.{BaseNormalizer, CExpr, Finalizer, NRCTranslator, Printer, Unnester}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Encoder, SparkSession, Row => SparkRow}
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
   def map(f: RepRow => Rep)(schema: RepRowEncoder): WrappedDataframe = this match {
     case w: Wrapper =>
       val symID = utilities.Symbol.fresh()
       val sym = Sym(symID, w.in.columns.map { f => RepElem(f, symID) }.toSeq)
       val out = f(sym)
       val fun = Fun(sym, out)
       Map(this, fun, schema)
     case s: Select =>
       val symID = utilities.Symbol.fresh()
       val sym = Sym(symID, s.self.asInstanceOf[Join].self.asInstanceOf[Wrapper].in.columns.map { f => RepElem(f, symID) }.toSeq)
       val out = f(sym)
       val fun = Fun(sym, out)
       Map(this, fun, schema)



  }

  def flatMap(f: RepRow => Rep): WrappedDataframe = this match {
    case w: Wrapper =>
      val symID = utilities.Symbol.fresh()
      val sym = Sym(symID, w.in.columns.map { f => RepElem(f, symID) }.toSeq)
      val out = f(sym)
      val fun = Fun(sym, out)
      FlatMap(this, fun)
    case s: Select =>
      val symID = utilities.Symbol.fresh()
      val sym = Sym(symID, s.self.asInstanceOf[Join].self.asInstanceOf[Wrapper].in.columns.map { f => RepElem(f, symID) }.toSeq)
      val out = f(sym)
      val fun = Fun(sym, out)
      FlatMap(this, fun)
  }

  def union(df: WrappedDataframe): WrappedDataframe = {
    Merge(this, df)
  }

  def join(df: Wrapper): WrappedDataframe = {
    Join(this, handleDupColumnNames(df), None)
  }

  def join(df: WrappedDataframe, joinCond: Rep): WrappedDataframe = {
    Join(this, handleDupColumnNames(df), Some(handleDupColumnNames(joinCond)))
  }

  /**
   * Join in the format df.join(df2, "columnName").
   * Joins the datasets on equality of the given column name(s) present in each dataset and then drops the duplicate.
   * In this case the matched column from the second dataset.
   */
  def join(df: Wrapper, withColumns: String): WrappedDataframe = {
    val joinColumns = EquiJoinCol(getNestedWrapperId(this), df.str, withColumns)
    Join(this, handleDupColumnNames(df), Some(joinColumns))
  }

  /**
   * Just like join(df: Wrapper, withColumns: String) but its possible to specify multiple columns in the join condition.
   */
  def join(df: Wrapper, withColumns: Seq[String]): WrappedDataframe = {
    val joinColumns = EquiJoinCol(getNestedWrapperId(this), df.str, withColumns:_*)
    Join(this, handleDupColumnNames(df), Some(joinColumns))
  }

  def dropDuplicates(): WrappedDataframe = {
    DropDuplicates(this)
  }

 // TODO - handle col("*") condition
  def select[A](cols: A*): WrappedDataframe = cols match {
    case Seq(_: Col, _*) => Select(this, cols.asInstanceOf[Seq[Col]])
    case Seq(_: String, _*) =>
      val reps: Seq[Col] = cols.flatMap{
        case "*" => unnestWildcardColumns(this)
        case col: String => Seq(BaseCol(getNestedWrapperId(this), col))
      }
      Select(this, reps)
  }

  private def unnestWildcardColumns(w: WrappedDataframe): Array[Col] = w match {
    case w:Wrapper => w.in.columns.map(z => BaseCol(getNestedWrapperId(this), z))
    case j: Join => unnestWildcardColumns(j.self) ++ unnestWildcardColumns(j.d2)
    //TODO - handle all operations
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

  def filter[A](col: A): WrappedDataframe = col match {
    case c: Col => Filter(this, c)
  }

  def where[A](col: A): WrappedDataframe = col match {
    case c: Col => Filter(this, c)
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
    val rep = this

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
    case Wrapper(in, e) => IMap(e -> in)
    case Merge(e1, e2) => getCtx(e1) ++ getCtx(e2)
    case Join(e1, e2, _) => getCtx(e1) ++ getCtx(e2)
    case Drop(e1, _) => getCtx(e1)
    case DropDuplicates(e1) => getCtx(e1)
    case GroupBy(e1, _) => getCtx(e1)
    case Reduce(e1, _, _) => getCtx(e1)
    case Select(e1, _) => getCtx(e1)
    case Map(e1, _, _) => getCtx(e1)
    case FlatMap(e1, Fun(_, out: Rep)) => getCtx(e1) ++ (out match {
      case RepRowInst(vals) => vals.flatMap(f => getCtx(f)).toMap
      case _ => IMap.empty
    })
    case RepElem(_, _) => IMap.empty
    case If(_, _, _) => IMap.empty
    case Filter(e1, _) => getCtx(e1)
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
  }

  /**
   * This takes the WrappedDataset as a Wrapper that is due to be joined and updates columns that may have been renamed during Wrapping process due to duplicate column names.
   * Currently only used in Joins.
   */
  private def handleDupColumnNames(w: WrappedDataframe): WrappedDataframe = w match {
    case w: Wrapper =>
      val columnNames = JoinContext.getMappingsForStr(w.str)
      val updatedNestedDf = columnNames.zip(w.in.columns).foldLeft(w.in) {
        case (accDf, (newCol, oldCol)) =>
          accDf.withColumnRenamed(oldCol, newCol)
      }

      val updatedWrapper = Wrapper(updatedNestedDf, w.str)
      updatedWrapper
    case Select(w, cols) => Select(handleDupColumnNames(w), cols)
    //TODO - check how duplicates are handles in nested Joins
    case Join(e1, e2, cond) => Join(handleDupColumnNames(e1), handleDupColumnNames(e2), cond)
  }

  /**
   * In the case of nested Equi-Joins its necessary to have the Dataset Identifier of the base nested Wrapper.
   * This recursively extracts that ID from the [[this]] object
   */
  private def getNestedWrapperId(df: WrappedDataframe): String = df match {
    case w: Wrapper => w.str
    case o: Operation => o match {
      case Join(lhs, _, _) => getNestedWrapperId(lhs)
      case Select(self, _) => getNestedWrapperId(self)
      case Filter(self, _) => getNestedWrapperId(self)
    }
  }

}


