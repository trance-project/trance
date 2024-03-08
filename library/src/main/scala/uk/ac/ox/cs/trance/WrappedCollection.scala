package uk.ac.ox.cs.trance

import framework.common.{ArrayType, BagType, BoolType, DoubleType, IntType, LongType, StringType, TupleAttributeType, TupleType}
import framework.plans.{BaseNormalizer, CExpr, Finalizer, NRCTranslator, Printer, Unnester}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{AbstractDataType, DataType, DataTypes, IntegerType, StructField, StructType, ArrayType => SparkArrayType}
import org.apache.spark.sql.{Column, DataFrame, Encoder, SparkSession, functions, types, Row => SparkRow}
import uk.ac.ox.cs.trance.utilities.JoinContext

import scala.collection.immutable.{Map => IMap}

trait WrappedCollection extends TraversableRep with NRCTranslator {

  /**
   * The constructor for WrappedDataframe is made to mimic Spark's [[Column]] syntax. <br>
   * df("column") returns a column in this case a [[Col]]
   * that can be used as a way of specifying columns contained in [[WrappedCollection]]s for use in [[Select]], [[Join]], [[Drop]]...
   */

  def schema: DataType = getSchema(this)

  def apply(colName: String): Col = {
    BaseCol(getCtx(this).keys.head, colName)
  }


  private def getSchema(r: Rep): DataType =  r match {
    case w: Wrapper => w.in.schema
    case s: Sym => s.schema
    case f: FlatMap => getSchema(f.f)
    case m: Map => getSchema(m.f)
    case s: Select => getSchema(s.self)
    case j: Join => StructType(getSchema(j.self).asInstanceOf[StructType].fields ++ getSchema(j.d2).asInstanceOf[StructType].fields)
    case r: Reduce => getSchema(r.self)
    case fn: Fun => getSchema(fn.out)
    case rp: RepProjection => getSchema(rp.r) match {
      case s: StructType =>
        val outGoingRepProjection = s(rp.name).dataType
        outGoingRepProjection
      case _ => sys.error("Incorrect type")
    }
    case RowIfThen(_, e1) => getSchema(e1)
    case RowIfThenElse(_, e1, e2) => getSchema(e1)
    case e: Equality => null // TODO - handle
    case RepRow(vals) =>
      StructType(vals.map(x => StructField(x._1, x._2 match {
        case _: MathCol => StructType(Seq(StructField(x._1, getSchema(x._2).asInstanceOf[StructType].fields.head.dataType))) // Hacky way of handling the right naming for MathCols
        case _ => getSchema(x._2)
      })))
    case RepSeq(rr@_*) if rr.nonEmpty =>
      SparkArrayType(getSchema(rr.head))
    case Drop(e1, _) => getSchema(e1)
    case m: MathCol => getSchema(m.rhs) // TODO - How can we handle naming with MathCol

  }

  def map(f: Sym => Rep): WrappedCollection =  {
      val symID = utilities.Symbol.fresh()
      val sym = Sym(symID, schema)
      val out = f(sym)
      val fun = Fun(sym, out)
      Map(this, fun)
  }

  def flatMap(f: Sym => TraversableRep): WrappedCollection = {
    val symID = utilities.Symbol.fresh()
      val sym = Sym(symID, schema)
      val out = f(sym)
      val fun = Fun(sym, out)
      FlatMap(this, fun)

  }

//  private def getNestedRelatedColumns(w: Rep): Seq[RepElem] = w match {
//    case r: RepElem => Seq(r)
//    case w: Wrapper => Seq.empty
//    case FlatMap(self, Fun(in, out)) => getNestedRelatedColumns(self) ++ getNestedRelatedColumns(out)
//    case As(e1, str) => (getNestedRelatedColumns(e1))
//    case Sym(rows) => rows.flatMap(f => getNestedRelatedColumns(f))
//    case RepRowInst(vals) => vals.flatMap(f => getNestedRelatedColumns(f))
//    case If(cond, thenBranch, elseBranch) => getNestedRelatedColumns(cond) ++ getNestedRelatedColumns(thenBranch) ++ getNestedRelatedColumns(elseBranch)
//    case Equality(lhs, rhs) => getNestedRelatedColumns(lhs) ++ getNestedRelatedColumns(rhs)
//  }

  def union(df: WrappedCollection): WrappedCollection = {
    Merge(this, df)
  }

  def join(df: Wrapper): WrappedCollection = {
    Join(this, handleDupColumnNames(df), None)
  }

  def join(df: WrappedCollection, joinCond: Rep): WrappedCollection = {
    Join(this, handleDupColumnNames(df), Some(handleDupColumnNames(joinCond)))
  }

  /**
   * Join in the format df.join(df2, "columnName").
   * Joins the datasets on equality of the given column name(s) present in each dataset and then drops the duplicate.
   * In this case the matched column from the second dataset.
   */
  def join(df: Wrapper, withColumns: String): WrappedCollection = {
    val joinColumns = EquiJoinCol(getNestedWrapperId(this), df.str, withColumns)
    Join(this, handleDupColumnNames(df), Some(joinColumns))
  }

  /**
   * Just like join(df: Wrapper, withColumns: String) but its possible to specify multiple columns in the join condition.
   */
  def join(df: Wrapper, withColumns: Seq[String]): WrappedCollection = {
    val joinColumns = EquiJoinCol(getNestedWrapperId(this), df.str, withColumns:_*)
    Join(this, handleDupColumnNames(df), Some(joinColumns))
  }

  def dropDuplicates(): WrappedCollection = {
    DropDuplicates(this)
  }

 // TODO - handle col("*") condition
  def select[A](cols: A*): WrappedCollection = cols match {
    case Seq(_: Col, _*) => Select(this, cols.asInstanceOf[Seq[Col]])
    case Seq(_: String, _*) =>
      val reps: Seq[Col] = cols.flatMap{
        case "*" => unnestWildcardColumns(this)
        case col: String => Seq(BaseCol(getNestedWrapperId(this), col))
      }
      Select(this, reps)
  }

  private def unnestWildcardColumns(w: WrappedCollection): Array[Col] = w match {
    case w:Wrapper => w.in.columns.map(z => BaseCol(getNestedWrapperId(this), z))
    case j: Join => unnestWildcardColumns(j.self) ++ unnestWildcardColumns(j.d2)
    //TODO - handle all operations
  }

  def groupBy(cols: String*): GroupBy = {
    GroupBy(this, cols.toList)
  }

  def drop(col: String, cols: String*): WrappedCollection = {
    Drop(this, col +: cols)
  }

  def drop(col: Column, cols: Column*): WrappedCollection = {
    Drop(this, col.toString() +: cols.map(_.toString))
  }

  def filter[A](col: A): WrappedCollection = col match {
    case c: Col => Filter(this, c)
  }

  def where[A](col: A): WrappedCollection = col match {
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
    case Map(e1, Fun(_, out: Rep)) => getCtx(e1) ++ getCtx(out)
    case FlatMap(e1, Fun(_, out: Rep)) => getCtx(e1) ++ getCtx(out)
    case RepSeq(reps@_*) => reps.flatMap(f => getCtx(f)).toMap
    case _: Sym => IMap.empty
    case As(e1, _) => getCtx(e1)
    case _: RepElem => IMap.empty
    case rp: RepProjection => getCtx(rp.r)
    case RowIfThenElse(_, e1, e2) => getCtx(e1) ++ getCtx(e2)
    case RowIfThen(_, e1) => getCtx(e1)
    case Equality(e1, e2) => getCtx(e1) ++ getCtx(e2)
    case Filter(e1, _) => getCtx(e1)
    case m: MathCol => getCtx(m.lhs) ++ getCtx(m.rhs)
    case RepRow(elems) => elems.flatMap(f => getCtx(f._2)).toMap
    case _ : Literal => IMap.empty
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
  private def handleDupColumnNames(w: WrappedCollection): WrappedCollection = w match {
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
  private def getNestedWrapperId(df: WrappedCollection): String = df match {
    case w: Wrapper => w.str
    case o: Operation => o match {
      case Join(lhs, _, _) => getNestedWrapperId(lhs)
      case Select(self, _) => getNestedWrapperId(self)
      case Filter(self, _) => getNestedWrapperId(self)
      case Map(e1, _) => getNestedWrapperId(e1)
    }
  }

}

