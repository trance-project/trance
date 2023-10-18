package uk.ac.ox.cs.trance

import uk.ac.ox.cs.trance.utilities.SparkUtil.getSparkSession
import framework.common.{BoolType, DoubleType, IntType, LongType, OpDivide, OpMinus, OpMod, OpMultiply, OpPlus, StringType, Type}
import framework.plans.{AddIndex, CDeDup, CExpr, Comprehension, Constant, EmptySng, Equals, Gt, Gte, If, InputRef, Lt, Lte, MathOp, Not, Projection, Record, Variable, Join => CJoin, Merge => CMerge, Project => CProject, Reduce => CReduce, Select => CSelect, Sng => CSng}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{BinaryOperator, EqualTo, Expression, And => SparkAnd, GreaterThan => SparkGreaterThan, GreaterThanOrEqual => SparkGreaterThanOrEqual, LessThan => SparkLessThan, LessThanOrEqual => SparkLessThanOrEqual, Not => SparkNot, Or => SparkOr}
import org.apache.spark.sql.functions.{expr, monotonically_increasing_id}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row}
import scala.collection.immutable.{Map => IMap}

object PlanConverter {

  /**
   * Recursively converts a normalised and unnested plan expression [[CExpr]] into a Spark Dataframe
   * @param ctx contains a mapping from the identifier -> Dataframe object(s) created in [[Wrapper]]
   *            <br> The ctx will also be extended to contain mapping between [[Variable]] identifier and [[Row]] while processing [[Projection]] & [[Comprehension]]
   */
  def convert[T](cExpr: CExpr, ctx: IMap[String, Any]): T = cExpr match {
    case Projection(e1, v, p, fields) =>
      val c1 = convert(e1, ctx).asInstanceOf[DataFrame]
      val outputSchema = createStructFields(p)

      val l = c1.flatMap { z => Seq(convert(p, ctx ++ getProjectionBinding(e1, z)).asInstanceOf[Row]) }(RowEncoder.apply(outputSchema))
      l.asInstanceOf[T]
    case CJoin(left, v, right, v2, cond, fields) =>
      val i1 = convert(left, ctx).asInstanceOf[DataFrame]
      val i2 = convert(right, ctx).asInstanceOf[DataFrame]

      val c = toSparkCond(cond)
      val col = expr(c)
      performJoin(i1, i2, col).asInstanceOf[T]
    case CMerge(e1, e2) =>
      convert(e1, ctx).asInstanceOf[DataFrame].union(convert(e2, ctx).asInstanceOf[DataFrame]).asInstanceOf[T]
    case CSelect(x, v, p) =>
      convert(x, ctx).asInstanceOf[T]
    case CDeDup(in) =>
      val i1 = convert(in, ctx).asInstanceOf[DataFrame]
      i1.dropDuplicates().asInstanceOf[T]
    case InputRef(data, tp) =>
      ctx(data).asInstanceOf[T]
    case Variable(name, tp) =>
      ctx(name).asInstanceOf[T]
    case AddIndex(e, name) =>
      val df = convert(e, ctx).asInstanceOf[DataFrame].withColumn(name, monotonically_increasing_id)
      df.asInstanceOf[T]
    case CProject(e1, field) =>
      val row = convert(e1, ctx).asInstanceOf[Row]
      val projectionFieldIndex = row.schema.fieldIndex(field)
      val columnValue = row.get(projectionFieldIndex)
      Row.fromSeq(Array(columnValue)).asInstanceOf[T]
    case CSng(e1) =>
      convert(e1, ctx)
    case Comprehension(e1, v, p, e) =>
      val c1 = convert(e1, ctx).asInstanceOf[DataFrame]
      // TODO - take schema from e for RowEncoder
      val k = c1.flatMap(z => Seq(convert(e, ctx + (v.name -> z)).asInstanceOf[Row]))(RowEncoder.apply(c1.schema))
      k.asInstanceOf[T]
    case Record(fields) =>
      val t = fields.map { x =>
        convert(x._2, ctx).asInstanceOf[Row]
      }.toArray
      val combinedRow = t.flatMap(row => row.toSeq)
      Row.fromSeq(combinedRow).asInstanceOf[T]
    case CReduce(in, v, keys, values) =>
      val d1 = convert(in, ctx).asInstanceOf[DataFrame]
      d1.groupBy(keys.head, keys.tail: _*).sum(values: _*).asInstanceOf[T]
    case MathOp(op, e1, e2) =>
      // TODO - need to handle when x & y could be a Double/Float
      val x = convert(e1, ctx).asInstanceOf[Row].getInt(0)
      val y = convert(e2, ctx).asInstanceOf[Row].getInt(0)
      op match {
        case OpMultiply => Row.fromSeq(Seq(x * y)).asInstanceOf[T]
        case OpPlus => Row.fromSeq(Seq(x + y)).asInstanceOf[T]
        case OpMinus => Row.fromSeq(Seq(x - y)).asInstanceOf[T]
        case OpMod => Row.fromSeq(Seq(x % y)).asInstanceOf[T]
        case OpDivide => Row.fromSeq(Seq(x / y)).asInstanceOf[T]
      }
    case Equals(e1, e2) =>
      val lhs = convert(e1, ctx).asInstanceOf[Row]
      val rhs = convert(e2, ctx).asInstanceOf[Row]
      if(lhs.get(0)==rhs.get(0)) true.asInstanceOf[T] else false.asInstanceOf[T]
    case Not(e1) =>
      val test = !convert(e1, ctx).asInstanceOf[Boolean]
      test.asInstanceOf[T]
      // TODO the following cases need to handle Double & Long. Currently only handles Integer.
    case Lt(e1, e2) =>
      val lhs = convert(e1, ctx).asInstanceOf[Row]
      val rhs = convert(e2, ctx).asInstanceOf[Row]
      if (lhs.getInt(0) < rhs.getInt(0)) true.asInstanceOf[T] else false.asInstanceOf[T]
    case Lte(e1, e2) =>
      val lhs = convert(e1, ctx).asInstanceOf[Row]
      val rhs = convert(e2, ctx).asInstanceOf[Row]
      if (lhs.getInt(0) <= rhs.getInt(0)) true.asInstanceOf[T] else false.asInstanceOf[T]
    case Gt(e1, e2) =>
      val lhs = convert(e1, ctx).asInstanceOf[Row]
      val rhs = convert(e2, ctx).asInstanceOf[Row]
      if (lhs.getInt(0) > rhs.getInt(0)) true.asInstanceOf[T] else false.asInstanceOf[T]
    case Gte(e1, e2) =>
      val lhs = convert(e1, ctx).asInstanceOf[Row]
      val rhs = convert(e2, ctx).asInstanceOf[Row]
      if (lhs.getInt(0) >= rhs.getInt(0)) true.asInstanceOf[T] else false.asInstanceOf[T]
    case If(cond, e1, e2) => if(convert(cond, ctx)) convert(e1, ctx) else convert(e2.get, ctx)
    case EmptySng => getSparkSession.emptyDataFrame.asInstanceOf[T]
    case Constant(e) => Row(e).asInstanceOf[T]
    case s@_ =>
      sys.error("Unsupported: " + s)
  }

  /**
   * This function is used while converting a [[Projection]] <br>
   * It binds the [[Variable]]'s identifier to the corresponding [[Row]] <br><br>
   * This mapping is then added to the ctx in convert() so that when converting the [[Projection.pattern]]
   * it's possible to retrieve this [[Row]] from the ctx when unnesting a [[Record]]
   */

  private def getProjectionBinding(e1: CExpr, row: Row): IMap[String, Any] = e1 match {
    case CSelect(_, v, _) => IMap(v.name -> row)
    case CJoin(left, _, right, _, _, _) => getProjectionBinding(left, row) ++ getProjectionBinding(right, row)
    case x@_ => sys.error("Unhandled projection type: " + x)
  }

  /**
   *  The join condition needs to be formatted for spark <br>
   *  && -> AND <br>
   *  || -> OR
   */
  private def toSparkCond(c: CExpr): String = {
    c.vstr.replaceAll("&&", " AND ").replaceAll("\\|\\|", " OR ")
  }

  /**
   * These 2 functions (performJoin() & processSparkExpression()) are to allow for self joins <br> eg. df1.join(df1, ...).
   * <br> [[Column]] names may be the same so it's necessary to explicitly state the join: <br> df(columnName) === df2(columnName) <br>
   * To avoid ambiguous conditions like columnName == columnName occurring.
   *
   * In the performJoin() function we check if there is a self join happening,
   * if not we perform a normal join otherwise we pass onto our processSparkExpression function to prevent ambiguity.
   */

  private def performJoin(i1: DataFrame, i2: DataFrame, condition: Column): DataFrame = condition.expr match {
    case BinaryOperator(e) =>
      if (!i1.except(i2).isEmpty) i1.join(i2, condition)
      else i1.join(i2, processSparkExpression(i1, i2, condition.expr))
    case s@_ => sys.error("Unhandled join condition: " + s)
  }

  private def processSparkExpression(i1: DataFrame, i2: DataFrame, expression: Expression): Column = expression match {
    case SparkAnd(left, right) =>
      val leftProcessed = processSparkExpression(i1, i2, left)
      val rightProcessed = processSparkExpression(i1, i2, right)
      leftProcessed && rightProcessed
    case SparkOr(left, right) =>
      val leftProcessed = processSparkExpression(i1, i2, left)
      val rightProcessed = processSparkExpression(i1, i2, right)
      leftProcessed || rightProcessed
    case EqualTo(left, right) => i1(left.sql) === i2(right.sql)
    case SparkLessThan(left, right) => i1(left.sql) < i2(right.sql)
    case SparkLessThanOrEqual(left, right) => i1(left.sql) <= i2(right.sql)
    case SparkGreaterThan(left, right) => i1(left.sql) > i2(right.sql)
    case SparkGreaterThanOrEqual(left, right) => i1(left.sql) >= i2(right.sql)
    case SparkNot(child) => !processSparkExpression(i1, i2, child)
  }

  /**
   * Used when creating the output schema when converting a [[Projection]]
   * @param p  Is the [[Projection.pattern]]
   * @return A [[StructType]] schema which is used for the output [[DataFrame]]
   */

  private def createStructFields(p: CExpr): StructType = p match {
    case Record(fields) =>
      StructType(fields.map(f => StructField(f._1, getStructDataType(f._2.tp))).toSeq)
    case s@_ => sys.error(s + " is not a valid pattern")
  }

  /**
   * Conversion from NRC/Plan [[Type]] to Spark [[DataType]] used when creating [[StructType]]
   */
  private def getStructDataType(c: Type): DataType = c match {
    case IntType => DataTypes.IntegerType
    case BoolType => DataTypes.BooleanType
    case StringType => DataTypes.StringType
    case DoubleType => DataTypes.DoubleType
    case LongType => DataTypes.LongType
  }
}
