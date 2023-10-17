package org.trance.nrclibrary

import com.sun.org.apache.xpath.internal.operations.NotEquals
import org.trance.nrclibrary.utilities.SparkUtil.getSparkSession
import framework.common.{BagType, BoolType, DoubleType, IntType, LongType, OpCmp, OpDivide, OpEq, OpGe, OpGt, OpMinus, OpMod, OpMultiply, OpNe, OpPlus, StringType, TupleAttributeType, TupleType, Type}
import framework.plans.{AddIndex, BaseNormalizer, CDeDup, CExpr, Comprehension, Constant, EmptySng, Equals, Finalizer, If, InputRef, MathOp, NRCTranslator, Nest, Printer, Projection, Record, Unnester, Variable, Join => CJoin, Merge => CMerge, Project => CProject, Reduce => CReduce, Select => CSelect, Sng => CSng}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{BinaryComparison, BinaryOperator, EqualTo, Expression, GenericRowWithSchema, And => SparkAnd, GreaterThan => SparkGreaterThan, GreaterThanOrEqual => SparkGreaterThanOrEqual, LessThan => SparkLessThan, LessThanOrEqual => SparkLessThanOrEqual, Not => SparkNot, Or => SparkOr}
import org.apache.spark.sql.functions.{expr, monotonically_increasing_id}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, functions, types}
import org.trance.nrclibrary.utilities.DropContext

object PlanConverter {
  def planToDF[T](cExpr: CExpr, ctx: Map[String, Any]): T = cExpr match {
    case Projection(e1, v, p, fields) =>
      val c1 = planToDF(e1, ctx).asInstanceOf[DataFrame]
      val outputSchema = StructType(createStructFields(p))

      val l = c1.flatMap { z => Seq(planToDF(p, ctx ++ getProjectionBinding(e1, z)).asInstanceOf[Row]) }(RowEncoder.apply(outputSchema))
      l.asInstanceOf[T]
    case CJoin(left, v, right, v2, cond, fields) =>
      val i1 = planToDF(left, ctx).asInstanceOf[DataFrame]
      val i2 = planToDF(right, ctx).asInstanceOf[DataFrame]

      val c = toSparkCond(cond)
      val col = expr(c)
      performJoin(i1, i2, col).asInstanceOf[T]
    case CMerge(e1, e2) =>
      planToDF(e1, ctx).asInstanceOf[DataFrame].union(planToDF(e2, ctx).asInstanceOf[DataFrame]).asInstanceOf[T]
    case CSelect(x, v, p) =>
        val df = planToDF(x, ctx).asInstanceOf[DataFrame]
      df.asInstanceOf[T]
    case CDeDup(in) =>
      val i1 = planToDF(in, ctx).asInstanceOf[DataFrame]
      i1.dropDuplicates().asInstanceOf[T]
    case InputRef(data, tp) =>
      val df = ctx(data)
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
      // TODO - x & y could be a Double/Float
      val x = planToDF(e1, ctx).asInstanceOf[Row].getInt(0)
      val y = planToDF(e2, ctx).asInstanceOf[Row].getInt(0)
      op match {
        case OpMultiply => Row.fromSeq(Seq(x * y)).asInstanceOf[T]
        case OpPlus => Row.fromSeq(Seq(x + y)).asInstanceOf[T]
        case OpMinus => Row.fromSeq(Seq(x - y)).asInstanceOf[T]
        case OpMod => Row.fromSeq(Seq(x % y)).asInstanceOf[T]
        case OpDivide => Row.fromSeq(Seq(x / y)).asInstanceOf[T]
      }
    case Equals(e1, e2) =>
//      val df = planToDF(e1, ctx).asInstanceOf[DataFrame]
//      val constant = planToDF(e2, ctx)
//      df.select(e1.asInstanceOf[CProject].field + "===" + constant).asInstanceOf[T]
      val lhs = planToDF(e1, ctx).asInstanceOf[Row]
      val rhs = planToDF(e2, ctx).asInstanceOf[Row]
      if(lhs.getInt(0)==rhs.getInt(0)) true.asInstanceOf[T] else false.asInstanceOf[T]
    case If(cond, e1, e2) => if(planToDF(cond, ctx)) planToDF(e1, ctx) else planToDF(e2.get, ctx)
    case EmptySng => getSparkSession.emptyDataFrame.asInstanceOf[T]
    case Constant(e) => Row(e).asInstanceOf[T]
    case s@_ =>
      sys.error("Unsupported: " + s)
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

  private def getOp(e: Col[_]): OpCmp = e match {
    case Equality(_, _) => OpEq
    case Inequality(_, _) => OpNe
    case GreaterThan(_, _) => OpGt
    case GreaterThanOrEqual(_, _) => OpGe
    case LessThan(_, _) => OpGt
    case LessThanOrEqual(_, _) => OpGe
  }

  private def getProjectionBinding(e1: CExpr, row: Row): Map[String, Any] = e1 match {
    case CSelect(_, v, _) => Map(v.name -> row)
    case CJoin(left, v, right, v2, cond, fields) => getProjectionBinding(left, row) ++ getProjectionBinding(right, row)
    case x@_ => sys.error("Unhandled projection type: " + x)
  }

  private def toSparkCond(c: CExpr): String = {
    c.vstr.replaceAll("&&", " AND ").replaceAll("\\|\\|", " OR ")
  }

  //TODO - more than one duplicate, suffix incrementing?

  private def handleDuplicateInputSchema(schema: StructType): StructType = {
    var fieldNamesSeen = Set[String]()
    var newFields = Seq[StructField]()

    schema.fields.foreach { field =>
      val fieldName = field.name
      val newFieldName = if (fieldNamesSeen.contains(fieldName)) s"${fieldName}_2" else fieldName
      fieldNamesSeen += fieldName
      newFields = newFields :+ StructField(newFieldName, field.dataType, field.nullable)
    }

    StructType(newFields)
  }

  // This method is to allow for self joins ie. df1.join(df1, ...)
  // Column names may be the same so it's necessary to explicitly state the join columns df(columnName) === df2(columnName)
  // To avoid ambiguous conditions like columnName == columnName occurring

  // In the performJoin() function we check if there is a self join happening,
  // if not we perform a normal join otherwise we pass onto our processSparkExpression function to prevent ambiguity.
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

  private def createStructFields(p: CExpr): Seq[StructField] = p match {
    case Record(fields) =>
      fields.map(f => StructField(f._1, getStructDataType(f._2.tp))).toSeq
    case s@_ => sys.error(s + " is not a valid pattern")
  }

  private def getStructDataType(c: Type): DataType = c match {
    case IntType => DataTypes.IntegerType
    case BoolType => DataTypes.BooleanType
  }
}
