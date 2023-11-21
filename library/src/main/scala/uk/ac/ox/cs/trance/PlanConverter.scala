package uk.ac.ox.cs.trance

import uk.ac.ox.cs.trance.utilities.SparkUtil.getSparkSession
import framework.common.{ArrayType, BagCType, BoolType, DoubleType, IntType, LongType, OpArithmetic, OpDivide, OpMinus, OpMod, OpMultiply, OpPlus, RecordCType, StringType, Type}
import framework.plans.{AddIndex, And, CDeDup, CExpr, Comprehension, Constant, EmptySng, Equals, Gt, Gte, If, InputRef, Lt, Lte, MathOp, Not, Or, Projection, Record, Variable, Join => CJoin, Merge => CMerge, Project => CProject, Reduce => CReduce, Select => CSelect, Sng => CSng}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{BinaryOperator, EqualTo, Expression, And => SparkAnd, GreaterThan => SparkGreaterThan, GreaterThanOrEqual => SparkGreaterThanOrEqual, LessThan => SparkLessThan, LessThanOrEqual => SparkLessThanOrEqual, Literal => SparkLiteral, Not => SparkNot, Or => SparkOr}
import org.apache.spark.sql.functions.{col, expr, monotonically_increasing_id}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType, ArrayType => SparkArrayType}
import org.apache.spark.sql.{Column, DataFrame, Row, functions}

import scala.annotation.tailrec
import scala.collection.immutable.{Map => IMap}

object PlanConverter {

  /**
   * Recursively converts a normalised and unnested plan expression [[CExpr]] into a Spark Dataframe
   *
   * @param ctx contains a mapping from the identifier -> Dataframe object(s) created in [[Wrapper]]
   *            <br> The ctx will also be extended to contain mapping between [[Variable]] identifier and [[Row]] while processing [[Projection]] & [[Comprehension]]
   */

  def convert[T](cExpr: CExpr, ctx: IMap[String, Any]): T = cExpr match {
    case Projection(e1, v, p, fields) =>
      val c1 = convert(e1, ctx).asInstanceOf[DataFrame]

      val outputSchema = createStructFields(p)
      println(outputSchema)

      val l = c1.flatMap { z => Seq(convert(p, ctx ++ getProjectionBinding(e1, z)).asInstanceOf[Row]) }(RowEncoder.apply(outputSchema))
      l.asInstanceOf[T]
    case CJoin(left, v, right, v2, cond, fields) =>
      val i1 = convert(left, ctx).asInstanceOf[DataFrame]
      val i2 = convert(right, ctx).asInstanceOf[DataFrame]

      val c = toSparkCond(cond)

      val joined = performJoin(i1, i2, c)
      handleSelfJoinColumns(joined).asInstanceOf[T]
    case CMerge(e1, e2) =>
      convert(e1, ctx).asInstanceOf[DataFrame].union(convert(e2, ctx).asInstanceOf[DataFrame]).asInstanceOf[T]
    case CSelect(x, v, p) =>
      val k = convert(x, ctx).asInstanceOf[DataFrame]
      p match {
        case Constant(true) => k.asInstanceOf[T]
        case _: Equals | _: Lte | _: Lt | _: Gte | _: Gt | _: Not | _: Or | _: And =>
          k.filter { z => convert(p, ctx + (v.name -> z)).asInstanceOf[Boolean] }.asInstanceOf[T]
        case s@_ => sys.error("Invalid Filtering in CSelect: " + s)
      }
    case CDeDup(in) =>
      val i1 = convert(in, ctx).asInstanceOf[DataFrame]
      i1.dropDuplicates().asInstanceOf[T]
    case InputRef(data, tp) =>
      ctx(data).asInstanceOf[T]
    case Variable(name, tp) =>
      val c = ctx(name)
      c.asInstanceOf[T]
    case AddIndex(e, name) =>
      val df = convert(e, ctx).asInstanceOf[DataFrame].withColumn(name, monotonically_increasing_id)
      df.asInstanceOf[T]
    case CProject(e1, field) =>
      val row = convert(e1, ctx).asInstanceOf[Row]
      val projectionFieldIndex = row.schema.fieldIndex(field)
      val columnValue = row.get(projectionFieldIndex)
      val r = Row(columnValue)
      r.asInstanceOf[T]
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
      val r = Row.fromSeq(combinedRow)
      r.asInstanceOf[T]
    case CReduce(in, v, keys, values) =>
      val d1 = convert(in, ctx).asInstanceOf[DataFrame]
      d1.groupBy(keys.head, keys.tail: _*).sum(values: _*).asInstanceOf[T]
    case MathOp(op, e1, e2) =>
      // TODO - need to handle when x & y could be a Double/Float
      val x = convert(e1, ctx).asInstanceOf[Row].get(0)
      val y = convert(e2, ctx).asInstanceOf[Row].get(0)
      mathResolver(op, x, y).asInstanceOf[T]
    case Equals(e1, e2) =>
      val lhs = convert(e1, ctx).asInstanceOf[Row]
      val rhs = convert(e2, ctx).asInstanceOf[Row]
      if (lhs.get(0) == rhs.get(0)) true.asInstanceOf[T] else false.asInstanceOf[T]
    case Not(e1) =>
      (!convert(e1, ctx).asInstanceOf[Boolean]).asInstanceOf[T]
    case And(e1, e2) =>
      val lhs = convert(e1, ctx).asInstanceOf[Boolean]
      val rhs = convert(e2, ctx).asInstanceOf[Boolean]
      if (lhs && rhs) true.asInstanceOf[T] else false.asInstanceOf[T]
    case Or(e1, e2) =>
      val lhs = convert(e1, ctx).asInstanceOf[Boolean]
      val rhs = convert(e2, ctx).asInstanceOf[Boolean]
      if (lhs || rhs) true.asInstanceOf[T] else false.asInstanceOf[T]
    case l@Lt(e1, e2) =>
      if (mathComparator(l, convert(e1, ctx).asInstanceOf[Row].get(0), convert(e2, ctx).asInstanceOf[Row].get(0))) {
        true.asInstanceOf[T]
      } else false.asInstanceOf[T]
    case l@Lte(e1, e2) =>
      if (mathComparator(l, convert(e1, ctx).asInstanceOf[Row].get(0), convert(e2, ctx).asInstanceOf[Row].get(0))) {
        true.asInstanceOf[T]
      } else false.asInstanceOf[T]
    case g@Gt(e1, e2) =>
      if (mathComparator(g, convert(e1, ctx).asInstanceOf[Row].get(0), convert(e2, ctx).asInstanceOf[Row].get(0))) {
        true.asInstanceOf[T]
      } else false.asInstanceOf[T]
    case g@Gte(e1, e2) =>
      if (mathComparator(g, convert(e1, ctx).asInstanceOf[Row].get(0), convert(e2, ctx).asInstanceOf[Row].get(0))) {
        true.asInstanceOf[T]
      } else false.asInstanceOf[T]
    case If(cond, e1, e2) => if (convert(cond, ctx)) convert(e1, ctx) else convert(e2.get, ctx)
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
   * Handles a few fringe cases when converting from CExpr to Spark Condition Column.
   * <br>
   * In case of an Equality with a column and a String Literal, the column must be explicitly defined
   * as a col in the Spark [[Column]] to avoid UnresolvedAttribute in translateColumn().
   * <br>
   * NRC doesn't explicitly support LessThan & LessThanOrEqual.
   * They are represented by swapping the Data and using GreaterThan or GreaterThanOrEqual during NRCConverter.
   * So this function will check if the datasets were swapped then represent those greaterThan/greaterThanOrEqual
   * as a lessThan/lessThanOrEqual when converting back to spark.
   * <br>
   *
   */
  private def toSparkCond(c: CExpr): Column = c match {
    case e: Equals =>
      val left = e.e1.vstr
      e.e2 match {
        case c: Constant => col(left) === c.data
        case _ => expr(formatCond(c))
      }
    case n: Not => functions.not(toSparkCond(n.e1))
    case gt: Gt =>
      gt match {
        case Gt(e1, e2) if e1.isInstanceOf[Constant] || e2.isInstanceOf[Constant] =>
          expr(formatCond(c))
        case g@Gt(CProject(e1: Variable, _), CProject(e2: Variable, _)) if e1.name > e2.name =>
          col(g.e2.vstr) < col(g.e1.vstr)
        case _ =>
          expr(formatCond(c))
      }
    case gte: Gte =>
      gte match {
        case Gte(e1, e2) if e1.isInstanceOf[Constant] || e2.isInstanceOf[Constant] =>
          expr(formatCond(c))
        case g@Gte(CProject(e1: Variable, _), CProject(e2: Variable, _)) if e1.name > e2.name =>
          col(g.e2.vstr) <= col(g.e1.vstr)
        case _ =>
          expr(formatCond(c))
      }
    case _ => expr(formatCond(c))
  }

  /**
   * The join condition needs to be formatted for spark <br>
   * && -> AND <br>
   * || -> OR
   */
  private def formatCond(c: CExpr): String = {
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
      i1.join(i2, processSparkExpression(i1, i2, condition.expr))

    //      if(i1.schema.equals(i2.schema) && i1.rdd.subtract(i2.rdd).isEmpty) {
    //        i1.join(i2, condition)
    //      }
    //      else {
    //        i1.join(i2, processSparkExpression(i1, i2, condition.expr))
    //
    //      }
    case SparkNot(e1) => i1.join(i2, processSparkExpression(i1, i2, condition.expr))
    case s: SparkLiteral => {
      if (s.value == true) {
        i1.join(i2)
      } else {
        sys.error("Invalid Join Condition: " + s)
      }
    }
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
    case EqualTo(left, right: SparkLiteral) => i1(left.sql) === right
    case EqualTo(left, right) => i1(left.sql) === i2(right.sql)
    case SparkLessThan(left, right: SparkLiteral) => i1(left.sql) < right
    case SparkLessThan(left, right) => i1(left.sql) < i2(right.sql)
    case SparkLessThanOrEqual(left, right: SparkLiteral) => i1(left.sql) <= right
    case SparkLessThanOrEqual(left, right) => i1(left.sql) <= i2(right.sql)
    case SparkGreaterThan(left, right: SparkLiteral) => i1(left.sql) > right
    case SparkGreaterThan(left, right) => i1(left.sql) > i2(right.sql)
    case SparkGreaterThanOrEqual(left, right: SparkLiteral) => i1(left.sql) >= right
    case SparkGreaterThanOrEqual(left, right) => i1(left.sql) >= i2(right.sql)
    case SparkNot(child) => !processSparkExpression(i1, i2, child)
  }

  /**
   * Used when creating the output schema when converting a [[Projection]]
   *
   * @param p Is the [[Projection.pattern]]
   * @return A [[StructType]] schema which is used for the output [[DataFrame]]
   *
   *         Also handles a unique case where spark promotes operations of Integer / Long to Double Type
   */

  private def createStructFields(p: CExpr): StructType = p match {
    case Record(fields) =>
      StructType(fields.map(f => f._2 match {
        case m: MathOp => if (checkDivisionExists(m)) {
          StructField(f._1, getStructDataType(DoubleType))
        } else {
          StructField(f._1, getStructDataType(f._2.tp))
        }
        case _ => StructField(f._1, getStructDataType(f._2.tp))
      }).toSeq)
    case If(_, e1, _) => createStructFields(e1)
    case CSng(e1) => createStructFields(e1)
    case s@_ => sys.error(s + " is not a valid pattern")
  }

  private def checkDivisionExists(op: MathOp): Boolean = op match {
    case MathOp(OpDivide, _, _) => true
    case MathOp(_, m: MathOp, m2: MathOp) => checkDivisionExists(m) || checkDivisionExists(m2)
    case _ => false
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
    case ArrayType(tp) => SparkArrayType(getStructDataType(tp))
    case BagCType(tp) => StructType(tp.attrs.map(f => StructField(f._1, getStructDataType(f._2))).toSeq)
    case s@_ => sys.error("Unhandled struct type: " + s)
  }

  /**
   * Self Joins will not rename have duplicate columns renamed in JoinCondContext if they come from the same WrappedDataframe.
   * Those columns manually have suffixes appended here.
   */
  private def handleSelfJoinColumns(df: DataFrame): DataFrame = {
    val duplicatesCount = collection.mutable.Map[String, Int]().withDefaultValue(1)
    val renamedColumns = df.columns.map { colName =>
      val count = duplicatesCount(colName)
      duplicatesCount(colName) += 1
      if (count > 1) {
        findNextAvailDupColumn(df, colName, count)
      } else colName
    }

    df.toDF(renamedColumns: _*)
  }

  @tailrec
  private def findNextAvailDupColumn(i1: DataFrame, s: String, i: Int): String = {
    val columnInQuestionsName = s"${s}" + "_" + String.valueOf(i)
    if (!i1.columns.contains(columnInQuestionsName)) {
      columnInQuestionsName
    } else {
      findNextAvailDupColumn(i1, s, i + 1)
    }
  }

  private def mathResolver(op: OpArithmetic, x: Any, y: Any): Row = (x, y) match {
    case (x: Int, y: Int) => op match {
      case OpMultiply => Row(x * y)
      case OpPlus => Row(x + y)
      case OpMinus => Row(x - y)
      case OpMod => Row(x % y)
      case OpDivide => Row(x.toDouble / y)
    }
    case (x: Int, y: Long) => op match {
      case OpMultiply => Row(x * y)
      case OpPlus => Row(x + y)
      case OpMinus => Row(x - y)
      case OpMod => Row(x % y)
      case OpDivide => Row(x.toDouble / y)
    }
    case (x: Int, y: Double) => op match {
      case OpMultiply => Row(x * y)
      case OpPlus => Row(x + y)
      case OpMinus => Row(x - y)
      case OpMod => Row(x % y)
      case OpDivide => Row(x / y)
    }
    case (x: Double, y: Double) => op match {
      case OpMultiply => Row(x * y)
      case OpPlus => Row(x + y)
      case OpMinus => Row(x - y)
      case OpMod => Row(x % y)
      case OpDivide => Row(x / y)
    }

    case (x: Double, y: Int) => op match {
      case OpMultiply => Row(x * y)
      case OpPlus => Row(x + y)
      case OpMinus => Row(x - y)
      case OpMod => Row(x % y)
      case OpDivide => Row(x / y)
    }
    case (x: Double, y: Long) => op match {
      case OpMultiply => Row(x * y)
      case OpPlus => Row(x + y)
      case OpMinus => Row(x - y)
      case OpMod => Row(x % y)
      case OpDivide => Row(x / y)
    }
    case (x: Long, y: Long) => op match {
      case OpMultiply => Row(x * y)
      case OpPlus => Row(x + y)
      case OpMinus => Row(x - y)
      case OpMod => Row(x % y)
      case OpDivide => Row(x.toDouble / y)
    }
    case (x: Long, y: Int) => op match {
      case OpMultiply => Row(x * y)
      case OpPlus => Row(x + y)
      case OpMinus => Row(x - y)
      case OpMod => Row(x % y)
      case OpDivide => Row(x.toDouble / y)
    }
    case (x: Long, y: Double) => op match {
      case OpMultiply => Row(x * y)
      case OpPlus => Row(x + y)
      case OpMinus => Row(x - y)
      case OpMod => Row(x % y)
      case OpDivide => Row(x / y)
    }
  }

  private def mathComparator(comp: CExpr, lhs: Any, rhs: Any): Boolean = (lhs, rhs) match {
    case (x: Int, y: Int) => comp match {
      case _: Lt => x < y
      case _: Lte => x <= y
      case _: Gt => x > y
      case _: Gte => x >= y
    }
    case (x: Int, y: Long) => comp match {
      case _: Lt => x < y
      case _: Lte => x <= y
      case _: Gt => x > y
      case _: Gte => x >= y
    }
    case (x: Int, y: Double) => comp match {
      case _: Lt => x < y
      case _: Lte => x <= y
      case _: Gt => x > y
      case _: Gte => x >= y
    }
    case (x: Double, y: Double) => comp match {
      case _: Lt => x < y
      case _: Lte => x <= y
      case _: Gt => x > y
      case _: Gte => x >= y
    }
    case (x: Double, y: Int) => comp match {
      case _: Lt => x < y
      case _: Lte => x <= y
      case _: Gt => x > y
      case _: Gte => x >= y
    }
    case (x: Double, y: Long) => comp match {
      case _: Lt => x < y
      case _: Lte => x <= y
      case _: Gt => x > y
      case _: Gte => x >= y
    }
    case (x: Long, y: Long) => comp match {
      case _: Lt => x < y
      case _: Lte => x <= y
      case _: Gt => x > y
      case _: Gte => x >= y
    }
    case (x: Long, y: Int) => comp match {
      case _: Lt => x < y
      case _: Lte => x <= y
      case _: Gt => x > y
      case _: Gte => x >= y
    }
    case (x: Long, y: Double) => comp match {
      case _: Lt => x < y
      case _: Lte => x <= y
      case _: Gt => x > y
      case _: Gte => x >= y
    }
  }
}
