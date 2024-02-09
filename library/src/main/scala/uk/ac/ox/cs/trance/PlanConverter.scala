package uk.ac.ox.cs.trance

import uk.ac.ox.cs.trance.utilities.SparkUtil.getSparkSession
import framework.common.{ArrayType, BagCType, BagType, BoolType, DoubleType, IntType, LongType, OpArithmetic, OpDivide, OpMinus, OpMod, OpMultiply, OpPlus, OptionType, RecordCType, StringType, Type}
import framework.plans.{AddIndex, And, CDeDup, CExpr, CUdf, Comprehension, Constant, EmptySng, Equals, Gt, Gte, InputRef, Lt, Lte, MathOp, Nest, Not, Or, OuterJoin, Projection, Record, Unnest, Variable, If => CIf, Join => CJoin, Merge => CMerge, Project => CProject, Reduce => CReduce, Select => CSelect, Sng => CSng}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{BinaryOperator, EqualTo, Expression, GenericRow, GenericRowWithSchema, And => SparkAnd, GreaterThan => SparkGreaterThan, GreaterThanOrEqual => SparkGreaterThanOrEqual, LessThan => SparkLessThan, LessThanOrEqual => SparkLessThanOrEqual, Literal => SparkLiteral, Not => SparkNot, Or => SparkOr}
import org.apache.spark.sql.functions.{col, expr, monotonically_increasing_id}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType, ArrayType => SparkArrayType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Row, functions, types}
import uk.ac.ox.cs.trance.PlanConverter.getProjectionBinding

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import scala.annotation.tailrec
import scala.collection.immutable.{Map => IMap}
import uk.ac.ox.cs.trance.utilities.SkewDataset

import scala.reflect.ClassTag

object PlanConverter {

  private def unnestRecord(c: CExpr): Record = c match {
    case CIf(_, e1, _) => unnestRecord(e1)
    case CSng(c) => unnestRecord(c)
    case r: Record => r
  }

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

      c1.show(false)
      c1.printSchema()

      val l = c1.flatMap { z =>
        convert(p, ctx ++ getProjectionBinding(p, z)).asInstanceOf[Seq[Row]]
      }(RowEncoder.apply(outputSchema))
      l.asInstanceOf[T]
    case Nest(in, v, key, value, filter, nulls, ctag) =>
      val c1 = convert(in, ctx).asInstanceOf[DataFrame]


      val unnestedRecord = unnestRecord(value)


      val outputColumnNames = unnestedRecord.fields.map(f => f._1 -> f._2.asInstanceOf[CProject].field)

      val nestedStruct = unnestedRecord.fields.keys.toSeq

      val columns: Seq[org.apache.spark.sql.Column] = outputColumnNames.map(f => c1(f._2)).toSeq

      val t = functions.struct(columns: _*)
      val updatedC1 = c1.withColumn(ctag, t)
      updatedC1.show()
      updatedC1.asInstanceOf[T]

    case u @ Unnest(in, v, path, v2, filter, fields) =>
      val c1 = convert(in, ctx).asInstanceOf[DataFrame]

      val cols = u.nextAttrs.keys.map(f => f -> col(path + "." + f)).toMap
      val unnestingProcess = c1.withColumns(cols)

      unnestingProcess.asInstanceOf[T]
      //TODO case OuterUnnest
    case OuterJoin(left, v, right, v2, cond, fields) =>
      val i1 = convert(left, ctx).asInstanceOf[DataFrame]
      val i2 = convert(right, ctx).asInstanceOf[DataFrame]

      val c = toSparkCond(cond)

      val joined = i1.join(i2, c, "left_outer")

      joined.show(false)
      joined.asInstanceOf[T]
    case CJoin(left, v, right, v2, cond, fields) =>
      val i1 = convert(left, ctx).asInstanceOf[DataFrame]
      val i2 = convert(right, ctx).asInstanceOf[DataFrame]

      val c = toSparkCond(cond)

      val joined = performJoin(i1, i2, c)
      handleSelfJoinColumns(joined).asInstanceOf[T]
    case CMerge(e1, e2) =>
      // This has issues with the generic type as a pattern matching case, hence the use of if/else if
      if(convert(e1, ctx).isInstanceOf[DataFrame] && convert(e2, ctx).isInstanceOf[DataFrame]) {
        val d1 = convert(e1, ctx).asInstanceOf[DataFrame]
        val d2 = convert(e2, ctx).asInstanceOf[DataFrame]
        d1.union(d2).asInstanceOf[T]
      }
      else if(convert(e1, ctx).isInstanceOf[Row] && convert(e2, ctx).isInstanceOf[Row]) {
        val r1 = convert(e1, ctx).asInstanceOf[Row]
        val r2 = convert(e2, ctx).asInstanceOf[Row]
        Seq(r1, r2).asInstanceOf[T]
      }
      else if(convert(e1, ctx).isInstanceOf[Seq[Row]] && convert(e2, ctx).isInstanceOf[Seq[Row]]) {
        val r1 = convert(e1, ctx).asInstanceOf[Seq[Row]]
        val r2 = convert(e2, ctx).asInstanceOf[Seq[Row]]
        (r1++r2).asInstanceOf[T]
      }
      else if(convert(e1, ctx).isInstanceOf[Seq[Row]] && convert(e2, ctx).isInstanceOf[Row]) {
        val r1 = convert(e1, ctx).asInstanceOf[Seq[Row]]
        val r2 = convert(e2, ctx).asInstanceOf[Row]
        (r1++Seq(r2)).asInstanceOf[T]
      }
      else {
        sys.error("Invalid Merge")
      }
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
      val c1 =
        try {
          ctx(data)
        } catch {
          case _: NoSuchElementException => sys.error("No key: " + data + " found in ctx: " + ctx)
        }
      c1.asInstanceOf[T]
    case Variable(name, tp) =>
      val c =  try {
        ctx(name)
      } catch {
        case e: NoSuchElementException => sys.error("No element " + name + " found in context " + ctx)
      }
      c.asInstanceOf[T]
    case AddIndex(e, name) =>
      val df = convert(e, ctx).asInstanceOf[DataFrame].withColumn(name, monotonically_increasing_id)
      df.asInstanceOf[T]
    case CProject(e1, field) =>
      val row = convert(e1, ctx).asInstanceOf[Row]
      val projectionFieldIndex = row.schema.fieldIndex(field)
      val rowToBePutInsideRowObject = row.get(projectionFieldIndex)
      Row(rowToBePutInsideRowObject).asInstanceOf[T]
    case CSng(e1) =>
      val sng = convert(e1, ctx).asInstanceOf[Seq[Row]]
      sng.asInstanceOf[T]
    case Comprehension(e1, v, p, e) =>
      val c1 = convert(e1, ctx).asInstanceOf[DataFrame]
      // TODO - take schema from e for RowEncoder
      val k = c1.flatMap(z => Seq(convert(e, ctx + (v.name -> z)).asInstanceOf[Row]))(RowEncoder.apply(c1.schema))
      k.asInstanceOf[T]
    case Record(fields) =>
      val t = fields.map { x =>
        val out = convert(x._2, ctx).asInstanceOf[Row]
        out
      }.toArray
      val combinedRow = t.flatMap(row => row.toSeq)
      val r = Seq(Row.fromSeq(combinedRow))
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
    case CIf(cond, e1, e2) => if (convert(cond, ctx)) convert(e1, ctx) else convert(e2.get, ctx)
    case EmptySng => getSparkSession.emptyDataFrame.asInstanceOf[T]
    case Constant(e) => Row(e).asInstanceOf[T]
    case CUdf(name, in, tp) =>
      val c1 = convert(in, ctx).asInstanceOf[GenericRow]
      //Below is temporary, using String in UDF as concatenation
      name match {
        case "Transform" => Row.fromSeq(c1.toSeq.map(f => f.asInstanceOf[String])).asInstanceOf[T]
        case n@_ =>  Row.fromSeq(c1.toSeq.map(f => f.asInstanceOf[String] + n)).asInstanceOf[T]
      }
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
    case InputRef(name, _) => IMap(name -> row)
    case CProject(e1, field) => getProjectionBinding(e1, row)
    case Variable(name, _) => IMap(name ->  row)
    case MathOp(_, e1, e2) => getProjectionBinding(e1, row) ++ getProjectionBinding(e2, row)
    case CUdf(_ ,in, _) => getProjectionBinding(in, row)
    case Record(fields) => fields.flatMap(f => getProjectionBinding(f._2, row))
    case CMerge(e1, e2) => getProjectionBinding(e1, row) ++ getProjectionBinding(e2, row)
    case CSng(e1) => getProjectionBinding(e1, row)
    case CIf(cond, e1, e2) =>
      val c = getProjectionBinding(cond, row)
      val left = getProjectionBinding(e1, row)
      e2 match {
        case Some(right) => c ++ left ++ getProjectionBinding(right, row)
        case None => c ++ left
      }
    case Equals(e1, e2) => getProjectionBinding(e1, row) ++ getProjectionBinding(e2, row)
    case Gt(e1, e2) => getProjectionBinding(e1, row) ++ getProjectionBinding(e2, row)
    case Gte(e1, e2) => getProjectionBinding(e1, row) ++ getProjectionBinding(e2, row)
    case Lt(e1, e2) => getProjectionBinding(e1, row) ++ getProjectionBinding(e2, row)
    case Lte(e1, e2) => getProjectionBinding(e1, row) ++ getProjectionBinding(e2, row)
    case Or(e1, e2) => getProjectionBinding(e1, row) ++ getProjectionBinding(e2, row)
    case And(e1, e2) => getProjectionBinding(e1, row) ++ getProjectionBinding(e2, row)
    case Constant(_) => IMap.empty
    case CJoin(left, _, right, _, _, _) => getProjectionBinding(left, row) ++ getProjectionBinding(right, row)
    case OuterJoin(left, _, right, _, _, _) => getProjectionBinding(left, row) ++ getProjectionBinding(right, row)
    case Nest(in, _, _, _, _, _, _) => getProjectionBinding(in, row)
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
        case InputRef(id, record: RecordCType) => StructField(f._1, getStructDataType(f._2.tp, Some(f._1)))

        case _ => StructField(f._1, getStructDataType(f._2.tp))
      }).toSeq)
    case CIf(_, e1, _) => createStructFields(e1)
    case CSng(e1) => createStructFields(e1)
    case CMerge(e1, e2) => createStructFields(e1)
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
  private def getStructDataType(c: Type, s: Option[String] = None): DataType = c match {
    case IntType => DataTypes.IntegerType
    case BoolType => DataTypes.BooleanType
    case StringType => DataTypes.StringType
    case DoubleType => DataTypes.DoubleType
    case LongType => DataTypes.LongType
    case ArrayType(tp) => SparkArrayType(getStructDataType(tp))
    case BagCType(tp) => StructType(tp.attrs.map(f => StructField(f._1, getStructDataType(f._2, Some(f._1)))).toSeq)
    case RecordCType(fields) =>
      val k =
        try {
          fields(s.get)
        } catch {
          case _: NoSuchElementException => sys.error("No element " + s + " in " + fields)
        }
      getStructDataType(k)
    case RecordCType(fields) => StructType(fields.map(f => StructField(f._1, getStructDataType(f._2))).toSeq)
    case OptionType(tp) => getStructDataType(tp)
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
