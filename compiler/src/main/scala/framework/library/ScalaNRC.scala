package framework.library

import framework.common.{BagType, BoolType, DoubleType, IntType, LongType, StringType, TupleAttributeType, TupleType }
import framework.library.utilities.SparkUtil.getSparkSession
import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession, types}
import framework.nrc._
import framework.plans.{CExpr, NRCTranslator, Variable}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import java.util.concurrent.atomic.AtomicInteger

class ScalaNRC(val input: Dataset[Row]) extends NRC with NRCTranslator {

  private val inputIdentifier: String = "input" + new AtomicInteger(1).getAndIncrement()
  val spark: SparkSession = getSparkSession
  val expr: Expr = createNRCExpression()
  val ctx: Map[String, Dataset[Row]] = Map(inputIdentifier -> input)

  private def createNRCExpression(): Expr = {

      val nrcTypeMap: Map[String, TupleAttributeType] = input.schema.fields.map{
        case StructField(name, ArrayType(dataType, _), _, _) => "array" -> BagType(TupleType(name -> typeToNRCType(dataType)))
        case StructField(name, dataType, _, _) => name -> typeToNRCType(dataType)
      }.toMap

      val expr = BagVarRef(inputIdentifier, BagType(TupleType(nrcTypeMap)))
      println("NRC Expr: " + expr)
      expr
  }

  def leaveNRC(): Dataset[Row] = {
    val plan = toPlan(expr)

    println("plan: " + plan)
    planToDataframe(plan)
  }

  private def toPlan(e: Expr): CExpr = {
    translate(e)
  }

  private def planToDataframe(cExpr: CExpr): Dataset[Row] = {
    cExpr match {
      case Variable(name, _) => ctx(name)
    }
  }

  private def typeToNRCType(s: DataType): TupleAttributeType = {
    println("s :" + s)
    s match {
      case structType: StructType => BagType(TupleType(structType.fields.map(f => f.name -> typeToNRCType(f.dataType)).toMap))
      case types.StringType => StringType
      case types.IntegerType => IntType
      case types.LongType => LongType
      case types.DoubleType => DoubleType
      case types.BooleanType => BoolType
      case _ => null
    }
  }

  def flatMap[U: Encoder](func: Row => TraversableOnce[U]): ScalaNRC = {
    val x: Dataset[Row] = input.mapPartitions(_.flatMap(func)).toDF()

    new ScalaNRC(input = x)
  }
}
