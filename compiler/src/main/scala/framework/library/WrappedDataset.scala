package framework.library

import framework.common.{BagType, BoolType, DoubleType, IntType, LongType, StringType, TupleAttributeType, TupleType, VarDef}
import framework.library.WrappedDataset.{addMapping, atomicInteger, ctx}
import framework.library.utilities.SparkUtil.getSparkSession
import org.apache.spark.sql.{Dataset, Row, SparkSession, types}
import framework.nrc._
import framework.plans.{CExpr, Comprehension, NRCTranslator, Variable}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

class WrappedDataset(val inputDf: Dataset[Row]) extends NRC with NRCTranslator {

  val spark: SparkSession = getSparkSession
  var expr: Expr = createNRCExpression()
  def this(df: Dataset[Row], b: Any){
    this(df)
    expr = b.asInstanceOf[Expr]
  }

  private def createNRCExpression(): Expr = {
      val inputIdentifier = "input_" + atomicInteger.getAndIncrement()
      WrappedDataset.addMapping(inputIdentifier, inputDf)

      val nrcTypeMap: Map[String, TupleAttributeType] = inputDf.schema.fields.map{
        case StructField(name, ArrayType(dataType, _), _, _) => "array" -> BagType(TupleType(name -> typeToNRCType(dataType)))
        case StructField(name, dataType, _, _) => name -> typeToNRCType(dataType)
      }.toMap

      BagVarRef(inputIdentifier, BagType(TupleType(nrcTypeMap)))
  }

  def leaveNRC(): Dataset[Row] = {
    val plan = toPlan(expr)

    println("plan: " + plan)
    planToDataframe(plan)
  }

  def toPlan(e: Expr): CExpr = {
    translate(e)
  }

  private def planToDataframe(cExpr: CExpr): Dataset[Row] = {
    cExpr match {
      case Comprehension(e1, v, p, e) =>
        println("COMPREHENSION:")
        println("________________")
        println("e1: " + e1)
        println("v: " + v)
        println("p: " + p)
        println("e: " + e)
        println("ctx: " + WrappedDataset.ctx)
        println("________________")

        val d1 = planToDataframe(e1)
        val d2 = planToDataframe(e)


        val d3 = d1.union(d2)
        d3
      case Variable(name, _) => WrappedDataset.getMapping(name)
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
  def flatMap(f: TupleExpr => BaseExpr)(e1: WrappedDataset, e2: WrappedDataset): WrappedDataset = {
    val x = VarDef("x", this.expr.asBag.tp.tp)
    //1. using scala reflection library

    var bagExpr = ForeachUnion(x, this.expr.asBag, this.expr.asBag).asBag

    new WrappedDataset(inputDf, bagExpr)
  }

  def map(): BagExpr = {
    null
  }
}

object WrappedDataset {

  private val atomicInteger: AtomicInteger = new AtomicInteger(1)
  val ctx: mutable.Map[String, Dataset[Row]] = collection.mutable.Map[String, Dataset[Row]]()

  def apply(d: Dataset[Row]): Unit = {
    val inputDf = d
  }
  private def addMapping(s: (String, Dataset[Row])): Unit = {
    ctx += s
    println(ctx)
  }

  private def getMapping(s: String): Dataset[Row] = {
    ctx.getOrElse(s, null)
  }

}
