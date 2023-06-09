package framework.library

import framework.common.{BagType, BoolType, DoubleType, IntType, LongType, StringType, TupleAttributeType, TupleType, VarDef}
import framework.library.WrappedDataset.{addMapping, atomicInteger, ctx}
import framework.library.utilities.SparkUtil.getSparkSession
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, types}
import framework.plans.{CExpr, Comprehension, NRCTranslator, Sng, Variable}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

class WrappedDataset(val inputDf: Dataset[Row]) {

  import WrappedDataset.nrc._
  val spark: SparkSession = getSparkSession

  // Workaround for limitation of auxiliary constructors needing to call original constructor
  var expr: Expr = if (inputDf != null) createNRCExpression() else expr

  def this(b: Any) {
    this(null)
    expr = b.asInstanceOf[Expr]
  }

  private def createNRCExpression(): Expr = {
    val inputIdentifier = "input_" + atomicInteger.getAndIncrement()
    WrappedDataset.addMapping(inputIdentifier, inputDf)

    val nrcTypeMap: Map[String, TupleAttributeType] = inputDf.schema.fields.map {
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
      case Sng(e1) => planToDataframe(e1)
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

        if (d2 != null) createFlatMapDataframe(d1, d2) else d1
      case Variable(name, _) => WrappedDataset.getMapping(name)
    }
  }

  private def createFlatMapDataframe(d1: DataFrame, d2: DataFrame): DataFrame = {
    val d3 = d1.collect().flatMap(_ => d2.collect())
    val result = spark.sparkContext.parallelize(d3)
    spark.createDataFrame(result, d2.schema)
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

  //Syntactic sugar method to allow user's
  // syntax to match spark flatMap while performing operations on a WrappedDataset
  def flatMap(f: TupleExpr => WrappedDataset)(implicit d : DummyImplicit): WrappedDataset = {
    val x = VarDef("x", this.expr.asBag.tp.tp)
    new WrappedDataset(ForeachUnion(x, this.expr.asBag, f(null).expr.asInstanceOf[BagExpr]).asBag)

  }

  def flatMap(f: TupleExpr => BagExpr): WrappedDataset = {
    val tupleIdentifier = "x_" + atomicInteger.getAndIncrement()
    val tvr = TupleVarRef(tupleIdentifier, this.expr.asBag.tp.tp)

    WrappedDataset.addMapping(tvr.name, this.inputDf)

    val bagExpr = ForeachUnion(tvr, this.expr.asBag, f(tvr)).asBag

    new WrappedDataset(bagExpr)
  }


  // TODO
  def map(): BagExpr = {
    null
  }

}

object WrappedDataset {

  val nrc: NRCTranslator = new NRCTranslator {}

  private val atomicInteger: AtomicInteger = new AtomicInteger(1)
  val ctx: mutable.Map[String, Dataset[Row]] = collection.mutable.Map[String, Dataset[Row]]()

  private def addMapping(s: (String, Dataset[Row])): Unit = {
    ctx += s
    println(ctx)
  }

  private def getMapping(s: String): Dataset[Row] = {
    ctx.getOrElse(s, null)
  }

  case object Singleton {
    def apply(x: nrc.TupleExpr): nrc.BagExpr = {
      nrc.Singleton(x)
    }
  }

}


