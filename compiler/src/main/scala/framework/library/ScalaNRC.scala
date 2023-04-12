package framework.library

import framework.common.{BagType, DoubleType, IntType, StringType, TupleAttributeType, TupleType, Type}
import framework.library.utilities.SparkUtil.getSparkSession
import org.apache.spark.sql.{Dataset, Row, types}
import framework.nrc._
import framework.plans.CExpr
import framework.plans.NRCTranslator
import org.apache.spark.sql.types.{DataType, StructField, StructType}

class ScalaNRC(val input: Dataset[Row]) extends NRC with NRCTranslator {

  val expr: Expr = convertToNRCExpression()
  def convertToNRCExpression(): Expr = {
      val unnestedTypesAsNRCType = input.schema.fields.map(f => f.name -> typeToNRCType(f.dataType)).toMap
      val expr = BagVarRef("input2", BagType(TupleType(unnestedTypesAsNRCType)))

      println("NRC Expr: " + expr)
      expr
  }

  def leaveNRC(): Dataset[Row] = {
    val plan = toPlan(expr)
    println("plan: " + plan)
    val df = planToDataframe(plan)
    println("After transformation: ")
    df.printSchema()
    df.show(false)
    df
  }

  private def toPlan(e: Expr): CExpr = {
    translate(e)
  }

  private def planToDataframe(cExpr: CExpr): Dataset[Row] = {
    val spark = getSparkSession()
    var attrs = cExpr.tp.attrs
    val schema = StructType(attrs.map { case (fieldName, fieldType) => StructField(fieldName, NRCToSparkType(fieldType), nullable = true) }.toSeq)
    val ds = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    ds
  }

//    val seq = attrs.map { case (k, v) => (k, v.toString)}.toSeq
//    val rdd = spark.sparkContext.parallelize(seq)
//    val schema = input.schema
//
//
//    val rowRDD = rdd.map(_ => Row.fromSeq(seq))
//
//    ds


  private def typeToNRCType(s: DataType): TupleAttributeType = {
    println("s :" + s)
    s match {
      case types.StringType => StringType
    }
  }

  private def NRCToSparkType(t: Type): DataType = {
    t match {
      case StringType => types.StringType
    }
  }

//  private def getNestedTypes(dt: DataType): Seq[DataType] = {
//    dt match {
//      case types.StringType => Seq(types.StringType)
//      case structType: types.StructType => structType.fields.flatMap(f => getNestedTypes(f.dataType))
//    }
//  }
  
  def flatMap(): Unit = {

  }
}
