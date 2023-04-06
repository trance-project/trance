package framework.library

import framework.common.{BagType, DoubleType, IntType, StringType, TupleAttributeType, TupleType, Type}
import org.apache.spark.sql.{Dataset, Row, types}
import framework.nrc._
import framework.plans.CExpr
import framework.plans.NRCTranslator
import org.apache.spark.sql.types.{DataType, StructType}

class ScalaNRC(input: Dataset[Row]) extends NRC with NRCTranslator {

  val expr: Expr = convertToNRCExpression()
  def convertToNRCExpression(): Expr = {
      val unnestedTypesAsNRCType = input.schema.fields.map(f => f.name -> typeToNRCType(getNestedTypes(f.dataType))).toMap
      val expr = BagVarRef("input2", BagType(TupleType(unnestedTypesAsNRCType)))

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
    //use code generator for example on how to go from cExpr to Dataframe
    null
  }

  private def typeToNRCType(s: Seq[DataType]): TupleAttributeType = {
    println("s :" + s)
    s match {
      case Seq(types.StringType) => StringType
    }
  }

  private def getNestedTypes(dt: DataType): Seq[DataType] = {
   dt match {
     case types.StringType => Seq(types.StringType)
     case structType: types.StructType => structType.fields.flatMap(f => getNestedTypes(f.dataType))
   }
  }
  def flatMap(): Unit = {

  }
}
