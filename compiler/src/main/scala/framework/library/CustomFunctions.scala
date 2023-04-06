package framework.library

import framework.nrc.NRC
import framework.plans.{CExpr, NRCTranslator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}

class EnterNRC(dataset: Dataset[Row]) {
  def enterNRC(): ScalaNRC = {
    val ds = new ScalaNRC(
      input = dataset,
    )
    ds
  }
}
object CustomFunctions {
  implicit def addEnterNRC(dataset: Dataset[Row]) = new EnterNRC(dataset)
}
