package framework.library

import org.apache.spark.sql.{Dataset, Row}
import scala.language.implicitConversions

class EnterNRC(dataset: Dataset[Row]) {
  def enterNRC(): ScalaNRC = {
    val ds = new ScalaNRC(
      input = dataset,
    )
    ds
  }
}
object CustomFunctions {
  implicit def addEnterNRC(dataset: Dataset[Row]): EnterNRC = new EnterNRC(dataset)
}
