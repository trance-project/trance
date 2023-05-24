package framework.library

import org.apache.spark.sql.{Dataset, Row}
import scala.language.implicitConversions

class CustomFunctions(dataset: Dataset[Row]) {
  def wrap(): WrappedDataset = {
    val ds = new WrappedDataset(
      inputDf = dataset,
    )
    ds
  }
}
object CustomFunctions {
  implicit def addWrap(dataset: Dataset[Row]): CustomFunctions = new CustomFunctions(dataset)
}
