package sparkutils.loader

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, DoubleType, StructField, StructType}

/** Base loader trait used for generated loaders. 
  *
  */


trait Table[T] {

  val schema: StructType

  val header: Boolean
  val delimiter: String
  def load(path: String): Dataset[T] 

}