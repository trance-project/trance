package sparkutils.loader

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, DoubleType, StructField, StructType}

case class ThousandGenomes(m_sample: String, family_id: String, population: String, gender: String)

class TGenomesLoader(spark: SparkSession) extends Table[ThousandGenomes] {

  import spark.implicits._

  val schema = StructType(Array(
                  StructField("m_sample", StringType), 
                  StructField("family_id", StringType),
                  StructField("population", StringType),
                  StructField("gender", StringType)))

  val header: Boolean = true
  val delimiter: String = ","

  def load(path: String): Dataset[ThousandGenomes] = 
    spark.read.schema(schema)
      .option("header", header)
      .option("delimiter", delimiter)
      .csv(path)
      .as[ThousandGenomes]

}