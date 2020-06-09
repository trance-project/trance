package sparkutils.loader

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, DoubleType, StructField, StructType}

case class ThousandGenomes(m_sample: String, family_id: String, population: String, gender: String)

class ClinicalLoader(spark: SparkSession, path: String) extends Serializable {

  import spark.implicits._

  val schema = StructType(Array(
                  StructField("m_sample", StringType), 
                  StructField("family_id", StringType),
                  StructField("population", StringType),
                  StructField("gender", StringType)))


  val table = spark.read.schema(schema)
    .option("header", true)
    .option("delimiter", ",")
    .csv(path)
    .as[ThousandGenomes]

}


