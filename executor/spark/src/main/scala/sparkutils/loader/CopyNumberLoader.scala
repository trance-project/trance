package sparkutils.loader

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.io.File

class CopyNumberLoader(spark: SparkSession) {

  import spark.implicits._
  val delimiter: String = "\t"  

  val aliquotUdf = udf { s: String => s.split(".")(1) }
  def load(dir: String) = {
    val files = (new File(dir)).listFiles().toSeq.map(f => s"$dir/${f.getName}")
    spark.read.format("csv")
      .option("header", "true")
      .option("comment", "#")
      .option("delimiter", delimiter)
      .option("inferSchema", "true")
      .load(files:_*)
	  .withColumn("filename", input_file_name)
	  //.withColumn("gl_aliquot_uuid", aliquotUdf($"filename"))
  }

}
