package sparkutils.loader

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import sparkutils.Config
import java.io.File

import org.apache.spark.SparkConf


class TCGLoader(spark: SparkSession) {

  import spark.implicits._

  val delimiter: String = "\t"

  def load(path: String, dir: Boolean = false) = {
    val files = if (dir){
      val files = new File(path)
      files.listFiles().toSeq.map(f => f.getName)
    }else Seq(path)

    spark.read.format("csv")
      .option("header", "true")
      .option("comment", "#")
      .option("delimiter", delimiter)
      .option("inferSchema", "true")
      .load(files:_*)

      .repartition(1000)
  }

}