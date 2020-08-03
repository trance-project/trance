package framework.examples.CancerDataLoader

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import java.io.File


class TCGLoader(spark: SparkSession) {

  import spark.implicits._


  val delimiter: String = "\t"

  def load(path: String, dir: Boolean = false) = {
    val files = if (dir){
      val files = new File(path)
      files.listFiles().toSeq.map(f => f.getName)
    }else Seq(path)

    val df = spark.read.format("csv")
                .option ("header", "true")
                .option("delimiter", delimiter)
                .option("inferSchema", "true")
                .load(files:_*)

    df
  }

}