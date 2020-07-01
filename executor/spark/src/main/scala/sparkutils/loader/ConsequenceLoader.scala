package sparkutils.loader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import scala.io.Source

case class ConseqCalc(id: Int, so_term: String, so_description: String, so_accession: String, display_term: String, impact: String)

class ConsequenceLoader(spark: SparkSession) {
  
  import spark.implicits._
  
  val delimiter = "\t"
  val header = true

  val schema = StructType(Array(StructField("so_term", StringType),
		StructField("so_description", StringType),
                StructField("so_accession", StringType),
		StructField("display_term", StringType),
		StructField("impact", StringType)))

  def load(path: String): Dataset[ConseqCalc] = {
    spark.read.schema(schema)
      .option("header", header)
      .option("delimiter", delimiter)
      .csv(path).as[ConseqCalc]
    }

  def read(filename: String): Map[String, Double] = {
    val lines = Source.fromFile(filename).getLines.toList
    val thresh = lines.size
    lines.tail.zipWithIndex.map{ case (l, id) =>
      val eles = l.split("\t") 
      (eles(0), (thresh-(id.toDouble+1.0))/thresh)
    }.toMap
  }

}
