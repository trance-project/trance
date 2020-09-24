package sparkutils.loader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import scala.io.Source

/** This reads sequence ontology information. Based on a table 
  * found in online materials that lists consequences from most 
  * to least impactful:
  * https://uswest.ensembl.org/info/genome/variation/prediction/predicted_data.html
  *
  **/

case class ConseqCalc0(so_term: String, so_description: String, so_accession: String, display_term: String, so_impact: String)
case class ConseqCalc(so_term: String, so_description: String, so_accession: String, display_term: String, so_impact: String, so_weight: Double)

class ConsequenceLoader(spark: SparkSession) {
  
  import spark.implicits._
  
  val delimiter = "\t"
  val header = true

  val schema = StructType(Array(StructField("so_term", StringType),
		StructField("so_description", StringType),
    StructField("so_accession", StringType),
		StructField("display_term", StringType),
		StructField("impact", StringType)))

  def load(path: String): Dataset[ConseqCalc0] = {
    spark.read.schema(schema)
      .option("header", header)
      .option("delimiter", delimiter)
      .csv(path).as[ConseqCalc0]
  }

  /** Loader that will assign a quantitative value to the impact 
    * based on the ordering of IMPACT rating of SO terms. 
    * See url referenced above.
    *
    **/
  def loadSequential(filename: String): Dataset[ConseqCalc] = {
    val lines = Source.fromFile(filename).getLines.toList
    val total = lines.size
    spark.sparkContext.parallelize(lines.tail.zipWithIndex.map{ case (l, id) =>
      val eles = l.split("\t") 
      ConseqCalc(eles(0), eles(1), eles(2), eles(3), eles(4), (total-(id.toDouble+1.0))/total)
    }.toSeq).toDF().as[ConseqCalc]
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
