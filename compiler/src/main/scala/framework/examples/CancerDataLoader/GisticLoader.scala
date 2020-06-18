package framework.examples.CancerDataLoader

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

case class sample(name: String, focalScore: Int)
case class gistic(gene: String, cytoband: String, samples: Seq[sample])
//  {(gene: String, cytoband: String, samples: {(name: String, focal_score: Int)})}

class GisticLoader(spark: SparkSession) {

  private final val COL_GENE = 0
  private final val COL_CYTOBAND = 4

  import spark.implicits._
  val delimiter: String = "\t"

  def load(path: String) : Dataset[gistic] ={
    val header = getHeader(read(path))
    // drop the header before going through the file line by line
    val file = spark.sparkContext.textFile(path)
      .mapPartitionsWithIndex { (id_x, iter) => if (id_x == 0) iter.drop(1) else iter }

    val data = file.map(
      line =>{
        val sline = line.split("\t")
        val gene = sline(COL_GENE)
        val cytoband = sline(COL_CYTOBAND)

        val buffer = ArrayBuffer[sample]()

        for(index <- 5 until sline.length){
          val name = header.getOrElse(index, "")
          buffer.append(sample(name, sline(index).toInt))
          }

          gistic(gene, cytoband, buffer.toSeq)
        }
      ).toDF().as[gistic]
    data
  }

  private def read(path: String): DataFrame = {

    val data: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("comment", "#")
      .option("delimiter", delimiter)
      .option("inferSchema", "true")
      .load(path)
    data
  }

  private def getHeader(df: DataFrame): HashMap[Int, String] = {

    val dict = HashMap.empty[Int, String]
    val header: Seq[StructField] = df.schema.toList
    val headerSize = header.length

    for(a <- 0 until headerSize){
      val field = header.apply(a)
      dict.put(a, field.name)

      println(field.name)
    }
    dict
  }
}