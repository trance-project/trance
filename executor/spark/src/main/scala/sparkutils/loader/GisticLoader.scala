package sparkutils.loader

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import java.io.File

case class Sample(gistic_sample: String, focal_score: Long)
case class Gistic(gistic_gene: String, gistic_gene_iso: String, cytoband: String, gistic_samples: Seq[Sample])
case class GisticDict1(gistic_gene: String, gistic_gene_iso: String, cytoband: String, gistic_samples: String)
case class GisticSampleDict2(_1: String, gistic_sample: String, focal_score: Long)

//  {(gene: String, cytoband: String, samples: {(name: String, focal_score: Long)})}

class GisticLoader(spark: SparkSession) {

  private final val COL_GENE = 0
  private final val COL_CYTOBAND = 4

  import spark.implicits._
  val delimiter: String = "\t"
  
  /**def load(path: String) : Dataset[Gistic] ={
    val header = getHeader(read(path))
    // drop the header before going through the file line by line
    val file = spark.sparkContext.textFile(path)
      .mapPartitionsWithIndex { (id_x, iter) => if (id_x == 0) iter.drop(1) else iter }

    val data = file.map(
      line =>{
        val sline = line.split("\t")
        val gene = sline(COL_GENE)
        val cytoband = sline(COL_CYTOBAND)

        val buffer = ArrayBuffer[Sample]()

        for(index <- 5 until sline.length){
          val name = header.getOrElse(index, "")
          buffer.append(Sample(name, sline(index).toInt))
          }

          Gistic(gene, cytoband, buffer.toSeq)
        }
      ).toDF().as[Gistic]
    data
  }**/

  def merge(dir: String): Dataset[Gistic] = {
    val files = (new File(dir)).listFiles().toList.map(f => s"$dir/${f.getName}")
    val ldf = read(files.head)
    val df = iterMerge(ldf, files.tail)
    val columns = spark.sparkContext.broadcast(df.columns.toSet -- Set("Gene Symbol", "Gene ID", "Cytoband"))
    df.mapPartitions{ it => it.map{ r => 
		val gid_iso = r.getString(r.fieldIndex("Gene Symbol"))
		val gid = gid_iso.split(".").toSeq match {
			case Nil => "null"
			case tail :: Nil => tail
			case head :: tail => head
		}
        Gistic(gid, gid_iso, r.getString(r.fieldIndex("Cytoband")), 
			columns.value.map(c => Sample(c, r.getInt(r.fieldIndex(c)))).toSeq
        )
    }}.as[Gistic]
  }

  def iterMerge(ldf: DataFrame, rfs: List[String]): DataFrame = rfs match {
    case Nil => ldf
    case rf :: tail =>
      val rdf = read(rf)
        .withColumnRenamed("Gene Symbol", "GS")
        .withColumnRenamed("Cytoband", "CB")
      val ndf = ldf.join(rdf, $"Gene Symbol" === $"GS" && $"Cytoband" === $"CB")
        .drop("GS", "CB")
      iterMerge(ndf, tail)
  }

  def shred(dfs: Dataset[Gistic]): (Dataset[GisticDict1], Dataset[GisticSampleDict2]) = {
	val tmp = dfs.rdd.zipWithIndex
	val dict1 = tmp.map{ case (row, id) => 
		GisticDict1(row.gistic_gene, row.gistic_gene_iso, row.cytoband, s"$id")}.toDF.as[GisticDict1]
	val dict2 = tmp.flatMap{ case (row, id) => row.gistic_samples.map(srow => 
		GisticSampleDict2(s"$id", srow.gistic_sample, srow.focal_score)) }.toDF.as[GisticSampleDict2]
	(dict1, dict2)
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
