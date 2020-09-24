package sparkutils.loader

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import java.io.File
import sparkutils.skew.SkewDataset._
import sparkutils.Config
import org.apache.spark.broadcast.Broadcast

/** Load Gistic file from GDC / ICGC gene level focal scores. Such as:
  * https://portal.gdc.cancer.gov/files/fd97b1fd-8f97-4b34-ba73-b7778159c6a1
  * 
  **/

case class Sample(gistic_sample: String, focal_score: Long)
case class Gistic(gistic_gene: String, gistic_gene_iso: String, cytoband: String, gistic_samples: Seq[Sample])
case class GisticDict1(gistic_gene: String, gistic_gene_iso: String, cytoband: String, gistic_samples: String)
case class GisticSampleDict2(_1: String, gistic_sample: String, focal_score: Long)

class GisticLoader(spark: SparkSession) {

  private final val COL_GENE = 0
  private final val COL_CYTOBAND = 4

  import spark.implicits._
  val delimiter: String = "\t"
  

  def merge(path: String, dir: Boolean = true): Dataset[Gistic] = {
    val files = if (dir) (new File(path)).listFiles().toList.map(f => s"$dir/${f.getName}")
		else List(path)
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
  	val tmp = dfs.repartition(Config.minPartitions).rdd.zipWithIndex
  	val dict1 = tmp.map{ case (row, id) => 
  		GisticDict1(row.gistic_gene, row.gistic_gene_iso, row.cytoband, s"$id")}.toDF.as[GisticDict1]
  	val dict2 = tmp.flatMap{ case (row, id) => row.gistic_samples.map(srow => 
  		GisticSampleDict2(s"$id", srow.gistic_sample, srow.focal_score)) }.toDF.as[GisticSampleDict2]
  	(dict1, dict2)
  }

  def shredSkew(dfs: Dataset[Gistic]): ((Dataset[GisticDict1], Dataset[GisticDict1]), 
    (Dataset[GisticSampleDict2], Dataset[GisticSampleDict2], Option[String], Broadcast[Set[String]])) = {
    val (dict1, dict2) = shred(dfs)
    val skew_dict1 = (dict1, dict1.empty)
	val nullKeys = spark.sparkContext.broadcast(Set.empty[String])
    val skew_dict2 = (dict2, dict2.empty, Some("_1"), nullKeys)//.repartition[String](col("_1"))
    (skew_dict1, skew_dict2)
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
