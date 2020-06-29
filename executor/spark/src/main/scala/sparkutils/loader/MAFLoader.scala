package sparkutils.loader

import org.apache.parquet.format.IntType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import sparkutils.Config
import scala.collection.mutable.ArrayBuffer
import java.io.File

case class Consequence(consequenceType:String, functionalImpact:String, SYMBOL: String, Consequence: String, HGVSp_Short: String, Transcript_ID: String, RefSeq:String , HGVSc:String , IMPACT:String ,CANONICAL:String , SIFT: String, PolyPhen:String, Strand:String)
case class GeneMaf(chromosome: String, biotype: String, geneId: String, Hugo_Symbol:String, consequences: Seq[Consequence])
case class Occurrences(donorId: String, end: Int, projectId: String, start: Int, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, genes: Seq[GeneMaf])
case class OccurrencesTop(donorId: String, vend: Int, projectId: String, start: Int, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, Hugo_Symbol: String, biotype: String, functionalImpact: String, consequenceType: String, all_effects: String)

class MAFLoader(spark: SparkSession) {

  import spark.implicits._

  val delimiter: String = "\t"

  /*Schema for now:
  * root
    |-- donorId: string (nullable = true)
    |-- end: integer (nullable = true)
    |-- projectId: string (nullable = true)
    |-- start: integer (nullable = true)
    |-- Reference_Allele: string (nullable = true)
    |-- chromosome: string (nullable = true)
    |-- geneId: string (nullable = true)
    |-- Hugo_symbol: string (nullable = true)
    |-- biotype: string (nullable = true)
    |-- SYMBOL: string (nullable = true)
    |-- Consequence: string (nullable = true)
    |-- HGVSp_Short: string (nullable = true)
    |-- Transcript_ID: string (nullable = true)
    |-- RefSeq: string (nullable = true)
    |-- HGVSc: string (nullable = true)
    |-- IMPACT: string (nullable = true)
    |-- CANONICAL: string (nullable = true)
    |-- SIFT: string (nullable = true)
    |-- PolyPhen: string (nullable = true)
    |-- Strand: string (nullable = true)
    |-- Variant_Class: string (nullable = true)
    |-- Consequence: string (nullable = true)
  * */
  def loadFlat(path: String, dir: Boolean = false): Dataset[OccurrencesTop] = {
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
      .selectExpr(
        "Gene as geneId", "case_id as donorId", "End_Position as vend","Center as projectId","Start_Position as start","Reference_Allele", "Tumor_Seq_Allele1", "Tumor_Seq_Allele2",
          "Chromosome as chromosome", "Hugo_symbol", "BIOTYPE as biotype",

        "Variant_Class as functionalImpact", "Consequence as consequenceType", "all_effects")
	  .as[OccurrencesTop]
	  .repartition(Config.maxPartitions)
  }


  /**def buildNested(data: Dataset[OccurrencesTop]): Dataset[Occurrences] = {

    val d =  data.map(

      line =>{

////        val gene_ID = line.getAs("geneId")
//        var gene_ID = ""
//        if(line.getAs("geneId") == null){
//          gene_ID = line.getAs("geneId").toString
//        }



        val w = (line.getAs("donorId").toString, line.getAs("end").toString.toInt,
                line.getAs("projectId").toString, line.getAs("start").toString.toInt,
                line.getAs("Reference_Allele").toString, line.getAs("Tumor_Seq_Allele1").toString,
                line.getAs("Tumor_Seq_Allele2").toString, line.getAs("chromosome").toString,
                line.getString(0),
          line.getAs("Hugo_symbol").toString,
                line.getAs[String]("biotype")
        )



        val impact = line.getAs("functionalImpact").toString

        val consequenceType = line.getAs("consequenceType").toString

        val buffer = ArrayBuffer[Consequence]() // consequences

        line.getAs[String]("all_effects").split(";").foreach(
          l => {
            val fields = l.split(",")
            val c = Consequence(consequenceType, impact, fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7), fields(8), fields(9), fields(10))
            buffer.append(c)
          }
        )

        (w, buffer.toSeq)
      }).rdd.reduceByKey((x,y) => x ++: y)
      .map(line => {
        val o = line._1
        val conseq = line._2

        val other: (String, Int, String, Int, String, String, String) = (o._1,o._2,o._3,o._4,o._5,o._6,o._7)

        val seq = ArrayBuffer[Gene]()
        seq.append(Gene(o._8,o._9,o._10,o._11,conseq))
        (other, seq)
      }).reduceByKey((x, y) => x ++: y)
      .map(
        line => {
          val o: (String, Int, String, Int, String, String, String) = line._1
          val genes: Seq[Gene] = line._2.toSeq

          Occurrences(o._1, o._2, o._3, o._4, o._5, o._6, o._7, genes)
        }
      ).toDF().as[Occurrences]
    d
  }

  def load(path: String): Dataset[Occurrences] ={
    val data_flat = loadFlat(path)
    val data = buildNested(data_flat)
    data
  }**/
}


