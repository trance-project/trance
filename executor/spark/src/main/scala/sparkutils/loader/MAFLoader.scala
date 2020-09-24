package sparkutils.loader

import org.apache.parquet.format.IntType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import sparkutils.Config
import scala.collection.mutable.ArrayBuffer
import java.io.File

/** Load Mutation Annotation Format (MAF) files from GDC / ICGC. Such as:
  * https://portal.gdc.cancer.gov/files/64cca73d-d95d-478e-8007-cf7a19809d0e
  *
  **/

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

}


