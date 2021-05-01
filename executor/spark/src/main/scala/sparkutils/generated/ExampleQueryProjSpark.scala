
package sparkutils.generated
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.scalalang._
import scala.collection.mutable.HashMap
import sparkutils._
import sparkutils.loader._
import sparkutils.skew.SkewDataset._
case class Record3394c2db27f8492da9aa2d7a608bea8c(date_of_shipment: String, quantity: Double, bcr_patient_uuid: String, biospecimen_barcode_bottom: String, is_derived_from_ffpe: String, concentration: Double, plate_row: String, plate_column: Int, bcr_sample_barcode: String, bcr_aliquot_uuid: String, plate_id: String, source_center: Int, bcr_aliquot_barcode: String, center_id: String, volume: Double, samples_index: Long)
case class Record521d782ca27b409ebf810198dc69b60c(bcr_patient_uuid: String, bcr_aliquot_uuid: String)
case class Record50d57db519c1498a8b5ce1e0950b267e(cn_end: Int, cn_chromosome: String, cn_start: Int, cn_aliquot_uuid: String, cn_gene_name: String, copynumber_index: Long, cn_gene_id: String, min_copy_number: Int, max_copy_number: Int, cn_copy_number: Int)
case class Record2918b5590b2a49e4815f67b1a08599c9(cn_aliquot_uuid: String, cn_gene_id: String, cn_copy_number: Int)
case class Recordbf0116ae52c3477689280dfb113991f6(bcr_patient_uuid: String, cn_aliquot_uuid: String, cn_gene_id: String, bcr_aliquot_uuid: String, cn_copy_number: Int)
case class Recordda8cb4f93cd64c169c1de235152ef5b7(sid: String, gene: String, cnum: Int)
case class Recordaa14ce664f264b63adcf71a8efcd97bb(element: String)
case class Record9cb295cb40604caca5e94cd59a65f607(polyphen_score: Double, sift_prediction: String, polyphen_prediction: String, flags: Seq[String], protein_end: Long, ts_strand: Long, impact: String, gene_id: String, amino_acids: String, sift_score: Double, cdna_start: Long, transcript_id: String, cdna_end: Long, exon: String, cds_end: Long, cds_start: Long, intron: String, consequence_terms: Seq[String], case_id: String, protein_start: Long, variant_allele: String, codons: String, distance: Long)
case class Record645752081f9548b0a6629e5a5d7e2318(vid: String, vend: Long, Tumor_Seq_Allele1: String, allele_string: String, Reference_Allele: String, oid: String, vstart: Long, projectId: String, donorId: String, seq_region_name: String, transcript_consequences: Seq[Record9cb295cb40604caca5e94cd59a65f607], chromosome: String, occurrences_index: Long, assembly_name: String, end: Long, Tumor_Seq_Allele2: String, strand: Long, start: Long, input: String, most_severe_consequence: String)
case class Recordbcb9bc39d6e542a2a45de3a042775da8(oid: String, donorId: String, transcript_consequences: Seq[Record9cb295cb40604caca5e94cd59a65f607], occurrences_index: Long)
case class Recordb8497a4607b64b9db860e950b2f21d81(transcript_consequences_index: Long, oid: String, donorId: String, transcript_consequences: Seq[Record9cb295cb40604caca5e94cd59a65f607], occurrences_index: Long)
case class Record53432ca5d8634b89941f5870918a234e(oid: String, impact: Option[String], gene_id: Option[String], donorId: String, occurrences_index: Long)
case class Recordb606fd21c8144411baad5493addbb19d(sid: String, gene: String, cnum: Int, cnvCases1_index: Long)
case class Record7908de6a47c14c00a2779de9a1051ae9(cnum: Option[Int], oid: String, impact: Option[String], gene_id: Option[String], donorId: String, occurrences_index: Long, gene: Option[String], sid: Option[String])
case class Record477ce21113d94d3ebc75f25d382b09bc(oid: String, gene1: Option[String], donorId: String, score1: Double, occurrences_index: Long)
case class Record914c4a1cfd1e4564934250e6bc0788c4(oid: String, gene1: Option[String], donorId: String, occurrences_index: Long)
case class Recordd8341bd87066476aacdabce1b3fae4f4(oid: String, donorId: String, occurrences_index: Long)
case class Record384fb3c2ab3d4d76aa37de3ca1d1c643(gene1: String, score1: Double)
case class Record3e711ef3f1294720a339d586191ee3cc(oid: String, donorId: String, occurrences_index: Long, cands1: Seq[Record384fb3c2ab3d4d76aa37de3ca1d1c643])
case class Recordce1010d43fd6438d844511fcb4293722(oid: String, sid1: String, cands1: Seq[Record384fb3c2ab3d4d76aa37de3ca1d1c643])
object ExampleQueryProjSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf()
     .setAppName("ExampleQueryProjSpark"+sf)
     .set("spark.sql.shuffle.partitions", Config.maxPartitions.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   
   import spark.implicits._
   val sloader = new BiospecLoader(spark)
val samples = sloader.load("/mnt/app_hdd/data/biospecimen/aliquot/nationwidechildrens.org_biospecimen_aliquot_dlbc.txt")
val cloader = new CopyNumberLoader(spark)
val copynumber = cloader.load("/mnt/app_hdd/data/cnv", true)

val geLoader = new GeneExpressionLoader(spark)
val expression = geLoader.load("/mnt/app_hdd/data/expression", true)

val occurrences = spark.read.json("/mnt/app_hdd/data/somatic/datasetPRAD")

   def f = { 
 
 val x21 = samples.withColumn("samples_index", monotonically_increasing_id())
 .as[Record3394c2db27f8492da9aa2d7a608bea8c]
 
val x24 = x21.select("bcr_patient_uuid", "bcr_aliquot_uuid")
          
 .as[Record521d782ca27b409ebf810198dc69b60c]
 
val x25 = copynumber.withColumn("copynumber_index", monotonically_increasing_id())
 .as[Record50d57db519c1498a8b5ce1e0950b267e]
 
val x28 = x25.select("cn_aliquot_uuid", "cn_gene_id", "cn_copy_number")
          
 .as[Record2918b5590b2a49e4815f67b1a08599c9]
 
val x31 = x24.equiJoin(x28, 
 Seq("bcr_aliquot_uuid"), Seq("cn_aliquot_uuid"), "inner").as[Recordbf0116ae52c3477689280dfb113991f6]
 
val x33 = x31.select("bcr_patient_uuid", "cn_gene_id", "cn_copy_number")
           .withColumnRenamed("bcr_patient_uuid", "sid")
 .withColumnRenamed("cn_gene_id", "gene")
 .withColumnRenamed("cn_copy_number", "cnum")
.withColumn("cnum", when(col("cnum").isNull, 0).otherwise(col("cnum")))
 .as[Recordda8cb4f93cd64c169c1de235152ef5b7]
 
val x34 = x33
val cnvCases1 = x34  
//cnvCases1.cache
//cnvCases1.count
val x35 = occurrences.withColumn("occurrences_index", monotonically_increasing_id())
 .as[Record645752081f9548b0a6629e5a5d7e2318]
 
val x38 = x35.select("oid", "donorId", "transcript_consequences", "occurrences_index")
          
 .as[Recordbcb9bc39d6e542a2a45de3a042775da8]
 
val x39 = x38.withColumn("transcript_consequences_index", monotonically_increasing_id())
 .as[Recordb8497a4607b64b9db860e950b2f21d81]
 
val x42 = x39.flatMap{
 case x40 => 
      if (x40.transcript_consequences.isEmpty) Seq(Record53432ca5d8634b89941f5870918a234e(x40.oid, None, None, x40.donorId, x40.occurrences_index))
   else x40.transcript_consequences.flatMap( x41 => if (x41.sift_score > 0.0) Seq(Record53432ca5d8634b89941f5870918a234e(x40.oid, Some(x41.impact), Some(x41.gene_id), x40.donorId, x40.occurrences_index)) else Seq() )

}.as[Record53432ca5d8634b89941f5870918a234e]
 
val x43 = cnvCases1.withColumn("cnvCases1_index", monotonically_increasing_id())
 .as[Recordb606fd21c8144411baad5493addbb19d]
 
val x46 = x43.select("sid", "gene", "cnum")
          
 .as[Recordda8cb4f93cd64c169c1de235152ef5b7]
 
val x49 = x42.equiJoin(x46, 
 Seq("gene_id","donorId"), Seq("gene","sid"), "left_outer").as[Record7908de6a47c14c00a2779de9a1051ae9]
 
val x51 = x49.select("cnum", "oid", "impact", "gene_id", "donorId", "occurrences_index")
            .withColumn("score1", ((col("cnum") + 0.01) * when(col("impact") === "HIGH", 0.8).otherwise(when(col("impact") === "MODERATE", 0.5).otherwise(when(col("impact") === "LOW", 0.3).otherwise(0.01)))))
.withColumn("score1", when(col("score1").isNull, 0.0).otherwise(col("score1")))
 .withColumnRenamed("gene_id", "gene1")
 .as[Record477ce21113d94d3ebc75f25d382b09bc]
 
val x53 = x51.groupByKey(x52 => Record914c4a1cfd1e4564934250e6bc0788c4(x52.oid, x52.gene1, x52.donorId, x52.occurrences_index))
 .agg(typed.sum[Record477ce21113d94d3ebc75f25d382b09bc](x52 => x52.score1)
).mapPartitions{ it => it.map{ case (key, score1) =>
   Record477ce21113d94d3ebc75f25d382b09bc(key.oid, key.gene1, key.donorId, score1, key.occurrences_index)
}}.as[Record477ce21113d94d3ebc75f25d382b09bc]
 
val x55 = x53.groupByKey(x54 => Recordd8341bd87066476aacdabce1b3fae4f4(x54.oid, x54.donorId, x54.occurrences_index)).mapGroups{
 case (key, value) => 
   val grp = value.flatMap(x54 => 
    (x54.gene1) match {
      case (None) => Seq()
      case _ => Seq(Record384fb3c2ab3d4d76aa37de3ca1d1c643(x54.gene1 match { case Some(x) => x; case _ => "null" }, x54.score1))
   }).toSeq
   Record3e711ef3f1294720a339d586191ee3cc(key.oid, key.donorId, key.occurrences_index, grp)
 }.as[Record3e711ef3f1294720a339d586191ee3cc]
 
val x57 = x55.select("oid", "donorId", "cands1")
           .withColumnRenamed("donorId", "sid1")
 .as[Recordce1010d43fd6438d844511fcb4293722]
 
val x58 = x57
val hybridScore1 = x58
//hybridScore1.count


}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start 
   println("ExampleTest,standard,"+sf+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
