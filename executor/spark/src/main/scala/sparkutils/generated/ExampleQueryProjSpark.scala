
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
case class Recordf236ac8cb274401b8a9161efdda92f77(name: String)
case class Record80ba30fcd4274734b603b0067e7e0c0b(p_name: String, url: String, gene_set: Seq[Recordf236ac8cb274401b8a9161efdda92f77], pathways_index: Long)
case class Record45f142447acd4343837baeec44b53850(p_name: String, gene_set: Seq[Recordf236ac8cb274401b8a9161efdda92f77], pathways_index: Long)
case class Record16a0738cb6ff47dc988da918292ef08b(element: String)
case class Record40ed9a8a2e4141b2ae2c8498dafe04ce(polyphen_score: Double, sift_prediction: String, polyphen_prediction: String, flags: Seq[String], protein_end: Long, ts_strand: Long, impact: String, gene_id: String, amino_acids: String, sift_score: Double, cdna_start: Long, transcript_id: String, cdna_end: Long, exon: String, cds_end: Long, cds_start: Long, intron: String, consequence_terms: Seq[String], case_id: String, protein_start: Long, variant_allele: String, codons: String, distance: Long)
case class Recorddd76bee7dcfb4558a39ec240bebc543f(vid: String, vend: Long, Tumor_Seq_Allele1: String, allele_string: String, Reference_Allele: String, oid: String, vstart: Long, projectId: String, donorId: String, seq_region_name: String, transcript_consequences: Seq[Record40ed9a8a2e4141b2ae2c8498dafe04ce], chromosome: String, occurrences_index: Long, assembly_name: String, end: Long, Tumor_Seq_Allele2: String, strand: Long, start: Long, input: String, most_severe_consequence: String)
case class Record75e3fa1e32834c68b683dcc6ceebd480(donorId: String, transcript_consequences: Seq[Record40ed9a8a2e4141b2ae2c8498dafe04ce])
case class Record93e5822eae5e4001958173ef3eef6603(p_name: String, donorId: Option[String], transcript_consequences: Option[Seq[Record40ed9a8a2e4141b2ae2c8498dafe04ce]], pathways_index: Long, gene_set: Seq[Recordf236ac8cb274401b8a9161efdda92f77])
case class Record2eef8849a1e740a8bea41e08d095fb64(p_name: String, transcript_consequences_index: Long, donorId: Option[String], transcript_consequences: Option[Seq[Record40ed9a8a2e4141b2ae2c8498dafe04ce]], pathways_index: Long, gene_set: Seq[Recordf236ac8cb274401b8a9161efdda92f77])
case class Record457516dbec554de9ab7db491475fdbed(p_name: String, impact: Option[String], donorId: Option[String], pathways_index: Long, gene_set: Seq[Recordf236ac8cb274401b8a9161efdda92f77])
case class Recordb1a1f8d846cb4a149a9fc0d571454373(p_name: String, gene_set_index: Long, impact: Option[String], donorId: Option[String], pathways_index: Long, gene_set: Seq[Recordf236ac8cb274401b8a9161efdda92f77])
case class Record3b9caae4ae9949ec8ca788bc2cc83b8b(p_name: String, impact: Option[String], donorId: Option[String], pathways_index: Long)
case class Recorddbb35248709f416197cd7c187900fa59(p_name: String, burden: Double, pathways_index: Long, sid: Option[String])
case class Record9635e200234a4409ad05c0507fa0f7d2(p_name: String, pathways_index: Long, sid: Option[String], burden: Double)
case class Record92027b4ec0ea45efbace9558c5a094a3(p_name: String, pathways_index: Long, sid: Option[String])
case class Record8042dabb1f2248ce803eb19550aaf320(p_name: String, pathways_index: Long)
case class Record633fad3b7a6a4dcbac1dff67941b5334(sid: String, burden: Double)
case class Record00286a3cf6f34922873c1af930be697f(p_name: String, pathways_index: Long, burdens: Seq[Record633fad3b7a6a4dcbac1dff67941b5334])
case class Record0d7b2fef7f124ee486a9df1898050292(pathway: String, burdens: Seq[Record633fad3b7a6a4dcbac1dff67941b5334])
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
val expression = geLoader.load("/mnt/app_hdd/data/expression", true, aliquot_file = "/mnt/app_hdd/data/fpkm_uq_case_aliquot.txt")

val occurrences = spark.read.json("/mnt/app_hdd/data/somatic/datasetPRAD")
val pathways =

   def f = { 
 
 val x17 = pathways.withColumn("pathways_index", monotonically_increasing_id())
 .as[Record80ba30fcd4274734b603b0067e7e0c0b]
 
val x20 = x17.select("p_name", "gene_set", "pathways_index")
          
 .as[Record45f142447acd4343837baeec44b53850]
 
val x21 = occurrences.withColumn("occurrences_index", monotonically_increasing_id())
 .as[Recorddd76bee7dcfb4558a39ec240bebc543f]
 
val x24 = x21.select("donorId", "transcript_consequences")
          
 .as[Record75e3fa1e32834c68b683dcc6ceebd480]
 
val x27 = x20.crossJoin(x24)
  .as[Record93e5822eae5e4001958173ef3eef6603]
 
val x28 = x27.withColumn("transcript_consequences_index", monotonically_increasing_id())
 .as[Record2eef8849a1e740a8bea41e08d095fb64]
 
val x32 = x28.flatMap{
 case x29 => 
      x29.transcript_consequences match {
     case Some(transcript_consequences) if transcript_consequences.nonEmpty => 
       transcript_consequences.map( x31 => Record457516dbec554de9ab7db491475fdbed(x29.p_name, Some(x31.impact), x29.donorId, x29.pathways_index, x29.gene_set) )
     case _ => Seq(Record457516dbec554de9ab7db491475fdbed(x29.p_name, None, x29.donorId, x29.pathways_index, x29.gene_set))
   }

}.as[Record457516dbec554de9ab7db491475fdbed]
 
val x33 = x32.withColumn("gene_set_index", monotonically_increasing_id())
 .as[Recordb1a1f8d846cb4a149a9fc0d571454373]
 
val x36 = x33.flatMap{
 case x34 => 
      if (x34.gene_set.isEmpty) Seq(Record3b9caae4ae9949ec8ca788bc2cc83b8b(x34.p_name, x34.impact, x34.donorId, x34.pathways_index))
   else x34.gene_set.flatMap( x35 => if (x35.name == x35.gene_id) Seq(Record3b9caae4ae9949ec8ca788bc2cc83b8b(x34.p_name, x34.impact, x34.donorId, x34.pathways_index)) else Seq() )

}.as[Record3b9caae4ae9949ec8ca788bc2cc83b8b]
 
val x38 = x36
            .withColumn("burden", when(col("impact") === "HIGH", 0.8).otherwise(when(col("impact") === "MODERATE", 0.5).otherwise(when(col("impact") === "LOW", 0.3).otherwise(0.01))))
.withColumn("burden", when(col("burden").isNull, 0.0).otherwise(col("burden")))
 .withColumnRenamed("donorId", "sid")
 .as[Recorddbb35248709f416197cd7c187900fa59]
 
val x40 = x38.groupByKey(x39 => Record92027b4ec0ea45efbace9558c5a094a3(x39.p_name, x39.pathways_index, x39.sid))
 .agg(typed.sum[Recorddbb35248709f416197cd7c187900fa59](x39 => x39.burden)
).mapPartitions{ it => it.map{ case (key, burden) =>
   Record9635e200234a4409ad05c0507fa0f7d2(key.p_name, key.pathways_index, key.sid, burden)
}}.as[Record9635e200234a4409ad05c0507fa0f7d2]
 
val x42 = x40.groupByKey(x41 => Record8042dabb1f2248ce803eb19550aaf320(x41.p_name, x41.pathways_index)).mapGroups{
 case (key, value) => 
   val grp = value.flatMap(x41 => 
    (x41.sid) match {
      case (None) => Seq()
      case _ => Seq(Record633fad3b7a6a4dcbac1dff67941b5334(x41.sid match { case Some(x) => x; case _ => "null" }, x41.burden))
   }).toSeq
   Record00286a3cf6f34922873c1af930be697f(key.p_name, key.pathways_index, grp)
 }.as[Record00286a3cf6f34922873c1af930be697f]
 
val x44 = x42.select("p_name", "burdens")
           .withColumnRenamed("p_name", "pathway")
 .as[Record0d7b2fef7f124ee486a9df1898050292]
 
val x45 = x44
val GMB = x45
//GMB.count

}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start 
   println("ExampleTest,standard,"+sf+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
