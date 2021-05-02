
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
case class Record684a0558c6724e82a514ffb9d4dd8d84(name: String)
case class Recorde2d351a6191340c48a72ed5037afdb76(p_name: String, url: String, gene_set: Seq[Record684a0558c6724e82a514ffb9d4dd8d84], pathways_index: Long)
case class Record0f8e53049fdb4189996bc548f4ff1866(p_name: String, gene_set: Seq[Record684a0558c6724e82a514ffb9d4dd8d84], pathways_index: Long)
case class Recordfd8e89609d074b9581ba34faebe37499(element: String)
case class Record67937087fcf34e099030b7376ee0841e(polyphen_score: Double, sift_prediction: String, polyphen_prediction: String, flags: Seq[String], protein_end: Long, ts_strand: Long, impact: String, gene_id: String, amino_acids: String, sift_score: Double, cdna_start: Long, transcript_id: String, cdna_end: Long, exon: String, cds_end: Long, cds_start: Long, intron: String, consequence_terms: Seq[String], case_id: String, protein_start: Long, variant_allele: String, codons: String, distance: Long)
case class Record902bc0d1576545a788145115023f6156(vid: String, vend: Long, Tumor_Seq_Allele1: String, allele_string: String, Reference_Allele: String, oid: String, vstart: Long, projectId: String, donorId: String, seq_region_name: String, transcript_consequences: Seq[Record67937087fcf34e099030b7376ee0841e], chromosome: String, occurrences_index: Long, assembly_name: String, end: Long, Tumor_Seq_Allele2: String, strand: Long, start: Long, input: String, most_severe_consequence: String)
case class Record5f10a1ab9fd14ce6b556cd2e7040cea7(donorId: String, transcript_consequences: Seq[Record67937087fcf34e099030b7376ee0841e])
case class Record80e2d942bbcb4240a6095f274da43d54(p_name: String, donorId: Option[String], transcript_consequences: Option[Seq[Record67937087fcf34e099030b7376ee0841e]], pathways_index: Long, gene_set: Seq[Record684a0558c6724e82a514ffb9d4dd8d84])
case class Recorde77f75da3303454ca09449c5142a355b(p_name: String, transcript_consequences_index: Long, donorId: Option[String], transcript_consequences: Option[Seq[Record67937087fcf34e099030b7376ee0841e]], pathways_index: Long, gene_set: Seq[Record684a0558c6724e82a514ffb9d4dd8d84])
case class Recordce898492da5b438c8d434de813b8c7b3(p_name: String, impact: Option[String], donorId: Option[String], pathways_index: Long, gene_set: Seq[Record684a0558c6724e82a514ffb9d4dd8d84])
case class Record6c8d60cb9d0f4d3aab4bcb5006eea98f(p_name: String, gene_set_index: Long, impact: Option[String], donorId: Option[String], pathways_index: Long, gene_set: Seq[Record684a0558c6724e82a514ffb9d4dd8d84])
case class Record2ae7a73cc9d347329d084b877ae29471(p_name: String, impact: Option[String], donorId: Option[String], pathways_index: Long)
case class Recorde2653acb809548f193fbe7a6aa29f978(p_name: String, burden: Double, pathways_index: Long, sid: Option[String])
case class Record59f6fcf145c1496eb3b57124c1a26aa9(p_name: String, pathways_index: Long, sid: Option[String], burden: Double)
case class Recordd782aad997d7420dbfde87df5ab98615(p_name: String, pathways_index: Long, sid: Option[String])
case class Recordc993d7abccdc47a49d31427181fc6226(p_name: String, pathways_index: Long)
case class Record63cfabbf418845daadb830699ff3d6e0(sid: String, burden: Double)
case class Recordad54f736c611422b93474cb53969e439(p_name: String, pathways_index: Long, burdens: Seq[Record63cfabbf418845daadb830699ff3d6e0])
case class Recordfd071c855a1f417caff8df29bea7a6de(pathway: String, burdens: Seq[Record63cfabbf418845daadb830699ff3d6e0])
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
 .as[Recorde2d351a6191340c48a72ed5037afdb76]
 
val x20 = x17.select("p_name", "gene_set", "pathways_index")
          
 .as[Record0f8e53049fdb4189996bc548f4ff1866]
 
val x21 = occurrences.withColumn("occurrences_index", monotonically_increasing_id())
 .as[Record902bc0d1576545a788145115023f6156]
 
val x24 = x21.select("donorId", "transcript_consequences")
          
 .as[Record5f10a1ab9fd14ce6b556cd2e7040cea7]
 
val x27 = x20.crossJoin(x24)
  .as[Record80e2d942bbcb4240a6095f274da43d54]
 
val x28 = x27.withColumn("transcript_consequences_index", monotonically_increasing_id())
 .as[Recorde77f75da3303454ca09449c5142a355b]
 
val x32 = x28.flatMap{
 case x29 => 
      x29.transcript_consequences match {
     case Some(transcript_consequences) if transcript_consequences.nonEmpty => 
       transcript_consequences.map( x31 => Recordce898492da5b438c8d434de813b8c7b3(x29.p_name, Some(x31.impact), x29.donorId, x29.pathways_index, x29.gene_set) )
     case _ => Seq(Recordce898492da5b438c8d434de813b8c7b3(x29.p_name, None, x29.donorId, x29.pathways_index, x29.gene_set))
   }

}.as[Recordce898492da5b438c8d434de813b8c7b3]
 
val x33 = x32.withColumn("gene_set_index", monotonically_increasing_id())
 .as[Record6c8d60cb9d0f4d3aab4bcb5006eea98f]
 
val x36 = x33.flatMap{
 case x34 => 
      if (x34.gene_set.isEmpty) Seq(Record2ae7a73cc9d347329d084b877ae29471(x34.p_name, x34.impact, x34.donorId, x34.pathways_index))
   else x34.gene_set.flatMap( x35 => if (x35.name == x35.gene_id) Seq(Record2ae7a73cc9d347329d084b877ae29471(x34.p_name, x34.impact, x34.donorId, x34.pathways_index)) else Seq() )

}.as[Record2ae7a73cc9d347329d084b877ae29471]
 
val x38 = x36
            .withColumn("burden", when(col("impact") === "HIGH", 0.8).otherwise(when(col("impact") === "MODERATE", 0.5).otherwise(when(col("impact") === "LOW", 0.3).otherwise(0.01))))
.withColumn("burden", when(col("burden").isNull, 0.0).otherwise(col("burden")))
 .withColumnRenamed("donorId", "sid")
 .as[Recorde2653acb809548f193fbe7a6aa29f978]
 
val x40 = x38.groupByKey(x39 => Recordd782aad997d7420dbfde87df5ab98615(x39.p_name, x39.pathways_index, x39.sid))
 .agg(typed.sum[Recorde2653acb809548f193fbe7a6aa29f978](x39 => x39.burden)
).mapPartitions{ it => it.map{ case (key, burden) =>
   Record59f6fcf145c1496eb3b57124c1a26aa9(key.p_name, key.pathways_index, key.sid, burden)
}}.as[Record59f6fcf145c1496eb3b57124c1a26aa9]
 
val x42 = x40.groupByKey(x41 => Recordc993d7abccdc47a49d31427181fc6226(x41.p_name, x41.pathways_index)).mapGroups{
 case (key, value) => 
   val grp = value.flatMap(x41 => 
    (x41.sid) match {
      case (None) => Seq()
      case _ => Seq(Record63cfabbf418845daadb830699ff3d6e0(x41.sid match { case Some(x) => x; case _ => "null" }, x41.burden))
   }).toSeq
   Recordad54f736c611422b93474cb53969e439(key.p_name, key.pathways_index, grp)
 }.as[Recordad54f736c611422b93474cb53969e439]
 
val x44 = x42.select("p_name", "burdens")
           .withColumnRenamed("p_name", "pathway")
 .as[Recordfd071c855a1f417caff8df29bea7a6de]
 
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
