
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
case class Record00c81f9a2414494ea3f51cd50474eece(name: String)
case class Record291a0417a75448b2a6e435d8f0ba5d67(p_name: String, url: String, gene_set: Seq[Record00c81f9a2414494ea3f51cd50474eece], pathways_index: Long)
case class Record77167a89da6d445e94165bf5ba8c20cc(p_name: String, gene_set: Seq[Record00c81f9a2414494ea3f51cd50474eece], pathways_index: Long)
case class Record217f06d7a86241ddb40bf77d9b15d6a2(element: String)
case class Record7bc4f14c267b45239dc8464b1a602e47(polyphen_score: Double, sift_prediction: String, polyphen_prediction: String, flags: Seq[String], protein_end: Long, ts_strand: Long, impact: String, gene_id: String, amino_acids: String, sift_score: Double, cdna_start: Long, transcript_id: String, cdna_end: Long, exon: String, cds_end: Long, cds_start: Long, intron: String, consequence_terms: Seq[String], case_id: String, protein_start: Long, variant_allele: String, codons: String, distance: Long)
case class Recordaeadfbc3723e478fade2d9e571356794(vid: String, vend: Long, Tumor_Seq_Allele1: String, allele_string: String, Reference_Allele: String, oid: String, vstart: Long, projectId: String, donorId: String, seq_region_name: String, transcript_consequences: Seq[Record7bc4f14c267b45239dc8464b1a602e47], chromosome: String, occurrences_index: Long, assembly_name: String, end: Long, Tumor_Seq_Allele2: String, strand: Long, start: Long, input: String, most_severe_consequence: String)
case class Record7c60a1a9035c4007a89dfa61aa4c886c(donorId: String, transcript_consequences: Seq[Record7bc4f14c267b45239dc8464b1a602e47])
case class Record977e6e23259342d193fbd087ffb39b3f(p_name: String, donorId: Option[String], transcript_consequences: Option[Seq[Record7bc4f14c267b45239dc8464b1a602e47]], pathways_index: Long, gene_set: Seq[Record00c81f9a2414494ea3f51cd50474eece])
case class Record96de1ab0317848eeb15764ca2bde3ec0(p_name: String, transcript_consequences_index: Long, donorId: Option[String], transcript_consequences: Option[Seq[Record7bc4f14c267b45239dc8464b1a602e47]], pathways_index: Long, gene_set: Seq[Record00c81f9a2414494ea3f51cd50474eece])
case class Record461f05e2969248cba8d84a6c1f3c6fad(p_name: String, impact: Option[String], donorId: Option[String], pathways_index: Long, gene_set: Seq[Record00c81f9a2414494ea3f51cd50474eece])
case class Recordf39afeb52bef4e3581418642717634ed(p_name: String, gene_set_index: Long, impact: Option[String], donorId: Option[String], pathways_index: Long, gene_set: Seq[Record00c81f9a2414494ea3f51cd50474eece])
case class Record9e56c95f438140d19a3a56c4e98dd79e(p_name: String, impact: Option[String], donorId: Option[String], pathways_index: Long)
case class Record141fb3775f2c41b9b3f724a9f80744a6(p_name: String, burden: Double, pathways_index: Long, sid: Option[String])
case class Record65d3fb2d4d48473d8bf7df104937e650(p_name: String, pathways_index: Long, sid: Option[String], burden: Double)
case class Recorddd4508feeefa4042b03486d9dda81c7a(p_name: String, pathways_index: Long, sid: Option[String])
case class Record34d5440be46144f394b1aea5756c99fa(p_name: String, pathways_index: Long)
case class Record91ae3498cee94ec68ec1f8202ac49bc1(sid: String, burden: Double)
case class Recordab058d8cbffe4206bada9678bb32ff08(p_name: String, pathways_index: Long, burdens: Seq[Record91ae3498cee94ec68ec1f8202ac49bc1])
case class Recordc015e4b12cca461f9449bcf7efcf292f(pathway: String, burdens: Seq[Record91ae3498cee94ec68ec1f8202ac49bc1])
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
val pathways =

   def f = { 
 
 val x17 = pathways.withColumn("pathways_index", monotonically_increasing_id())
 .as[Record291a0417a75448b2a6e435d8f0ba5d67]
 
val x20 = x17.select("p_name", "gene_set", "pathways_index")
          
 .as[Record77167a89da6d445e94165bf5ba8c20cc]
 
val x21 = occurrences.withColumn("occurrences_index", monotonically_increasing_id())
 .as[Recordaeadfbc3723e478fade2d9e571356794]
 
val x24 = x21.select("donorId", "transcript_consequences")
          
 .as[Record7c60a1a9035c4007a89dfa61aa4c886c]
 
val x27 = x20.crossJoin(x24)
  .as[Record977e6e23259342d193fbd087ffb39b3f]
 
val x28 = x27.withColumn("transcript_consequences_index", monotonically_increasing_id())
 .as[Record96de1ab0317848eeb15764ca2bde3ec0]
 
val x32 = x28.flatMap{
 case x29 => 
      x29.transcript_consequences match {
     case Some(transcript_consequences) if transcript_consequences.nonEmpty => 
       transcript_consequences.map( x31 => Record461f05e2969248cba8d84a6c1f3c6fad(x29.p_name, Some(x31.impact), x29.donorId, x29.pathways_index, x29.gene_set) )
     case _ => Seq(Record461f05e2969248cba8d84a6c1f3c6fad(x29.p_name, None, x29.donorId, x29.pathways_index, x29.gene_set))
   }

}.as[Record461f05e2969248cba8d84a6c1f3c6fad]
 
val x33 = x32.withColumn("gene_set_index", monotonically_increasing_id())
 .as[Recordf39afeb52bef4e3581418642717634ed]
 
val x36 = x33.flatMap{
 case x34 => 
      if (x34.gene_set.isEmpty) Seq(Record9e56c95f438140d19a3a56c4e98dd79e(x34.p_name, x34.impact, x34.donorId, x34.pathways_index))
   else x34.gene_set.flatMap( x35 => if (x35.name == x35.gene_id) Seq(Record9e56c95f438140d19a3a56c4e98dd79e(x34.p_name, x34.impact, x34.donorId, x34.pathways_index)) else Seq() )

}.as[Record9e56c95f438140d19a3a56c4e98dd79e]
 
val x38 = x36
            .withColumn("burden", when(col("impact") === "HIGH", 0.8).otherwise(when(col("impact") === "MODERATE", 0.5).otherwise(when(col("impact") === "LOW", 0.3).otherwise(0.01))))
.withColumn("burden", when(col("burden").isNull, 0.0).otherwise(col("burden")))
 .withColumnRenamed("donorId", "sid")
 .as[Record141fb3775f2c41b9b3f724a9f80744a6]
 
val x40 = x38.groupByKey(x39 => Recorddd4508feeefa4042b03486d9dda81c7a(x39.p_name, x39.pathways_index, x39.sid))
 .agg(typed.sum[Record141fb3775f2c41b9b3f724a9f80744a6](x39 => x39.burden)
).mapPartitions{ it => it.map{ case (key, burden) =>
   Record65d3fb2d4d48473d8bf7df104937e650(key.p_name, key.pathways_index, key.sid, burden)
}}.as[Record65d3fb2d4d48473d8bf7df104937e650]
 
val x42 = x40.groupByKey(x41 => Record34d5440be46144f394b1aea5756c99fa(x41.p_name, x41.pathways_index)).mapGroups{
 case (key, value) => 
   val grp = value.flatMap(x41 => 
    (x41.sid) match {
      case (None) => Seq()
      case _ => Seq(Record91ae3498cee94ec68ec1f8202ac49bc1(x41.sid match { case Some(x) => x; case _ => "null" }, x41.burden))
   }).toSeq
   Recordab058d8cbffe4206bada9678bb32ff08(key.p_name, key.pathways_index, grp)
 }.as[Recordab058d8cbffe4206bada9678bb32ff08]
 
val x44 = x42.select("p_name", "burdens")
           .withColumnRenamed("p_name", "pathway")
 .as[Recordc015e4b12cca461f9449bcf7efcf292f]
 
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
