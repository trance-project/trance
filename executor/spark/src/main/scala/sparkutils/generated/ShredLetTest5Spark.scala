
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
case class Record5c5434db9afa49aeb4c2a6f468d76d5d(donorId: String)
case class Record5cae748dbe7b4fc08cba7bdd076d50b3(donorId: String, transcript_consequences: String)
case class Record539dbc570d3b42a5821e23b07fcd7d84(impact: String, gene_id: String, _1: String)
case class Record971185dbaf8e435aa82f73507f0145c4(impact: String, gene_id: String, transcript_consequences_1: String)
case class Record041a243b09d2431cae6684456649e322(donorId: String, transcript_consequences_LABEL: String)
case class Record2253b8dd2be14f15bebd204819e0174d(impact: String, gene_id: String, donorId: String, transcript_consequences_1: String)
case class Recorda908d3982c4943dc997ff37b92332b9b(bcr_patient_uuid: String, bcr_aliquot_uuid: String)
case class Recordaf661ff228054b5e8e0d2dc2f9a9dd3b(bcr_patient_uuid: String, impact: String, gene_id: String, donorId: String, bcr_aliquot_uuid: String, transcript_consequences_1: String)
case class Record105e9dd4a614492591f5a689073bd6a8(cn_aliquot_uuid: String, cn_gene_id: String, cn_copy_number: Int)
case class Record860916802d344edb851411d6588eae76(bcr_patient_uuid: String, impact: String, gene_id: String, donorId: String, cn_aliquot_uuid: String, cn_gene_id: String, bcr_aliquot_uuid: String, cn_copy_number: Int, transcript_consequences_1: String)
case class Record46b53b824756468bb78878aae740bfa7(x: String, hybrid_gene: String, hybrid_score: Double)
case class Record74590227092e444995c7e9c37be29149(x: String, hybrid_gene: String)
case class Record90d775101000438fbfc7bd13dec9d104(donorId: String, x: String, hybrid_gene: String, hybrid_score: Double)
case class Record5e08d31773b34624ad3503bf1d4d6a86(hybrid_gene: String, hybrid_score: Double)
object ShredLetTest5Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf()
     .setAppName("ShredLetTest5Spark"+sf)
     .set("spark.sql.shuffle.partitions", Config.maxPartitions.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   
   import spark.implicits._

val biospecLoader = new BiospecLoader(spark)
val biospec = biospecLoader.load("/nfs_qc4/genomics/gdc/biospecimen/aliquot/")
//val biospec = biospecLoader.load("/nfs_qc4/genomics/gdc/biospecimen/aliquot/nationwidechildrens.org_biospecimen_aliquot_brca.txt")

val IBag_samples__D = biospec
IBag_samples__D.cache
IBag_samples__D.count

val odict1 = spark.read.json(s"/nfs_qc4/genomics/gdc/somatic//odict1Full/").as[OccurrDict1]
//val odict1 = spark.table("fodict1")
val IBag_occurrences__D = odict1
IBag_occurrences__D.cache
IBag_occurrences__D.count
val odict2 = spark.read.json(s"/nfs_qc4/genomics/gdc/somatic//odict2Full/").as[OccurTransDict2Mid]
//val odict2 = spark.table("fodict2")
val IMap_occurrences__D_transcript_consequences = odict2
IMap_occurrences__D_transcript_consequences.cache
IMap_occurrences__D_transcript_consequences.count


val cnLoader = new CopyNumberLoader(spark)
val copynumber = cnLoader.load("/nfs_qc4/genomics/gdc/gene_level/", true)
.withColumn("cn_gene_id", substring(col("cn_gene_id"), 1,15)).as[CopyNumber]
val IBag_copynumber__D = copynumber
IBag_copynumber__D.cache
IBag_copynumber__D.count

/**   val samples = spark.table("samples")
val IBag_samples__D = samples

val copynumber = spark.table("copynumber")
val IBag_copynumber__D = copynumber

val odict1 = spark.table("fodict1")
val IBag_occurrences__D = odict1

// issue with partial shredding here
val odict2 = spark.table("fodict2").drop("flags")
val IMap_occurrences__D_transcript_consequences = odict2

val odict3 = spark.table("fodict3")
val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
**/
    def f = {
 
 
var start0 = System.currentTimeMillis()
val x39 = IBag_occurrences__D.select("transcript_consequences")
  .withColumnRenamed("transcript_consequences", "donorId")
          
 .as[Record5c5434db9afa49aeb4c2a6f468d76d5d]
 
val x41 = IBag_occurrences__D.select("donorId", "transcript_consequences")
          
 .as[Record5cae748dbe7b4fc08cba7bdd076d50b3]
 
val x43 = IMap_occurrences__D_transcript_consequences.select("impact", "gene_id", "_1")
          
 .as[Record539dbc570d3b42a5821e23b07fcd7d84]
 
val x46 = x41.withColumnRenamed("transcript_consequences", "transcript_consequences_LABEL")
   .as[Record041a243b09d2431cae6684456649e322].equiJoin(
   x43.withColumnRenamed("_1", "transcript_consequences_1").as[Record971185dbaf8e435aa82f73507f0145c4], 
   Seq("transcript_consequences_LABEL"), Seq("transcript_consequences_1"), "inner").drop("transcript_consequences_LABEL")//, "transcript_consequences_1")
   .as[Record2253b8dd2be14f15bebd204819e0174d]
 
val x48 = IBag_samples__D.select("bcr_patient_uuid", "bcr_aliquot_uuid")
          
 .as[Recorda908d3982c4943dc997ff37b92332b9b]
 
val x51 = x46.join(x48, col("donorId") === col("bcr_patient_uuid"))
  .as[Recordaf661ff228054b5e8e0d2dc2f9a9dd3b]
 
val x53 = IBag_copynumber__D.select("cn_aliquot_uuid", "cn_gene_id", "cn_copy_number")
          
 .as[Record105e9dd4a614492591f5a689073bd6a8]
 
val x56 = x51.join(x53, col("bcr_aliquot_uuid") === col("cn_aliquot_uuid") && col("donorId") === col("bcr_patient_uuid") && col("gene_id") === col("cn_gene_id"), "inner")
  .as[Record860916802d344edb851411d6588eae76]
 
val x58 = x56.select("gene_id", "cn_copy_number", "impact", "transcript_consequences_1")
            .withColumn("hybrid_score", ((col("cn_copy_number") + 0.01) * when(col("impact") === "HIGH", 0.8).otherwise(when(col("impact") === "MODERATE", 0.5).otherwise(when(col("impact") === "LOW", 0.3).otherwise(0.01)))))
.withColumn("hybrid_score", when(col("hybrid_score").isNull, 0.0).otherwise(col("hybrid_score")))
 .withColumnRenamed("transcript_consequences_1", "x")
 .withColumnRenamed("gene_id", "hybrid_gene")
 .as[Record46b53b824756468bb78878aae740bfa7]
 
val x60 = x58.groupByKey(x59 => Record74590227092e444995c7e9c37be29149(x59.x, x59.hybrid_gene))
 .agg(typed.sum[Record46b53b824756468bb78878aae740bfa7](x59 => x59.hybrid_score)
).mapPartitions{ it => it.map{ case (key, hybrid_score) =>
   Record46b53b824756468bb78878aae740bfa7(key.x, key.hybrid_gene, hybrid_score)
}}.as[Record46b53b824756468bb78878aae740bfa7]
 
val x63 = x39.equiJoin(x60, 
 Seq("donorId"), Seq("x"), "inner").as[Record90d775101000438fbfc7bd13dec9d104]
 
val x65 = x63.select("hybrid_gene", "hybrid_score")
          
 .as[Record5e08d31773b34624ad3503bf1d4d6a86]
 
val x66 = x65
val MBag_LetTest5__D_1 = x66
MBag_LetTest5__D_1.count

var end0 = System.currentTimeMillis() - start0
println("Inlined,shredded,"+end0+",query,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("Inlined,shredded,"+sf+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
