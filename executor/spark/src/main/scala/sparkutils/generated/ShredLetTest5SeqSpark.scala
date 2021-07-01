
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
case class Record46456b0c8f8f4be487a4ac180d684e02(bcr_patient_uuid: String, bcr_aliquot_uuid: String)
case class Record443cad33080a448c9918804e92c8b977(cn_aliquot_uuid: String, cn_gene_id: String, cn_copy_number: Int)
case class Record16924c40199348beb51fc5c8e8a427bf(bcr_patient_uuid: String, cn_aliquot_uuid: String, cn_gene_id: String, bcr_aliquot_uuid: String, cn_copy_number: Int)
case class Record95b38c644c47481da49f58004a0effb9(sid: String, gene: String, cnum: Int)
case class Record4d748068a65b4c80be63b8354ab5d6b1(donorId: String, transcript_consequences: String)
case class Record0a55749238a14aa6bd56367cf94e5a09(o__F_transcript_consequences: String, o__F_donorId: String)
case class Record956627e82442449fb9fe541ccdf00340(hybrid_case: String, hybrid_scores: Record0a55749238a14aa6bd56367cf94e5a09)
case class Recordbe8bb2e77081425fb77117d3c6841efc(hybrid_scores: Record0a55749238a14aa6bd56367cf94e5a09)
case class Recordb07434e537b34865be4b65b1cb7a9a60(_LABEL: Record0a55749238a14aa6bd56367cf94e5a09)
case class Recordc7ee03cb0b26454b9467a5064bc54fff(impact: String, gene_id: String, _1: String)
case class Record46fe442f282242c3b63465146b8d903d(impact: String, gene_id: String, o__F_transcript_consequences_1: String)
case class Record05af637473d5481c97f955bfe7648111(_LABEL: Record0a55749238a14aa6bd56367cf94e5a09, impact: String, gene_id: String)
case class Record7828fa8adc244f549636d44867584a26(bcr_patient_uuid: String, impact: String, gene_id: String, bcr_aliquot_uuid: String, _LABEL: Record0a55749238a14aa6bd56367cf94e5a09)
case class Recorde6fb48af233b4a369e49a69a49d884ab(bcr_patient_uuid: String, impact: String, gene_id: String, cn_aliquot_uuid: String, cn_gene_id: String, bcr_aliquot_uuid: String, cn_copy_number: Int, _LABEL: Record0a55749238a14aa6bd56367cf94e5a09)
case class Record303d872fbd2546648a99dc1abc0755d4(hybrid_gene: String, hybrid_score: Double, _1: Record0a55749238a14aa6bd56367cf94e5a09)
case class Record70774fcc00aa4822b6dbbe5ee8011ee8(hybrid_gene: String, _1: Record0a55749238a14aa6bd56367cf94e5a09, hybrid_score: Double)
case class Record3f0ab17a53074bd9bb4ba4cb0972b377(_1: Record0a55749238a14aa6bd56367cf94e5a09, hybrid_gene: String)
case class Recordf37f015ea31b4edba38fe06a87fc92e8(_1: Record0a55749238a14aa6bd56367cf94e5a09, hybrid_gene: String, hybrid_score: Double)
case class Recordc116518470c641e0a86f4184be5ac8f9(hybrid_gene: String, hybrid_score: Double, hybrid_scores_1: Record0a55749238a14aa6bd56367cf94e5a09)
case class Recordb1f686d5011e43789efbcfe8121358cc(hybrid_scores_LABEL: Record0a55749238a14aa6bd56367cf94e5a09)
case class Record29448684bfbd47019b3d370df810e1cd(hybrid_gene: String, hybrid_score: Double)
object ShredLetTest5SeqSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf()
     .setAppName("ShredLetTest5SeqSpark"+sf)
     .set("spark.sql.shuffle.partitions", Config.maxPartitions.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   implicit val encoderRecord0a55749238a14aa6bd56367cf94e5a09: Encoder[Record0a55749238a14aa6bd56367cf94e5a09] = Encoders.product[Record0a55749238a14aa6bd56367cf94e5a09]
val udfRecord0a55749238a14aa6bd56367cf94e5a09 = udf { (o__F_transcript_consequences: String,o__F_donorId: String) => Record0a55749238a14aa6bd56367cf94e5a09(o__F_transcript_consequences,o__F_donorId) }

   import spark.implicits._
   val samples = spark.table("samples")
val IBag_samples__D = samples

val copynumber = spark.table("copynumber")
val IBag_copynumber__D = copynumber

val odict1 = spark.table("odict1")
val IBag_occurrences__D = odict1

// issue with partial shredding here
val odict2 = spark.table("odict2").drop("flags")
val IMap_occurrences__D_transcript_consequences = odict2

val odict3 = spark.table("odict3")
val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3

    def f = {
 
 
var start0 = System.currentTimeMillis()
val x62 = IBag_samples__D.select("bcr_patient_uuid", "bcr_aliquot_uuid")
          
 .as[Record46456b0c8f8f4be487a4ac180d684e02]
 
val x64 = IBag_copynumber__D.select("cn_aliquot_uuid", "cn_gene_id", "cn_copy_number")
          
 .as[Record443cad33080a448c9918804e92c8b977]
 
val x67 = x62.equiJoin(x64, 
 Seq("bcr_aliquot_uuid"), Seq("cn_aliquot_uuid"), "inner").as[Record16924c40199348beb51fc5c8e8a427bf]
 
val x69 = x67.select("bcr_patient_uuid", "cn_gene_id", "cn_copy_number")
           .withColumnRenamed("bcr_patient_uuid", "sid")
 .withColumnRenamed("cn_gene_id", "gene")
 .withColumnRenamed("cn_copy_number", "cnum")
.withColumn("cnum", when(col("cnum").isNull, 0).otherwise(col("cnum")))
 .as[Record95b38c644c47481da49f58004a0effb9]
 
val x70 = x69
val MBag_cnvTmp__D_1 = x70  
//MBag_cnvTmp__D_1.cache
//MBag_cnvTmp__D_1.count
val x72 = IBag_occurrences__D.select("donorId", "transcript_consequences")
          
 .as[Record4d748068a65b4c80be63b8354ab5d6b1]
 
val x74 = x72
            .withColumn("hybrid_scores", udfRecord0a55749238a14aa6bd56367cf94e5a09(col("transcript_consequences"), col("donorId")))
 .withColumnRenamed("donorId", "hybrid_case")
 .as[Record956627e82442449fb9fe541ccdf00340]
 
val x75 = x74
val MBag_initScores__D_1 = x75  
//MBag_initScores__D_1.cache
//MBag_initScores__D_1.count
val x77 = MBag_initScores__D_1.select("hybrid_scores")
          
 .as[Recordbe8bb2e77081425fb77117d3c6841efc]
 
val x79 = x77
           .withColumnRenamed("hybrid_scores", "_LABEL")
 .as[Recordb07434e537b34865be4b65b1cb7a9a60]
 
val x80 = x79.distinct 
val x81 = x80
val Dom_MBag_initScores__D_1_hybrid_scores1 = x81  
//Dom_MBag_initScores__D_1_hybrid_scores1.cache
//Dom_MBag_initScores__D_1_hybrid_scores1.count
val x83 = Dom_MBag_initScores__D_1_hybrid_scores1
          
 
 
val x85 = IMap_occurrences__D_transcript_consequences.select("impact", "gene_id", "_1")
          
 .as[Recordc7ee03cb0b26454b9467a5064bc54fff]
 
val x88 = x83.withColumn("o__F_transcript_consequences_LABEL", col("_LABEL").getField("o__F_transcript_consequences"))
   .as[Recordb07434e537b34865be4b65b1cb7a9a60].equiJoin(
   x85.withColumnRenamed("_1", "o__F_transcript_consequences_1").as[Record46fe442f282242c3b63465146b8d903d], 
   Seq("o__F_transcript_consequences_LABEL"), Seq("o__F_transcript_consequences_1"), "inner").drop("o__F_transcript_consequences_LABEL", "o__F_transcript_consequences_1")
   .as[Record05af637473d5481c97f955bfe7648111]
 
val x90 = IBag_samples__D.select("bcr_patient_uuid", "bcr_aliquot_uuid")
          
 .as[Record46456b0c8f8f4be487a4ac180d684e02]
 
val x93 = x88.withColumn("o__F_donorId_LABEL", col("_LABEL").getField("o__F_donorId"))
	.join(x90, col("o__F_donorId_LABEL") === col("bcr_patient_uuid"))
  	.as[Record7828fa8adc244f549636d44867584a26]
 
val x95 = IBag_copynumber__D.select("cn_aliquot_uuid", "cn_gene_id", "cn_copy_number")
          
 .as[Record443cad33080a448c9918804e92c8b977]
 
val x98 = x93.join(x95, col("bcr_aliquot_uuid") === col("cn_aliquot_uuid") && col("_LABEL").getField("o__F_donorId") === col("bcr_patient_uuid") && col("gene_id") === col("cn_gene_id"), "inner")
  .as[Recorde6fb48af233b4a369e49a69a49d884ab]
 
val x100 = x98.select("gene_id", "cn_copy_number", "impact", "_LABEL")
            .withColumn("hybrid_score", ((col("cn_copy_number") + 0.01) * when(col("impact") === "HIGH", 0.8).otherwise(when(col("impact") === "MODERATE", 0.5).otherwise(when(col("impact") === "LOW", 0.3).otherwise(0.01)))))
.withColumn("hybrid_score", when(col("hybrid_score").isNull, 0.0).otherwise(col("hybrid_score")))
 .withColumnRenamed("gene_id", "hybrid_gene")
 .withColumnRenamed("_LABEL", "_1")
 .as[Record303d872fbd2546648a99dc1abc0755d4]
 
val x102 = x100.groupByKey(x101 => Record3f0ab17a53074bd9bb4ba4cb0972b377(x101._1, x101.hybrid_gene))
 .agg(typed.sum[Record303d872fbd2546648a99dc1abc0755d4](x101 => x101.hybrid_score)
).mapPartitions{ it => it.map{ case (key, hybrid_score) =>
   Record70774fcc00aa4822b6dbbe5ee8011ee8(key.hybrid_gene, key._1, hybrid_score)
}}.as[Record70774fcc00aa4822b6dbbe5ee8011ee8]
 
val x103 = x102
val MMap_initScores__D_1_hybrid_scores_1 = x103  
//MMap_initScores__D_1_hybrid_scores_1.cache
//MMap_initScores__D_1_hybrid_scores_1.count
val x105 = MBag_initScores__D_1.select("hybrid_scores")
          
 .as[Recordbe8bb2e77081425fb77117d3c6841efc]
 
val x107 = MMap_initScores__D_1_hybrid_scores_1
          
 
 
val x110 = x105.withColumnRenamed("hybrid_scores", "hybrid_scores_LABEL")
   .as[Recordb1f686d5011e43789efbcfe8121358cc].equiJoin(
   x107.withColumnRenamed("_1", "hybrid_scores_1").as[Recordc116518470c641e0a86f4184be5ac8f9], 
   Seq("hybrid_scores_LABEL"), Seq("hybrid_scores_1"), "inner").drop("hybrid_scores_LABEL", "hybrid_scores_1")
   .as[Record29448684bfbd47019b3d370df810e1cd]
 
val x112 = x110
          
 
 
val x113 = x112
val MBag_LetTest5__D_1 = x113
MBag_LetTest5__D_1.count





var end0 = System.currentTimeMillis() - start0
println("Sequential,shredded,"+end0+",query,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("Sequential,shredded,"+sf+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
