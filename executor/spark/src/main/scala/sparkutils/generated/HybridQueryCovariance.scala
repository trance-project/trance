
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

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}

case class Recordcb602f32cb3845058f444b60c5130df8(date_of_shipment: String, quantity: Double, bcr_patient_uuid: String, biospecimen_barcode_bottom: String, is_derived_from_ffpe: String, concentration: Double, plate_row: String, plate_column: Int, bcr_sample_barcode: String, bcr_aliquot_uuid: String, plate_id: String, source_center: Int, bcr_aliquot_barcode: String, center_id: String, volume: Double, samples_index: Long)
case class Recordae0e973d41434b108f26926d6edbe2ee(bcr_patient_uuid: String, bcr_aliquot_uuid: String, samples_index: Long)
case class Recorda1c59ff7c0d9438ea5b62d3bffff0b56(element: String)
case class Recordc0f787aa058e49bb9476a978ac90cbb5(polyphen_score: Double, sift_prediction: String, polyphen_prediction: String, flags: Seq[String], protein_end: Long, ts_strand: Long, impact: String, gene_id: String, amino_acids: String, sift_score: Double, cdna_start: Long, transcript_id: String, cdna_end: Long, exon: String, cds_end: Long, cds_start: Long, intron: String, consequence_terms: Seq[String], case_id: String, protein_start: Long, variant_allele: String, codons: String, distance: Long)
case class Record5a59629519964943a9c6bdbff79cbcec(vid: String, vend: Long, Tumor_Seq_Allele1: String, allele_string: String, Reference_Allele: String, oid: String, vstart: Long, projectId: String, donorId: String, seq_region_name: String, transcript_consequences: Seq[Recordc0f787aa058e49bb9476a978ac90cbb5], chromosome: String, occurrences_index: Long, assembly_name: String, end: Long, Tumor_Seq_Allele2: String, strand: Long, start: Long, input: String, most_severe_consequence: String)
case class Recorde038801808a94455bc00f7db6b8b4299(donorId: String, transcript_consequences: Seq[Recordc0f787aa058e49bb9476a978ac90cbb5])
case class Recordec6f9e247c0d4d95ab56a8417f095d84(bcr_patient_uuid: String, donorId: Option[String], transcript_consequences: Option[Seq[Recordc0f787aa058e49bb9476a978ac90cbb5]], bcr_aliquot_uuid: String, samples_index: Long)
case class Recorda1fb5fa473a44977924a1cfb8725b415(bcr_patient_uuid: String, transcript_consequences_index: Long, donorId: Option[String], transcript_consequences: Option[Seq[Recordc0f787aa058e49bb9476a978ac90cbb5]], bcr_aliquot_uuid: String, samples_index: Long)
case class Record9a5f61fafe744c6088cba1e595399eb1(polyphen_score: Option[Double], bcr_patient_uuid: String, impact: Option[String], gene_id: Option[String], sift_score: Option[Double], bcr_aliquot_uuid: String, samples_index: Long)
case class Recordb73729f887d1453fad440c86d5991aa8(cn_end: Int, cn_chromosome: String, cn_start: Int, cn_aliquot_uuid: String, cn_gene_name: String, copynumber_index: Long, cn_gene_id: String, min_copy_number: Int, max_copy_number: Int, cn_copy_number: Int)
case class Record8d34c94ff1ea449eb47d8fcec0d35544(cn_aliquot_uuid: String, cn_gene_id: String, min_copy_number: Int, max_copy_number: Int, cn_copy_number: Int)
case class Recordb077f93ea2be4331af2f1c94e3c3ec54(polyphen_score: Option[Double], bcr_patient_uuid: String, impact: Option[String], gene_id: Option[String], sift_score: Option[Double], cn_aliquot_uuid: Option[String], cn_gene_id: Option[String], bcr_aliquot_uuid: String, min_copy_number: Option[Int], max_copy_number: Option[Int], cn_copy_number: Option[Int], samples_index: Long)
case class Record305b06356cb047938d48487c6bc710d1(bcr_patient_uuid: String, score: Double, gene: Option[String], samples_index: Long)
case class Record8b670e30d5ea4ffa8e3e9936642d8f7d(bcr_patient_uuid: String, gene: Option[String], samples_index: Long, score: Double)
case class Record648107100a094e4694ed1b828daf8076(bcr_patient_uuid: String, gene: Option[String], samples_index: Long)
case class Recorddcdf49c611624e0c99cfa1120aca3195(bcr_patient_uuid: String, samples_index: Long, gene: Option[String], score: Double)
case class Record3d09abdab8a24d4189e1936203df19d4(bcr_patient_uuid: String, samples_index: Long)
case class Record16d3e5a9b55040229eb03fd51c64ff6d(gene: String, score: Double)
// error about unapply not found?
// { 
//   implicit val ord = Ordering.by(unapply) 
// }
case class Record9ad0b3d529684055bee413a5abd1272d(bcr_patient_uuid: String, samples_index: Long, scores: Seq[Record16d3e5a9b55040229eb03fd51c64ff6d])
case class Record8eedb2c661fc4b28be54c5f9d99d7d79(sample: String, scores: Seq[Record16d3e5a9b55040229eb03fd51c64ff6d])
object HybridQueryCovariance {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf()
     .setAppName("HybridQueryCovariance")
     .set("spark.sql.shuffle.partitions", Config.maxPartitions.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   
   val basepath = "/Users/jac/data/dlbc/dlbc"
   import spark.implicits._
   
   val sloader = new BiospecLoader(spark)
   val samples = sloader.load(s"$basepath/samples.txt")
   samples.cache
   samples.count

  val cloader = new CopyNumberLoader(spark)
  val copynumber = spark.read.option("header", true).schema(cloader.schema)
		   	.csv(s"$basepath/cnv.csv").as[CopyNumber]
  copynumber.cache
  copynumber.count

  val occurrences = spark.read.json(s"$basepath/occurrences.json").as[OccurrenceMid]
  occurrences.cache
  occurrences.count

   def f = { 
 
     val x18 = samples.withColumn("samples_index", monotonically_increasing_id())
     .as[Recordcb602f32cb3845058f444b60c5130df8]
     
    val x21 = x18.select("bcr_patient_uuid", "bcr_aliquot_uuid", "samples_index")
              
     .as[Recordae0e973d41434b108f26926d6edbe2ee]
     
    val x22 = occurrences.withColumn("occurrences_index", monotonically_increasing_id())
     .as[Record5a59629519964943a9c6bdbff79cbcec]
     
    val x25 = x22.select("donorId", "transcript_consequences")
              
     .as[Recorde038801808a94455bc00f7db6b8b4299]
     
    val x28 = x21.equiJoin(x25, 
     Seq("bcr_patient_uuid"), Seq("donorId"), "left_outer").as[Recordec6f9e247c0d4d95ab56a8417f095d84]
     
    val x29 = x28.withColumn("transcript_consequences_index", monotonically_increasing_id())
     .as[Recorda1fb5fa473a44977924a1cfb8725b415]
     
    val x33 = x29.flatMap{
     case x30 => 
          x30.transcript_consequences match {
         case Some(transcript_consequences) if transcript_consequences.nonEmpty => 
           transcript_consequences.map( x32 => Record9a5f61fafe744c6088cba1e595399eb1(Some(x32.polyphen_score), x30.bcr_patient_uuid, Some(x32.impact), Some(x32.gene_id), Some(x32.sift_score), x30.bcr_aliquot_uuid, x30.samples_index) )
         case _ => Seq(Record9a5f61fafe744c6088cba1e595399eb1(None, x30.bcr_patient_uuid, None, None, None, x30.bcr_aliquot_uuid, x30.samples_index))
       }

    }.as[Record9a5f61fafe744c6088cba1e595399eb1]
     
    val x34 = copynumber.withColumn("copynumber_index", monotonically_increasing_id())
     .as[Recordb73729f887d1453fad440c86d5991aa8]
     
    val x37 = x34.select("cn_aliquot_uuid", "cn_gene_id", "min_copy_number", "max_copy_number", "cn_copy_number")
              
     .as[Record8d34c94ff1ea449eb47d8fcec0d35544]
     
    val x40 = x33.equiJoin(x37, 
     Seq("gene_id","bcr_aliquot_uuid"), Seq("cn_gene_id","cn_aliquot_uuid"), "left_outer").as[Recordb077f93ea2be4331af2f1c94e3c3ec54]
     
    val x42 = x40.select("polyphen_score", "bcr_patient_uuid", "impact", "gene_id", "sift_score", "min_copy_number", "max_copy_number", "cn_copy_number", "samples_index")
                .withColumn("score", ((((((col("cn_copy_number") + col("min_copy_number")) + col("max_copy_number")) + 0.01 / 3) * when(col("impact") === "HIGH", 0.8).otherwise(when(col("impact") === "MODERATE", 0.5).otherwise(when(col("impact") === "LOW", 0.3).otherwise(0.01)))) * (col("polyphen_score") + 0.01)) * (col("sift_score") + 0.01)))
    .withColumn("score", when(col("score").isNull, 0.0001).otherwise(col("score")))
     .withColumnRenamed("gene_id", "gene")
     .as[Record305b06356cb047938d48487c6bc710d1]
     
    val x44 = x42.groupByKey(x43 => Record648107100a094e4694ed1b828daf8076(x43.bcr_patient_uuid, x43.gene, x43.samples_index))
     .agg(typed.sum[Record305b06356cb047938d48487c6bc710d1](x43 => x43.score)
    ).mapPartitions{ it => it.map{ case (key, score) =>
       Record8b670e30d5ea4ffa8e3e9936642d8f7d(key.bcr_patient_uuid, key.gene, key.samples_index, score)
    }}.as[Record8b670e30d5ea4ffa8e3e9936642d8f7d]
     
    val x46 = x44
              
     
     
    val x48 = x46.groupByKey(x47 => Record3d09abdab8a24d4189e1936203df19d4(x47.bcr_patient_uuid, x47.samples_index)).mapGroups{
     case (key, value) => 
       val grp = value.flatMap(x47 => 
        (x47.gene) match {
          case (None) => Seq()
          case _ => Seq(Record16d3e5a9b55040229eb03fd51c64ff6d(x47.gene match { case Some(x) => x; case _ => "null" }, x47.score))
       }).toSeq.sortBy(r => r.gene)
       Record9ad0b3d529684055bee413a5abd1272d(key.bcr_patient_uuid, key.samples_index, grp)
     }.as[Record9ad0b3d529684055bee413a5abd1272d]
     
    val x50 = x48.select("bcr_patient_uuid", "scores")
               .withColumnRenamed("bcr_patient_uuid", "sample")
     .as[Record8eedb2c661fc4b28be54c5f9d99d7d79]
     
    val x51 = x50
    val HybridQuery = x51
    // HybridQuery.print
    // HybridQuery.cache
    // HybridQuery.count

    val rows = HybridQuery.rdd.filter(x => x.scores.nonEmpty).map(x => Vectors.dense(x.scores.map(y => y.score).toArray))
      .zipWithIndex.map{ case (v, i) => IndexedRow(i, v) }
    val mat = new IndexedRowMatrix(rows)
    
    val bmat = mat.toBlockMatrix()
    val trans = bmat.transpose
    val covariance = trans.multiply(bmat)
    covariance.toLocalMatrix()

  }

  var start = System.currentTimeMillis()
  val res = f
  var end = System.currentTimeMillis() - start 
  println(res.toString())
  println("HybridQueryCovariance,standard,"+sf+","+end+",total,"+spark.sparkContext.applicationId)

 }
}
