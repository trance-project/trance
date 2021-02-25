
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

case class Record16c4ed668ea644029c4947ee51040b7c(g_sample: String, call: Double)
case class Record314577fb8db2498d9da3023b7b54b32f(reference: String, genotypes: Seq[Record16c4ed668ea644029c4947ee51040b7c], alternate: String, vcf_index: Long, contig: String, start: Int)
case class Recorde0a6909bc00745e084f58c33c5b95ffe(genotypes: Seq[Record16c4ed668ea644029c4947ee51040b7c], contig: String, start: Int)
case class Recordbc9376f9929c4185bb55f1dc30ee21f1(g_start: Int, g_gene_name: String, genes_index: Long, g_gene_id: String, g_end: Int, g_contig: String)
case class Record074fc980aa4e4e82b923a6d688268ea2(g_start: Int, g_gene_name: String, g_end: Int, g_contig: String)
case class Record81ce27b3fc094a639cef93905b8cfb21(g_start: Int, g_gene_name: String, genotypes: Seq[Record16c4ed668ea644029c4947ee51040b7c], g_end: Int, contig: String, g_contig: String, start: Int)
case class Recordf0756592e99a4d13a58bfeed0aab8375(g_gene_name: String, g_sample: String, call: Double)
case class Recordf2d7473289244e00b43537460997ebed(sample: String, gene: String, burden: Double)
case class Record1eb449c904a149389dac1da1df6ae994(sample: String, gene: String)
case class Record2b64981091fd4d1cb202d981d0a3393f(sample: String)
case class Record7135effccb304191bef68f687daaf8fe(gene: String, burden: Double)
object Record7135effccb304191bef68f687daaf8fe {
  implicit val ord = Ordering.by(unapply)
}
case class Record937cad18174b442489acf77890e617f7(sample: String, burdens: Seq[Record7135effccb304191bef68f687daaf8fe])
object GeneBurdenCovariance {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf()
     .setAppName("GeneBurdenCovariance"+sf)
     .set("spark.sql.shuffle.partitions", Config.maxPartitions.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   
   import spark.implicits._
   val vloader = new VariantLoader(spark, "/mnt/app_hdd/scratch/biodata/supersm.vcf") //"/mnt/app_hdd/data/Data/Variants/supersm.vcf")
val vcf = vloader.loadDS
vcf.cache
vcf.count

val gtfLoader = new GeneLoader(spark)
val genes = gtfLoader.loadGTF("/mnt/app_hdd/scratch/biodata/genes.csv")    //"/mnt/app_hdd/data/Data/Map/genes.csv")
genes.cache
genes.count

   def f = { 
 
 val x13 = vcf.withColumn("vcf_index", monotonically_increasing_id())
 .as[Record314577fb8db2498d9da3023b7b54b32f]
 
val x15 = x13.select("genotypes","contig","start")
 .as[Recorde0a6909bc00745e084f58c33c5b95ffe] 
val x16 = genes.withColumn("genes_index", monotonically_increasing_id())
 .as[Recordbc9376f9929c4185bb55f1dc30ee21f1]
 
val x18 = x16.select("g_start","g_gene_name","g_end","g_contig")
 .as[Record074fc980aa4e4e82b923a6d688268ea2] 
val x21 = x15.join(x18, col("contig") === col("g_contig") && col("start") >= col("g_start") && col("g_end") >= col("start"), "inner")
  .as[Record81ce27b3fc094a639cef93905b8cfb21]
 
val x24 = x21.flatMap{ case x22 => 
   x22.genotypes.map( x23 => Recordf0756592e99a4d13a58bfeed0aab8375(x22.g_gene_name, x23.g_sample, x23.call) )
}.as[Recordf0756592e99a4d13a58bfeed0aab8375]
 
val x26 = x24
           .withColumnRenamed("g_sample", "sample")
 .withColumnRenamed("g_gene_name", "gene")
 .withColumnRenamed("call", "burden")
.withColumn("burden", when(col("burden").isNull, 0.0).otherwise(col("burden")))
 .as[Recordf2d7473289244e00b43537460997ebed]
 
val x28 = x26.groupByKey(x27 => Record1eb449c904a149389dac1da1df6ae994(x27.sample, x27.gene))
 .agg(typed.sum[Recordf2d7473289244e00b43537460997ebed](x27 => x27.burden)
).mapPartitions{ it => it.map{ case (key, burden) =>
   Recordf2d7473289244e00b43537460997ebed(key.sample, key.gene, burden)
}}.as[Recordf2d7473289244e00b43537460997ebed]
 
val x29 = x28
val Burden = x29
//Burden.print
//Burden.cache
//Burden.count
val x31 = Burden 
val x33 = x31.groupByKey(x32 => Record2b64981091fd4d1cb202d981d0a3393f(x32.sample)).mapGroups{
 case (key, value) => 
   val grp = value.flatMap(x32 => 
    () match {
      
      case _ => Seq(Record7135effccb304191bef68f687daaf8fe(x32.gene, x32.burden))
   }).toSeq.sorted
   Record937cad18174b442489acf77890e617f7(key.sample, grp)
 }.as[Record937cad18174b442489acf77890e617f7]
 
val x34 = x33
val Groups = x34
//Groups.print
//Groups.cache
//Groups.count

val rows = Groups.rdd.map(x => Vectors.dense(x.burdens.map(y => y.burden).toArray))
  .zipWithIndex.map{ case (v, i) => IndexedRow(i, v) }
val mat = new IndexedRowMatrix(rows).toBlockMatrix
val trans = mat.transpose
val covariance = trans.multiply(mat)
println(covariance.toLocalMatrix.toString())

}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start 
   println("GeneBurdenCovariance,standard,"+sf+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
