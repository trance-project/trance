
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

case class Record3bc4aa8d5c684938b781a97bd584ba21(genotypes: String, contig: String, start: Int)
case class Record957525f7fd8e4b09b35f3dcdf1d65652(g_start: Int, g_gene_name: String, g_end: Int, g_contig: String)
case class Record1ae034c3ae194b14b2a38a2c3b4b8503(g_start: Int, g_gene_name: String, genotypes: String, g_end: Int, contig: String, g_contig: String, start: Int)
case class Record57075efce69645b3980259fa919398bd(g_sample: String, call: Double, genotypes_1: String)
case class Record9ffd8a52ab93452ca8408ecfa7c4ed5e(genotypes_LABEL: String, g_start: Int, g_gene_name: String, g_end: Int, contig: String, g_contig: String, start: Int)
case class Recordb7867f8e0637493ab4005cdf7c615b72(g_start: Int, g_gene_name: String, g_sample: String, g_end: Int, contig: String, g_contig: String, start: Int, call: Double)
case class Recordbee52bca00144db0b243f0272327e6f2(sample: String, gene: String, burden: Double)
case class Recordfa226222d7b14df190bbf8b2266d9753(sample: String, gene: String)
case class Recordba7227c81ca84381863cfa17735a0cc5(sample: String)
case class Record4dd82424da7342c79498ea44477582b3(gene: String, burden: Double)
object Record4dd82424da7342c79498ea44477582b3 {
  implicit val ord = Ordering.by(unapply)
}
case class Recordb4f2981f52e84aba9fa2735eec90d37d(sample: String, burdens: Seq[Record4dd82424da7342c79498ea44477582b3])
object ShredGeneBurdenCovariance {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf()
     .setAppName("ShredGeneBurdenCovariance"+sf)
     .set("spark.sql.shuffle.partitions", Config.maxPartitions.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   
   import spark.implicits._
   val vloader = new VariantLoader(spark, "biodata/supersm.vcf")
val (vcf, genotypes) = vloader.shredDS
val IBag_vcf__D = vcf
val IDict_vcf__D_genotypes = genotypes
IBag_vcf__D.cache
IBag_vcf__D.count
IDict_vcf__D_genotypes.cache
IDict_vcf__D_genotypes.count

val gtfLoader = new GeneLoader(spark)
val Gtfs = gtfLoader.loadGTF("biodata/genes.csv")
val IBag_genes__D = Gtfs
IBag_genes__D.cache
IBag_genes__D.count

    def f = {
 
 
var start0 = System.currentTimeMillis()
val x24 = IBag_vcf__D.select("genotypes","contig","start")
 .as[Record3bc4aa8d5c684938b781a97bd584ba21] 
val x26 = IBag_genes__D.select("g_start","g_gene_name","g_end","g_contig")
 .as[Record957525f7fd8e4b09b35f3dcdf1d65652] 
val x29 = x24.join(x26, col("contig") === col("g_contig") && col("start") >= col("g_start") && col("g_end") >= col("start"), "inner")
  .as[Record1ae034c3ae194b14b2a38a2c3b4b8503]
 
val x31 = IDict_vcf__D_genotypes 
val x34 = x29.withColumnRenamed("genotypes", "genotypes_LABEL")
   .as[Record9ffd8a52ab93452ca8408ecfa7c4ed5e].equiJoin(
   x31.withColumnRenamed("_1", "genotypes_1").as[Record57075efce69645b3980259fa919398bd], 
   Seq("genotypes_LABEL", "genotypes_1"), "inner").drop("genotypes_LABEL", "genotypes_1")
   .as[Recordb7867f8e0637493ab4005cdf7c615b72]
 
val x36 = x34
           .withColumnRenamed("g_sample", "sample")
 .withColumnRenamed("g_gene_name", "gene")
 .withColumnRenamed("call", "burden")
.withColumn("burden", when(col("burden").isNull, 0.0).otherwise(col("burden")))
 .as[Recordbee52bca00144db0b243f0272327e6f2]
 
val x38 = x36.groupByKey(x37 => Recordfa226222d7b14df190bbf8b2266d9753(x37.sample, x37.gene))
 .agg(typed.sum[Recordbee52bca00144db0b243f0272327e6f2](x37 => x37.burden)
).mapPartitions{ it => it.map{ case (key, burden) =>
   Recordbee52bca00144db0b243f0272327e6f2(key.sample, key.gene, burden)
}}.as[Recordbee52bca00144db0b243f0272327e6f2]
 
val x39 = x38
val MBag_Burden_1 = x39
//MBag_Burden_1.print
//MBag_Burden_1.cache
//MBag_Burden_1.count
val x41 = MBag_Burden_1 
val x43 = x41.groupByKey(x42 => Recordba7227c81ca84381863cfa17735a0cc5(x42.sample)).mapGroups{
 case (key, value) => 
   val grp = value.flatMap(x42 => 
    () match {
      
      case _ => Seq(Record4dd82424da7342c79498ea44477582b3(x42.gene, x42.burden))
   }).toSeq.sorted
   Recordb4f2981f52e84aba9fa2735eec90d37d(key.sample, grp)
 }.as[Recordb4f2981f52e84aba9fa2735eec90d37d]
 
val x44 = x43
val MBag_Groups_1 = x44
//MBag_Groups_1.print
//MBag_Groups_1.cache
//MBag_Groups_1.count

val rows = MBag_Groups_1.rdd.map(x => Vectors.dense(x.burdens.map(y => y.burden).toArray))
  .zipWithIndex.map{ case (v, i) => IndexedRow(i, v) }
val mat = new IndexedRowMatrix(rows).toBlockMatrix
val trans = mat.transpose
val covariance = trans.multiply(mat)
println(covariance.toLocalMatrix.toString())

var end0 = System.currentTimeMillis() - start0
//println("ShredGeneBurdenCovariance,shred,"+sf+","+end0+",query,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("ShredGeneBurdenCovariance,standard,"+sf+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
