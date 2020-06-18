package sparkutils.loader

import sparkutils.Config
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object App {
 
  def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master)
     .setAppName("TestLoader"+sf)
     .set("spark.sql.shuffle.partitions", Config.maxPartitions.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()

   val vloader = new VariantLoader(spark, "/Users/jac/bioqueries/data/sub.vcf")
   val variants = vloader.loadVCF

   val veploader = new VepLoader(spark)
   val annotations = veploader.loadAnnotations(variants)
   annotations.take(10).foreach(println(_))

  }

}