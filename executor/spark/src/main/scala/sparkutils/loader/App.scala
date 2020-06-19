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

     val vloader = new VariantLoader(spark, "/nfs/home/jaclyns/sub.vcf")
     val variants = vloader.loadVCF

     val veploader = new VepLoader(spark)
     val annots = veploader.loadOccurences(variants)
     annots.cache
     annots.count 
     println("annotations here")
     println(annots.show())
     
  }

}
