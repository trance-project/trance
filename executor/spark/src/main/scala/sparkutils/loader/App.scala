package sparkutils.loader

import sparkutils.Config
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object App {
 
  def main(args: Array[String]){
     val sf = Config.datapath.split("/").last
     val conf = new SparkConf()
       .setAppName("TestLoader"+sf)
       .set("spark.sql.shuffle.partitions", Config.maxPartitions.toString)
     val spark = SparkSession.builder().config(conf).getOrCreate()

     import spark.implicits._

	val vepLoader = new VepLoader(spark)
	val mafLoader = new MAFLoader(spark)
	val maf = mafLoader.loadFlat(s"/nfs_qc4/genomics/gdc/somatic/brca/TCGA.BRCA.mutect.995c0111-d90b-4140-bee7-3845436c3b42.DR-10.0.somatic.maf")
	val (muts, annots) = vepLoader.normalizeAnnots(maf)

	muts.take(10).foreach(println(_))
	println("the annotations")
	annots.take(10).foreach(println(_))
	println(annots.count)

	// TESTING AREA FOR LOADERS
       
  }

}
