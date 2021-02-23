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

	/**val vepLoader = new VepLoader(spark)
	val mafLoader = new MAFLoader(spark)
	val maf = mafLoader.loadFlat(s"/nfs_qc4/genomics/gdc/somatic/brca/TCGA.BRCA.mutect.995c0111-d90b-4140-bee7-3845436c3b42.DR-10.0.somatic.maf")
	val (muts, annots) = vepLoader.normalizeAnnots(maf)

	val occurrences = spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/datasetBrca/").as[OccurrenceMid]
	val (odict1, odict2, odict3) = vepLoader.shredMid(occurrences)
	odict1.cache
	odict1.count
	odict2.cache
	odict2.count
	odict3.cache
	odict3.count
	odict1.write.json("file:///nfs_qc4/genomics/gdc/somatic/odictBrca1/")
	odict2.write.json("file:///nfs_qc4/genomics/gdc/somatic/odictBrca2/")
	odict3.write.json("file:///nfs_qc4/genomics/gdc/somatic/odictBrca3/")

	muts.take(10).foreach(println(_))
	println("the annotations")
	annots.take(10).foreach(println(_))
	println(annots.count)**/

	// TESTING AREA FOR LOADERS
  //val gtfLoader = new GTFLoader(spark, "/Users/jac/bioqueries/data/Homo_sapiens.GRCh37.87.chr.gtf")
  //val gtfs = gtfLoader.loadDS
  //gtfs.coalesce(1).write.csv("/Users/jac/bioqueries/data/genes.csv")
       
  val vcfLoader = new VariantLoader(spark, "/Users/jac/bioqueries/data/supersm.vcf")
  val vcfs = vcfLoader.loadDS
  vcfs.coalesce(1).write.json("/Users/jac/bioqueries/data/vcf.json")
  
  }

}
