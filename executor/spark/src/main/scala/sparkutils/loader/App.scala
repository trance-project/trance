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

     import spark.implicits._
     //val vloader = new VariantLoader(spark, "/nfs_qc4/genomics/sub.vcf")
     //val variants = vloader.loadVCF
     //variants.take(10).foreach(println(_))
	 //val annots = veploader.loadAnnotations(variants)
     //annots.cache
     //annots.count 
     //println("annotations here")
     //println(annots.show())
	 //annots.take(10).foreach(println(_))
	 val veploader = new VepLoader(spark)
	 val mloader = new MAFLoader(spark)
	 //val fmaf = "TCGA.BRCA.mutect.995c0111-d90b-4140-bee7-3845436c3b42.DR-10.0.somatic.maf"
	 val fmaf = ""
	 //val fmaf = "small.maf"
	 val maf = mloader.loadFlat(s"/nfs_qc4/genomics/gdc/somatic/mafs/$fmaf")

	 val occurrences = veploader.loadOccurrencesMid(maf)
	 //val (vind, annots) = veploader.loadOccurrencesMid(maf)
	 //println(occurrences.printSchema)
	 //occurrences.take(10).foreach(println(_))
	 occurrences.write.format("json").save("file:///nfs_qc4/genomics/gdc/somatic/datasetFull/")
	 //one.write.format("json").save("file://nfs_qc4/genomics/gdc/somatic/vindexed/")
	 //annotations.take(10).foreach(println(_))
	 /**
	 //val occurences = spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/dataset/").as[VepAnnotTrunc]
	 //occurrences.take(10).foreach(println(_))
	 //annotations.take(10).foreach(println(_))
	 val joined = veploader.buildOccurrences(occurrences, annotations)
	 //joined.take(100).foreach(println(_))
	 val total = joined.count
	 println(s"writing out this many somatic mutations $total")
	 joined.write.format("json").save("file:///nfs_qc4/genomics/gdc/somatic/dataset/")
	 val occurrences = veploader.buildOccurrences(maf, "file:///nfs_qc4/genomics/gdc/somatic/dataset/")
	 val stats = occurrences.map(o => o.transcript_consequences match {
		case Some(ts) => (ts.size, ts.map(t => t.consequence_terms.size))
		case None => (0, Nil)
	 })
	 stats.collect.foreach(println(_))**/
   //       val gloader = new GisticLoader(spark)
   //       val gistic = gloader.merge("/nfs_qc4/genomics/gdc/gistic/")
   //       gistic.count
		 // gistic.write.format("json").save("file:///nfs_qc4/genomics/gdc/gistic/dataset")
	/**val bloader = new BiospecLoader(spark)
	val biospec = bloader.load("/nfs_qc4/genomics/gdc/biospecimen/")
	println(biospec.show())
       val cloader = new ConsequenceLoader(spark)
       val thresh = cloader.loadSequential("/nfs_qc4/genomics/calc_variant_conseq.txt")
       thresh.collect.foreach(println(_))**/
    // val nloader = new NetworkLoader(spark)
    // val network = nloader.load("/home/jacith/shredder/executor/spark/data/geno/9606.protein.links.full.v11.0.txt")
    // println(network.show())
    //val bloader = new BiomartLoader(spark)
    //val biomart = bloader.load("/home/jacith/shredder/executor/spark/data/geno/mart_export.txt")
    //println(biomart.show())
	//val cloader = new CopyNumberLoader(spark)
	//val copynum = cloader.load("/nfs_qc4/genomics/gdc/gene_level/", true)
	//copynum.take(10).foreach(println(_))

    }

}
