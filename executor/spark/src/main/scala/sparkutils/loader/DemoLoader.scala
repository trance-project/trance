package sparkutils.loader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

case class SamplesMin(bcr_patient_uuid: String, bcr_aliquot_uuid: String)
case class CopyNumber2(cn_gene_id: String, cn_gene_name: String, cn_chromosome: String, cn_start: Int, cn_end: Int, cn_copy_number: Int, min_copy_number: Int, max_copy_number: Int, cn_aliquot_uuid: String, bcr_aliquot_uuid: String, bcr_patient_uuid: String)


class DemoLoader(spark: SparkSession) extends Serializable {

  import spark.implicits._

  val sloader = new BiospecLoader(spark)
  val cloader = new CopyNumberLoader(spark)

  def loadCopyNum(path: String = "/data/cnv"): Dataset[CopyNumber] = cloader.load(path)

  def loadSamples(path: String = "/data/samples.txt"): Dataset[Biospec] = sloader.load(path)

  def loadCopyAndSamples(cpath: String = "/data/cnv", spath: String = "/data/samples.txt"): (Dataset[CopyNumber], Dataset[Biospec]) = {
    val samples = sloader.load(spath)
    val copy = cloader.load(cpath).join(
      samples.select("bcr_patient_uuid", "bcr_aliquot_uuid").as[SamplesMin], 
      col("bcr_aliquot_uuid") === col("cn_aliquot_uuid")
    ).as[CopyNumber2].withColumn("cn_aliquot_uuid", col("bcr_patient_uuid")).drop("bcr_patient_uuid").as[CopyNumber]
    
    (copy, samples)
  
  }

  def loadOccurrences(path: String = "/data/occurrences"): Dataset[OccurrenceMid] = 
  	spark.read.json(path).as[OccurrenceMid]

  def loadOccurrShred(paths: Seq[String] = Seq("/data/odict1", "/data/odict2", "/data/odict3")): (Dataset[OccurrDict1], Dataset[OccurTransDict2Mid], Dataset[OccurrTransConseqDict3]) = {
  	(spark.read.json(paths(0)).as[OccurrDict1],
		 spark.read.json(paths(1)).as[OccurTransDict2Mid],
		 spark.read.json(paths(2)).as[OccurrTransConseqDict3])
  }

}