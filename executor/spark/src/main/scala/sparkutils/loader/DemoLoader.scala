package sparkutils.loader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

class DemoLoader(spark: SparkSession) extends Serializable {

  import spark.implicits._

  val sloader = new BiospecLoader(spark)
  // val cloader = new CopyNumberLoader(spark)

  // def loadCopyNum(): Dataset[CopyNumber] = cloader.load("/data/cnv")

  def loadSamples(path: String = "/data/samples.txt"): Dataset[Biospec] = sloader.load(path)

  def loadOccurrences(path: String = "/data/occurrences"): Dataset[OccurrenceMid] = 
  	spark.read.json(path).as[OccurrenceMid]

  def loadOccurrShred(paths: Seq[String] = Seq("/data/odict1", "/data/odict2", "/data/odict3")): (Dataset[OccurrDict1], Dataset[OccurTransDict2Mid], Dataset[OccurrTransConseqDict3]) = {
  	(spark.read.json(paths(0)).as[OccurrDict1],
		 spark.read.json(paths(1)).as[OccurTransDict2Mid],
		 spark.read.json(paths(2)).as[OccurrTransConseqDict3])
  }

}