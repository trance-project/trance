package sparkutils

/** Config that reads data.flat. 
  * Currently reads fields that are specific to tpch
  */
object Config {

  val prop = new java.util.Properties
  val fsin = new java.io.FileInputStream("data.flat")
  prop.load(fsin)

  // fix this
  val datapath = prop.getProperty("datapath")
  val master = prop.getProperty("master", "local[*]")
  val minPartitions = prop.getProperty("minPartitions", "400").toInt
  val maxPartitions = prop.getProperty("maxPartitions", "1000").toInt
  val threshold = prop.getProperty("threshold", ".0025").toDouble

  // vep information
  val vepHome = prop.getProperty("vephome", "/usr/local/ensembl-vep/vep")
  val vepCache = prop.getProperty("vepcache", "/mnt/app_hdd/")

}
