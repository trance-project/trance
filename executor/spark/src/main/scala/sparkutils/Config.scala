package sparkutils

/** Config that reads data.flat. 
  * Currently reads fields that are specific to tpch
  */
object Config {

  val prop = new java.util.Properties
  /**try{  	 
    val fsin = new java.io.FileInputStream("data.flat")
    prop.load(fsin)
  }catch{
    case _:Throwable => println("data.flat not found")	
  }**/
  // assumes at executor/spark
  val basepath = System.getProperty("user.dir")
  val datapath = prop.getProperty("datapath", s"$basepath/data/tpch")

  // deprecated should be set through spark-submit only
  // val master = prop.getProperty("master", "local[*]")
  val minPartitions = prop.getProperty("minPartitions", "400").toInt
  val maxPartitions = prop.getProperty("maxPartitions", "1000").toInt
  val threshold = prop.getProperty("threshold", ".0025").toDouble
  val sample = prop.getProperty("sample", "1000").toInt
  val heavyKeyStrategy = prop.getProperty("heavyKeyStrategy", "sample")

  // vep information
  val vepHome = prop.getProperty("vephome", "/usr/local/ensembl-vep/vep")
  val vepCache = prop.getProperty("vepcache", "/mnt/app_hdd/")

}
