package sparkutils

/** Config that reads data.flat. 
  * Currently reads fields that are specific to tpch
  */
object Config {

  val prop = new java.util.Properties
  val fsin = new java.io.FileInputStream("data.flat")
  prop.load(fsin)

  val datapath = prop.getProperty("datapath")
  val master = prop.getProperty("master")
  val minPartitions = prop.getProperty("minPartitions").toInt
  val threshold = prop.getProperty("threshold").toInt

  // tpch specific
  val goalParts = prop.getProperty("goalParts")
  val lparts = prop.getProperty("lineitem").toInt

}
