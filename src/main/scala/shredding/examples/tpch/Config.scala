package shredding.examples.tpch

object Config {
  val prop = new java.util.Properties
  val fsin = new java.io.FileInputStream("data.flat")
  prop.load(fsin)
  val datapath = prop.getProperty("datapath")
}
