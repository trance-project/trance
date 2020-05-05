package sparkutils.loader

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{IntegerType, StringType, DoubleType, StructField, StructType}
import org.apache.spark.HashPartitioner

case class PartSupp(ps_partkey: Int, ps_suppkey: Int, ps_availqty: Int, ps_supplycost: Double, ps_comment: String)

case class Part(p_partkey: Int, p_name: String, p_mfgr: String, p_brand: String, p_type: String, p_size: Int, p_container: String, p_retailprice: Double, p_comment: String)

case class Customer(c_custkey: Int, c_name: String, c_address: String, c_nationkey: Int, c_phone: String, c_acctbal: Double, c_mktsegment: String, c_comment: String)

case class Order(o_orderkey: Int, o_custkey: Int, o_orderstatus: String, o_totalprice: Double, o_orderdate: String, o_orderpriority: String, o_clerk: String, o_shippriority: Int, o_comment: String)

case class Lineitem(l_orderkey: Int, l_partkey: Int, l_suppkey: Int, l_linenumber: Int, l_quantity: Double, l_extendedprice: Double, l_discount: Double, l_tax: Double, l_returnflag: String, l_linestatus: String, l_shipdate: String, l_commitdate: String, l_receiptdate: String, l_shipinstruct: String, l_shipmode: String, l_comment: String)

case class Supplier(s_suppkey: Int, s_name: String, s_address: String, s_nationkey: Int, s_phone: String, s_acctbal: Double, s_comment: String)

case class Region(r_regionkey: Int, r_name: String, r_comment: String)

case class Nation(n_nationkey: Int, n_name: String, n_regionkey: Int, n_comment: String)


/** Config reader specific to tpch loader **/
object Config {
  val prop = new java.util.Properties
  val fsin = new java.io.FileInputStream("data.flat")
  prop.load(fsin)
  val datapath = prop.getProperty("datapath")
  val master = prop.getProperty("master")
  val minPartitions = prop.getProperty("minPartitions").toInt
  val threshold = prop.getProperty("threshold").toInt
  val goalParts = prop.getProperty("goalParts")
  val lparts = prop.getProperty("lineitem").toInt
}

/** RDD and Dataset loaders for TPCH **/
class TPCHLoader(spark: SparkSession) extends Serializable {

  val datapath = Config.datapath
  val parts = Config.minPartitions
  val goalParts = Config.goalParts
  val lparts = Config.lparts

  def parseBigInt(n: String): Int = {
    val b = BigInt(n).intValue()
    if (b < 0) b*(-1) else b 
  }
  
  def triggerGC = {
	  val partitioner = new HashPartitioner(parts)
	  spark.sparkContext.parallelize((1 until parts), parts).map(x => (x,x)).partitionBy(partitioner).foreach(x => System.gc())
	  System.gc()
  }

  import spark.implicits._

  /** Customer Loaders **/

  def loadCustomer(path: String = s"file:///$datapath/customer.tbl"):RDD[Customer] = {
    spark.sparkContext.textFile(path, minPartitions = parts).mapPartitions(it =>
		it.toVector.map(line => {
        	val l = line.split("\\|")
            Customer(l(0).toInt, l(1), l(2), l(3).toInt, l(4), l(5).toDouble, l(6), l(7))
		}).iterator, true).repartition(parts)
  }

  def loadCustomerDF():Dataset[Customer] = { 
    val schema = StructType(Array(
                      StructField("c_custkey", IntegerType), 
                      StructField("c_name", StringType),
                      StructField("c_address", StringType),
                      StructField("c_nationkey", IntegerType), 
                      StructField("c_phone", StringType), 
                      StructField("c_acctbal", DoubleType),
                      StructField("c_mktsegment", StringType), 
                      StructField("c_comment", StringType)))

    val customers = spark.read.schema(schema)
      .option("delimiter", "|")
      .csv(s"file:///$datapath/customer.tbl")
      .as[Customer]
    customers.repartition(parts)
  }


  /** PartSupp Loaders **/

  def loadPartSupp():RDD[PartSupp] = {
    spark.sparkContext.textFile(s"file:///$datapath/partsupp.tbl", minPartitions = parts).map(line => {
                    val l = line.split("\\|")
                    PartSupp(l(0).toInt, l(1).toInt, l(2).toInt, l(3).toDouble, l(4))})
  }


  /** Part Loaders **/

  def loadPart(path: String = s"file:///$datapath/part.tbl"):RDD[Part] = {
    spark.sparkContext.textFile(path, minPartitions = parts).mapPartitions(it =>
		it.toVector.map(line => {
        	val l = line.split("\\|")
            Part(l(0).toInt, l(1), l(2), l(3), l(4), l(5).toInt, l(6), l(7).toDouble, l(8))
  		}).iterator, true).repartition(parts)
  }

  def loadPartDF():Dataset[Part] = {
    val schema = StructType(Array(
                      StructField("p_partkey", IntegerType), 
                      StructField("p_name", StringType), 
                      StructField("p_mfgr", StringType), 
                      StructField("p_brand", StringType), 
                      StructField("p_type", StringType), 
                      StructField("p_size", IntegerType), 
                      StructField("p_container", StringType), 
                      StructField("p_retailprice", DoubleType), 
                      StructField("p_comment", StringType)))

    val partsdf = spark.read.schema(schema)
      .option("delimiter", "|")
      .csv(s"file:///$datapath/part.tbl")
      .as[Part]
    partsdf.repartition(parts)
  }


  /** Order Loaders **/

  def loadOrder(path: String = s"file:///$datapath/order.tbl"):RDD[Order] = {
    //val ofile = if (datapath.split("/").last.startsWith("sfs")) { "order.tbl" } else { "orders.tbl" }
    spark.sparkContext.textFile(path ,minPartitions = parts).mapPartitions(it => 
		it.toVector.map(line => {
      		val l = line.split("\\|")
            Order(parseBigInt(l(0)), l(1).toInt, l(2), l(3).toDouble, l(4), l(5), l(6), l(7).toInt, l(8))
  		}).iterator, true).repartition(parts)
  }

  def loadOrderDF():Dataset[Order] = {

    val schema = StructType(Array(
                  StructField("o_orderkey", IntegerType), 
                  StructField("o_custkey", IntegerType), 
                  StructField("o_orderstatus", StringType), 
                  StructField("o_totalprice", DoubleType), 
                  StructField("o_orderdate", StringType), 
                  StructField("o_orderpriority", StringType), 
                  StructField("o_clerk", StringType), 
                  StructField("o_shippriority", IntegerType), 
                  StructField("o_comment", StringType)))
   
    val ofile = if (datapath.split("/").last.startsWith("sfs")) { "order.tbl" } else { "order.tbl" }
    val orders = spark.read.schema(schema)
      .option("delimiter", "|")
      .csv(s"file:///$datapath/$ofile")
      .as[Order]
    orders.repartition(parts)
  }


  /** Lineitem Loaders **/

  def loadLineitem():RDD[Lineitem] = {
  	spark.sparkContext.textFile(s"file:///$datapath/lineitem.tbl",minPartitions = lparts).mapPartitions(it => 
		it.toVector.map(line => {
			val l = line.split("\\|")
        	Lineitem(parseBigInt(l(0)), l(1).toInt, l(2).toInt, l(3).toInt, l(4).toDouble, l(5).toDouble, 
				l(6).toDouble, l(7).toDouble, l(8), l(9), l(10), l(11), l(12), l(13), l(14), l(15))
		}).iterator, true).repartition(lparts)
  }

  def loadLineitemDF():Dataset[Lineitem] = {
    val schema = StructType(Array(
                   StructField("l_orderkey", IntegerType), 
                   StructField("l_partkey", IntegerType),
                   StructField("l_suppkey", IntegerType),
                   StructField("l_linenumber", IntegerType), 
                   StructField("l_quantity", DoubleType), 
                   StructField("l_extendedprice", DoubleType), 
                   StructField("l_discount", DoubleType), 
                   StructField("l_tax", DoubleType), 
                   StructField("l_returnflag", StringType), 
                   StructField("l_linestatus", StringType), 
                   StructField("l_shipdate", StringType), 
                   StructField("l_commitdate", StringType), 
                   StructField("l_receiptdate", StringType), 
                   StructField("l_shipinstruct", StringType), 
                   StructField("l_shipmode", StringType), 
                   StructField("l_comment", StringType)))

    val lineitem = spark.read.schema(schema)
      .option("delimiter", "|")
      .csv(s"file:///$datapath/lineitem.tbl")
      .as[Lineitem]
    lineitem.repartition(lparts)
  }


  /** Suppier Loaders **/

  def loadSupplier():RDD[Supplier] = {
    spark.sparkContext.textFile(s"file:///$datapath/supplier.tbl",minPartitions = parts).map(line => {
                    val l = line.split("\\|")
                    Supplier(l(0).toInt, l(1), l(2), l(3).toInt, l(4), l(5).toDouble, l(6))})
  }

  def loadSupplierDF():Dataset[Supplier] = {
    val schema = StructType(Array(
                   StructField("s_suppkey", IntegerType), 
                   StructField("s_name", StringType),
                   StructField("s_address", StringType),
                   StructField("s_nationkey", IntegerType), 
                   StructField("s_phone", StringType), 
                   StructField("s_acctbal", DoubleType), 
                   StructField("s_comment", StringType)))

    val supplier = spark.read.schema(schema)
      .option("delimiter", "|")
      .csv(s"file:///$datapath/supplier.tbl")
      .as[Supplier]
    supplier.repartition(parts)
  }

  /** Region Loaders **/

  def loadRegion():RDD[Region] = {
    spark.sparkContext.textFile(s"file:///$datapath/region.tbl",minPartitions = parts).mapPartitions(it =>
      it.map(line => {
                    val l = line.split("\\|")
                    Region(l(0).toInt, l(1), l(2))}), true).repartition(parts)
  }

  def loadRegionDF():Dataset[Region] = {
    val schema = StructType(Array(
                   StructField("r_regionkey", IntegerType), 
                   StructField("r_name", StringType),
                   StructField("r_comment", StringType)))

    val region = spark.read.schema(schema)
      .option("delimiter", "|")
      .csv(s"file:///$datapath/region.tbl")
      .as[Region]
    region.repartition(parts)
  }


  /** Nation Loaders **/

  def loadNation():RDD[Nation] = {
    spark.sparkContext.textFile(s"file:///$datapath/nation.tbl",minPartitions = parts).mapPartitions(it =>
      it.map(line => {
                    val l = line.split("\\|")
                    Nation(l(0).toInt, l(1), l(2).toInt, l(3))}), true).repartition(parts)
  }

  def loadNationDF():Dataset[Nation] = {
    val schema = StructType(Array(
                   StructField("n_nationkey", IntegerType), 
                   StructField("n_name", StringType),
                   StructField("n_regionkey", IntegerType), 
                   StructField("n_comment", StringType)))

    val nation = spark.read.schema(schema)
      .option("delimiter", "|")
      .csv(s"file:///$datapath/nation.tbl")
      .as[Nation]
    nation.repartition(parts)
  }

}

object TPCHLoader {
  def apply(spark: SparkSession): TPCHLoader = new TPCHLoader(spark)
}
