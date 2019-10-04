package sprkloader

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{IntegerType, StringType, DoubleType, StructField, StructType}

case class PartSupp(ps_partkey: Int, ps_suppkey: Int, ps_availqty: Int, ps_supplycost: Double, ps_comment: String, uniqueId: Long) extends CaseClassRecord

case class Part2(p_partkey: Int, p_name: String, p_mfgr: String, p_brand: String, p_type: String, p_size: Int, p_container: String, p_retailprice: Double, p_comment: String)

case class Part(p_partkey: Int, p_name: String, p_mfgr: String, p_brand: String, p_type: String, p_size: Int, p_container: String, p_retailprice: Double, p_comment: String, uniqueId: Long) extends CaseClassRecord

case class Customer2(c_custkey: Int, c_name: String, c_address: String, c_nationkey: Int, c_phone: String, c_acctbal: Double, c_mktsegment: String, c_comment: String)

case class Customer(c_custkey: Int, c_name: String, c_address: String, c_nationkey: Int, c_phone: String, c_acctbal: Double, c_mktsegment: String, c_comment: String, uniqueId: Long) extends CaseClassRecord

case class Orders2(o_orderkey: Int, o_custkey: Int, o_orderstatus: String, o_totalprice: Double, o_orderdate: String, o_orderpriority: String, o_clerk: String, o_shippriority: Int, o_comment: String)

case class Orders(o_orderkey: Int, o_custkey: Int, o_orderstatus: String, o_totalprice: Double, o_orderdate: String, o_orderpriority: String, o_clerk: String, o_shippriority: Int, o_comment: String, uniqueId: Long) extends CaseClassRecord

case class Lineitem2(l_orderkey: Int, l_partkey: Int, l_suppkey: Int, l_linenumber: Int, l_quantity: Double, l_extendedprice: Double, l_discount: Double, l_tax: Double, l_returnflag: String, l_linestatus: String, l_shipdate: String, l_commitdate: String, l_receiptdate: String, l_shipinstruct: String, l_shipmode: String, l_comment: String)

case class Lineitem(l_orderkey: Int, l_partkey: Int, l_suppkey: Int, l_linenumber: Int, l_quantity: Double, l_extendedprice: Double, l_discount: Double, l_tax: Double, l_returnflag: String, l_linestatus: String, l_shipdate: String, l_commitdate: String, l_receiptdate: String, l_shipinstruct: String, l_shipmode: String, l_comment: String, uniqueId: Long) extends CaseClassRecord

case class Supplier(s_suppkey: Int, s_name: String, s_address: String, s_nationkey: Int, s_phone: String, s_acctbal: Double, s_comment: String, uniqueId: Long) extends CaseClassRecord

case class Region(r_regionkey: Int, r_name: String, r_comment: String, uniqueId: Long) extends CaseClassRecord

case class Nation(n_nationkey: Int, n_name: String, n_regionkey: Int, n_comment: String, uniqueId: Long) extends CaseClassRecord

case class Q1Flat(P__F: Int, C__F: Int, L__F: Int, O__F: Int, uniqueId: Long) extends CaseClassRecord
case class Q1Flat2(Query4__F: Q1Flat, uniqueId: Long) extends CaseClassRecord
case class Q3Flat(O__F: Int, C__F: Int, PS__F: Int, S__F: Int, L__F: Int, P__F: Int, uniqueId: Long) extends CaseClassRecord
case class Q3Flat2(N__F: Int, Query7__F: Q3Flat, uniqueId: Long) extends CaseClassRecord
case class Q5Flat(Query5__F: Q3Flat, uniqueId: Long) extends CaseClassRecord

object Config {
  val prop = new java.util.Properties
  val fsin = new java.io.FileInputStream("data.flat")
  prop.load(fsin)
  val datapath = prop.getProperty("datapath")
  val master = prop.getProperty("master")
  val minPartitions = prop.getProperty("minPartitions").toInt
}

class TPCHLoader(spark: SparkSession) extends Serializable {

  val datapath = Config.datapath
  val parts = Config.minPartitions

  def parseBigInt(n: String): Int = {
    val b = BigInt(n).intValue()
    if (b < 0) b*(-1) else b 
  }
  
  import spark.implicits._

  def loadCustomers():RDD[Customer] = {
    spark.sparkContext.textFile(s"file:///$datapath/customer.tbl", minPartitions = parts).map(line => {
                    val l = line.split("\\|")
                    Customer(l(0).toInt, l(1), l(2), l(3).toInt, l(4), l(5).toDouble, l(6), l(7), l(0).toLong)})
  }

  def loadCustomersDF():Dataset[Customer2] = { 
    val schema = StructType(Array(
                      StructField("c_custkey", IntegerType), 
                      StructField("c_name", StringType),
                      StructField("c_address", StringType),
                      StructField("c_nationkey", IntegerType), 
                      StructField("c_phone", StringType), 
                      StructField("c_acctbal", DoubleType),
                      StructField("c_mktsegment", StringType), 
                      StructField("c_comment", StringType)))

    spark.read.schema(schema)
      .option("delimiter", "|")
      .csv(s"file:///$datapath/customer.tbl")
      .as[Customer2]
  }

  def loadShredCustomers(flat: Int):RDD[(Int, Customer)] = {
    spark.sparkContext.textFile(s"file:///$datapath/customer.tbl").map(line => {
                    val l = line.split("\\|")
                    (flat, Customer(l(0).toInt, l(1), l(2), l(3).toInt, l(4), l(5).toDouble, l(6), l(7), l(0).toLong))})
  }
 
  def loadPartSupp():RDD[PartSupp] = {
    spark.sparkContext.textFile(s"file:///$datapath/partsupp.tbl", minPartitions = parts).map(line => {
                    val l = line.split("\\|")
                    PartSupp(l(0).toInt, l(1).toInt, l(2).toInt, l(3).toDouble, l(4), l(0).toLong)})
  }

  def loadShredPartSupp(flat: Int):RDD[(Int, PartSupp)] = {
    spark.sparkContext.textFile(s"file:///$datapath/partsupp.tbl").map(line => {
                    val l = line.split("\\|")
                    (flat, PartSupp(l(0).toInt, l(1).toInt, l(2).toInt, l(3).toDouble, l(4), l(0).toLong))})
  }

  def loadPart():RDD[Part] = {
    spark.sparkContext.textFile(s"file:///$datapath/part.tbl", minPartitions = parts).map(line => {
                    val l = line.split("\\|")
                    Part(l(0).toInt, l(1), l(2), l(3), l(4), l(5).toInt, l(6), l(7).toDouble, l(8), l(0).toLong)})
  }

  def loadPartDF():Dataset[Part2] = {
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

    spark.read.schema(schema)
      .option("delimiter", "|")
      .csv(s"file:///$datapath/part.tbl")
      .as[Part2]
  }

  def loadShredPart(flat: Int):RDD[(Int, Part)] = {
    spark.sparkContext.textFile(s"file:///$datapath/part.tbl").map(line => {
                    val l = line.split("\\|")
                    (flat, Part(l(0).toInt, l(1), l(2), l(3), l(4), l(5).toInt, l(6), l(7).toDouble, l(8), l(0).toLong))})
  }


  def loadOrders():RDD[Orders] = {
    val ofile = if (datapath.split("/").last.startsWith("sfs")) { "order.tbl" } else { "orders.tbl" }
    spark.sparkContext.textFile(s"file:///$datapath/$ofile",minPartitions = parts).map(line => {
                    val l = line.split("\\|")
                    Orders(parseBigInt(l(0)), l(1).toInt, l(2), l(3).toDouble, l(4), l(5), l(6), l(7).toInt, l(8), l(0).toLong)})
  }


  def loadOrdersDF():Dataset[Orders2] = {

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
   
    val ofile = if (datapath.split("/").last.startsWith("sfs")) { "order.tbl" } else { "orders.tbl" }
    spark.read.schema(schema)
      .option("delimiter", "|")
      .csv(s"file:///$datapath/$ofile")
      .as[Orders2]
  }

  def loadShredOrders(flat: Int):RDD[(Int, Orders)] = {
    val ofile = if (datapath.split("/").last.startsWith("sfs")) { "order.tbl" } else { "orders.tbl" }
    spark.sparkContext.textFile(s"file:///$datapath/$ofile").map(line => {
                    val l = line.split("\\|")
                    (flat, Orders(l(0).toInt, l(1).toInt, l(2), l(3).toDouble, l(4), l(5), l(6), l(7).toInt, l(8), l(0).toLong))})
  }

  def loadLineitem():RDD[Lineitem] = {
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }

    spark.sparkContext.textFile(s"file:///$datapath/lineitem.tbl",minPartitions = parts).map(line => {
                    val l = line.split("\\|")
                    Lineitem(parseBigInt(l(0)), l(1).toInt, l(2).toInt, l(3).toInt, l(4).toDouble, l(5).toDouble, l(6).toDouble, 
                      l(7).toDouble, l(8), l(9), l(10), l(11), l(12), l(13), l(14), l(15), newId)})
  }
  
  def loadLineitemDF():Dataset[Lineitem2] = {
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

    spark.read.schema(schema)
      .option("delimiter", "|")
      .csv(s"file:///$datapath/lineitem.tbl")
      .as[Lineitem2]

  }



  def loadShredLineitem(flat: Int):RDD[(Int, Lineitem)] = {
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }

    spark.sparkContext.textFile(s"file:///$datapath/lineitem.tbl").map(line => {
                    val l = line.split("\\|")
                    (flat, Lineitem(l(0).toInt, l(1).toInt, l(2).toInt, l(3).toInt, l(4).toDouble, l(5).toDouble, l(6).toDouble, 
                      l(7).toDouble, l(8), l(9), l(10), l(11), l(12), l(13), l(14), l(15), newId))})
  }

  def loadSupplier():RDD[Supplier] = {
    spark.sparkContext.textFile(s"file:///$datapath/supplier.tbl",minPartitions = parts).map(line => {
                    val l = line.split("\\|")
                    Supplier(l(0).toInt, l(1), l(2), l(3).toInt, l(4), l(5).toDouble, l(6), l(0).toLong)})
  }

  def loadShredSupplier(flat: Int):RDD[(Int, Supplier)] = {
    spark.sparkContext.textFile(s"file:///$datapath/supplier.tbl").map(line => {
                    val l = line.split("\\|")
                    (flat, Supplier(l(0).toInt, l(1), l(2), l(3).toInt, l(4), l(5).toDouble, l(6), l(0).toLong))})
  }

  def loadRegion():RDD[Region] = {
    spark.sparkContext.textFile(s"file:///$datapath/region.tbl",minPartitions = parts).map(line => {
                    val l = line.split("\\|")
                    Region(l(0).toInt, l(1), l(2), l(0).toLong)})
  }

  def loadShredRegion(flat: Int):RDD[(Int, Region)] = {
    spark.sparkContext.textFile(s"file:///$datapath/region.tbl").map(line => {
                    val l = line.split("\\|")
                    (flat, Region(l(0).toInt, l(1), l(2), l(0).toLong))})
  }

  def loadNation():RDD[Nation] = {
    spark.sparkContext.textFile(s"file:///$datapath/nation.tbl",minPartitions = parts).map(line => {
                    val l = line.split("\\|")
                    Nation(l(0).toInt, l(1), l(2).toInt, l(3), l(0).toLong)})
  }

  def loadShredNation(flat: Int):RDD[(Int, Nation)] = {
    spark.sparkContext.textFile(s"file:///$datapath/nation.tbl").map(line => {
                    val l = line.split("\\|")
                    (flat, Nation(l(0).toInt, l(1), l(2).toInt, l(3), l(0).toLong))})
  }

}

object TPCHLoader {
  def apply(spark: SparkSession): TPCHLoader = new TPCHLoader(spark)
}
