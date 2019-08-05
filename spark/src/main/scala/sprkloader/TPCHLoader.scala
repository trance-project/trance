package sprkloader

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

case class PartSupp(ps_partkey: Int, ps_suppkey: Int, ps_availqty: Int, ps_supplycost: Double, ps_comment: String, uniqueId: Long)

case class Part(p_partkey: Int, p_name: String, p_mfgr: String, p_brand: String, p_type: String, p_size: Int, p_container: String, p_retailprice: Double, p_comment: String, uniqueId: Long)

case class Customer(c_custkey: Int, c_name: String, c_address: String, c_nationkey: Int, c_phone: String, c_acctbal: Double, c_mktsegment: String, c_comment: String, uniqueId: Long)

case class Orders(o_orderkey: Int, o_custkey: Int, o_orderstatus: String, o_totalprice: Double, o_orderdate: String, o_orderpriority: String, o_clerk: String, o_shippriority: Int, o_comment: String, uniqueId: Long)

case class Lineitem(l_orderkey: Int, l_partkey: Int, l_suppkey: Int, l_linenumber: Int, l_quantity: Double, l_extendedprice: Double, l_discount: Double, l_tax: Double, l_returnflag: String, l_linestatus: String, l_shipdate: String, l_commitdate: String, l_receiptdate: String, l_shipinstruct: String, l_shipmode: String, l_comment: String, uniqueId: Long)

case class Supplier(s_suppkey: Int, s_name: String, s_address: String, s_nationkey: Int, s_phone: String, s_acctbal: Double, s_comment: String, uniqueId: Long)

case class Region(r_regionkey: Int, r_name: String, r_comment: String, uniqueId: Long)

case class Nation(n_nationkey: Int, n_name: String, n_regionkey: Int, n_comment: String, uniqueId: Long) 

case class Q1Flat(P__F: Int, C__F: Int, L__F: Int, O__F: Int, uniqueId: Long)
case class Q1Flat2(Query4__F: Q1Flat, uniqueId: Long)
case class Q3Flat(O__F: Int, C__F: Int, PS__F: Int, S__F: Int, L__F: Int, P__F: Int, uniqueId: Long)
case class Q3Flat2(N__F: Int, Query7__F: Q3Flat, uniqueId: Long)
case class Q5Flat(Query5__F: Q3Flat, uniqueId: Long)

object Config {
  val prop = new java.util.Properties
  val fsin = new java.io.FileInputStream("data.flat")
  prop.load(fsin)
  val datapath = prop.getProperty("datapath")
  val master = prop.getProperty("master")
}

class TPCHLoader(spark: SparkSession) extends Serializable {

  val datapath = Config.datapath


  def loadCustomers():RDD[Customer] = {
    spark.sparkContext.textFile(s"file:///$datapath/customer.tbl").map(line => {
                    val l = line.split("\\|")
                    Customer(l(0).toInt, l(1), l(2), l(3).toInt, l(4), l(5).toDouble, l(6), l(7), l(0).toLong)})
  }
 
  def loadShredCustomers(flat: Int):RDD[(Int, Customer)] = {
    spark.sparkContext.textFile(s"file:///$datapath/customer.tbl").map(line => {
                    val l = line.split("\\|")
                    (flat, Customer(l(0).toInt, l(1), l(2), l(3).toInt, l(4), l(5).toDouble, l(6), l(7), l(0).toLong))})
  }
 
  def loadPartSupp():RDD[PartSupp] = {
    spark.sparkContext.textFile(s"file:///$datapath/partsupp.tbl").map(line => {
                    val l = line.split("\\|")
                    PartSupp(l(0).toInt, l(1).toInt, l(2).toInt, l(3).toDouble, l(4), l(0).toLong)})
  }

  def loadShredPartSupp(flat: Int):RDD[(Int, PartSupp)] = {
    spark.sparkContext.textFile(s"file:///$datapath/partsupp.tbl").map(line => {
                    val l = line.split("\\|")
                    (flat, PartSupp(l(0).toInt, l(1).toInt, l(2).toInt, l(3).toDouble, l(4), l(0).toLong))})
  }

  def loadPart():RDD[Part] = {
    spark.sparkContext.textFile(s"file:///$datapath/part.tbl").map(line => {
                    val l = line.split("\\|")
                    Part(l(0).toInt, l(1), l(2), l(3), l(4), l(5).toInt, l(6), l(7).toDouble, l(8), l(0).toLong)})
  }

  def loadShredPart(flat: Int):RDD[(Int, Part)] = {
    spark.sparkContext.textFile(s"file:///$datapath/part.tbl").map(line => {
                    val l = line.split("\\|")
                    (flat, Part(l(0).toInt, l(1), l(2), l(3), l(4), l(5).toInt, l(6), l(7).toDouble, l(8), l(0).toLong))})
  }

  def loadOrders():RDD[Orders] = {
    spark.sparkContext.textFile(s"file:///$datapath/order.tbl").map(line => {
                    val l = line.split("\\|")
                    Orders(l(0).toInt, l(1).toInt, l(2), l(3).toDouble, l(4), l(5), l(6), l(7).toInt, l(8), l(0).toLong)})
  }

  def loadShredOrders(flat: Int):RDD[(Int, Orders)] = {
    spark.sparkContext.textFile(s"file:///$datapath/order.tbl").map(line => {
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

    spark.sparkContext.textFile(s"file:///$datapath/lineitem.tbl").map(line => {
                    val l = line.split("\\|")
                    Lineitem(l(0).toInt, l(1).toInt, l(2).toInt, l(3).toInt, l(4).toDouble, l(5).toDouble, l(6).toDouble, 
                      l(7).toDouble, l(8), l(9), l(10), l(11), l(12), l(13), l(14), l(15), newId)})
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
    spark.sparkContext.textFile(s"file:///$datapath/supplier.tbl").map(line => {
                    val l = line.split("\\|")
                    Supplier(l(0).toInt, l(1), l(2), l(3).toInt, l(4), l(5).toDouble, l(6), l(0).toLong)})
  }

  def loadShredSupplier(flat: Int):RDD[(Int, Supplier)] = {
    spark.sparkContext.textFile(s"file:///$datapath/supplier.tbl").map(line => {
                    val l = line.split("\\|")
                    (flat, Supplier(l(0).toInt, l(1), l(2), l(3).toInt, l(4), l(5).toDouble, l(6), l(0).toLong))})
  }

  def loadRegion():RDD[Region] = {
    spark.sparkContext.textFile(s"file:///$datapath/region.tbl").map(line => {
                    val l = line.split("\\|")
                    Region(l(0).toInt, l(1), l(2), l(0).toLong)})
  }

  def loadShredRegion(flat: Int):RDD[(Int, Region)] = {
    spark.sparkContext.textFile(s"file:///$datapath/region.tbl").map(line => {
                    val l = line.split("\\|")
                    (flat, Region(l(0).toInt, l(1), l(2), l(0).toLong))})
  }

  def loadNation():RDD[Nation] = {
    spark.sparkContext.textFile(s"file:///$datapath/nation.tbl").map(line => {
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
