package sprkloader

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{IntegerType, StringType, DoubleType, StructField, StructType}
import org.apache.spark.HashPartitioner

case class PartSupp(ps_partkey: Int, ps_suppkey: Int, ps_availqty: Int, ps_supplycost: Double, ps_comment: String)

case class Part(p_partkey: Int, p_name: String, p_mfgr: String, p_brand: String, p_type: String, p_size: Int, p_container: String, p_retailprice: Double, p_comment: String)

case class PartProj(p_partkey: Int, p_name: String)
case class PartProj4(p_partkey: Int, p_name: String, p_retailprice: Double)

case class Customer(c_custkey: Int, c_name: String, c_address: String, c_nationkey: Int, c_phone: String, c_acctbal: Double, c_mktsegment: String, c_comment: String)

case class CustomerProj(c_custkey: Int, c_name: String)

case class CustomerProj5(c_custkey: Int, c_name: String, c_nationkey: Int)

case class Order(o_orderkey: Int, o_custkey: Int, o_orderstatus: String, o_totalprice: Double, o_orderdate: String, o_orderpriority: String, o_clerk: String, o_shippriority: Int, o_comment: String)

case class OrdersProj(o_orderkey: Int, o_custkey: Int, o_orderdate: String)

case class OrdersProj4(o_orderkey: Int, o_custkey: Int, o_orderdate: String, o_priority: String)

case class Lineitem(l_orderkey: Int, l_partkey: Int, l_suppkey: Int, l_linenumber: Int, l_quantity: Double, l_extendedprice: Double, l_discount: Double, l_tax: Double, l_returnflag: String, l_linestatus: String, l_shipdate: String, l_commitdate: String, l_receiptdate: String, l_shipinstruct: String, l_shipmode: String, l_comment: String)

case class LineitemProj(l_orderkey: Int, l_partkey: Int, l_quantity: Double) 

case class LineitemProj5(l_orderkey: Int, l_suppkey: Int)

object Lineitem{
  	val LineRegex = "Lineitem\\((\\d+),(\\d+),(\\d+),(\\d+),(\\d+\\.\\d+),(\\d+\\.\\d+),(\\d+\\.\\d+),(\\d+\\.\\d+),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*)\\)".r
	def unapply(str: String): Lineitem = { 
		str match { 
		  case LineRegex(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p) => 
			Lineitem(a.toInt, b.toInt, c.toInt, d.toInt, e.toDouble, f.toDouble, 
				g.toDouble, h.toDouble, i, j, k, l, m, n, o, p)
			case _ => ???
		}
	}
}

case class Supplier(s_suppkey: Int, s_name: String, s_address: String, s_nationkey: Int, s_phone: String, s_acctbal: Double, s_comment: String)

case class SupplierProj(s_suppkey: Int, s_name: String, s_nationkey: Int)

case class Region(r_regionkey: Int, r_name: String, r_comment: String)

case class Nation(n_nationkey: Int, n_name: String, n_regionkey: Int, n_comment: String)

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
  val threshold = prop.getProperty("threshold").toInt
  val goalParts = prop.getProperty("goalParts")
  val lparts = prop.getProperty("lineitem").toInt
}

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

  def loadCustomer(path: String = s"file:///$datapath/customer.tbl"):RDD[Customer] = {
    spark.sparkContext.textFile(path, minPartitions = parts).mapPartitions(it =>
		it.toVector.map(line => {
        	val l = line.split("\\|")
            Customer(l(0).toInt, l(1), l(2), l(3).toInt, l(4), l(5).toDouble, l(6), l(7))
		}).iterator, true).repartition(parts)
  }

  def loadCustomersProj(path: String = s"file:///$datapath/customer.tbl"):RDD[CustomerProj] = {
    spark.sparkContext.textFile(path, minPartitions = parts).mapPartitions(it =>
		it.map(line => {
        	val l = line.split("\\|")
            CustomerProj(l(0).toInt, l(1))
		}), true).repartition(parts)
  }

  def loadCustomersProj5(path: String = s"file:///$datapath/customer.tbl"):RDD[CustomerProj5] = {
    spark.sparkContext.textFile(path, minPartitions = parts).mapPartitions(it =>
		it.map(line => {
        	val l = line.split("\\|")
            CustomerProj5(l(0).toInt, l(1), l(3).toInt)
		}), true).repartition(parts)
  }

  def loadCustomersDF():Dataset[Customer] = { 
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
      .as[Customer]
  }

  def loadPartSupp():RDD[PartSupp] = {
    spark.sparkContext.textFile(s"file:///$datapath/partsupp.tbl", minPartitions = parts).map(line => {
                    val l = line.split("\\|")
                    PartSupp(l(0).toInt, l(1).toInt, l(2).toInt, l(3).toDouble, l(4))})
  }

  def loadPart(path: String = s"file:///$datapath/part.tbl"):RDD[Part] = {
    spark.sparkContext.textFile(path, minPartitions = parts).mapPartitions(it =>
		it.toVector.map(line => {
        	val l = line.split("\\|")
            Part(l(0).toInt, l(1), l(2), l(3), l(4), l(5).toInt, l(6), l(7).toDouble, l(8))
  		}).iterator, true).repartition(parts)
  }

  def loadPartProj(path: String = s"file:///$datapath/part.tbl"):RDD[PartProj] = {
    spark.sparkContext.textFile(path, minPartitions = parts).mapPartitions(it =>
		it.map(line => {
        	val l = line.split("\\|")
            PartProj(l(0).toInt, l(1))
  		}), true).repartition(parts)
  }

  def loadPartProj4(path: String = s"file:///$datapath/part.tbl"):RDD[PartProj4] = {
    spark.sparkContext.textFile(path, minPartitions = parts).mapPartitions(it =>
		it.map(line => {
        	val l = line.split("\\|")
            PartProj4(l(0).toInt, l(1), l(7).toDouble)
  		}), true).repartition(parts)
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

    spark.read.schema(schema)
      .option("delimiter", "|")
      .csv(s"file:///$datapath/part.tbl")
      .as[Part]
  }

  def loadOrder(path: String = s"file:///$datapath/order.tbl"):RDD[Order] = {
    //val ofile = if (datapath.split("/").last.startsWith("sfs")) { "order.tbl" } else { "orders.tbl" }
    spark.sparkContext.textFile(path ,minPartitions = parts).mapPartitions(it => 
		it.toVector.map(line => {
      		val l = line.split("\\|")
            Order(parseBigInt(l(0)), l(1).toInt, l(2), l(3).toDouble, l(4), l(5), l(6), l(7).toInt, l(8))
  		}).iterator, true).repartition(parts)
  }

  def loadOrdersProj(path: String = s"file:///$datapath/order.tbl"):RDD[OrdersProj] = {
    //val ofile = if (datapath.split("/").last.startsWith("sfs")) { "order.tbl" } else { "orders.tbl" }
    spark.sparkContext.textFile(path, minPartitions = parts).mapPartitions(it => 
		it.map(line => {
      		val l = line.split("\\|")
            OrdersProj(parseBigInt(l(0)), l(1).toInt, l(4))
  		}), true).repartition(parts)
  }

  def loadOrdersProj4(path: String = s"file:///$datapath/order.tbl"):RDD[OrdersProj4] = {
    //val ofile = if (datapath.split("/").last.startsWith("sfs")) { "order.tbl" } else { "orders.tbl" }
    spark.sparkContext.textFile(path, minPartitions = parts).mapPartitions(it => 
		it.map(line => {
      		val l = line.split("\\|")
            OrdersProj4(parseBigInt(l(0)), l(1).toInt, l(4), l(5))
  		}), true).repartition(parts)
  }

  def loadOrdersDF():Dataset[Order] = {

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
    spark.read.schema(schema)
      .option("delimiter", "|")
      .csv(s"file:///$datapath/$ofile")
      .as[Order]
  }

  // this is hard coded directory of files split with bash
  // split --line-bytes=797780948 --filter='gzip > $FILE.gz' /path/to/input /path/to/output
  def loadLineitem():RDD[Lineitem] = {
  	spark.sparkContext.textFile(s"file:///$datapath/lineitem.tbl",minPartitions = lparts).mapPartitions(it => 
		it.toVector.map(line => {
			val l = line.split("\\|")
        	Lineitem(parseBigInt(l(0)), l(1).toInt, l(2).toInt, l(3).toInt, l(4).toDouble, l(5).toDouble, 
				l(6).toDouble, l(7).toDouble, l(8), l(9), l(10), l(11), l(12), l(13), l(14), l(15))
		}).iterator, true).repartition(lparts)
  }
 
  def loadLineitemProj(path: String = s"file:///$datapath/lineitem.tbl"):RDD[LineitemProj] = {
	spark.sparkContext.textFile(path, minPartitions = lparts).mapPartitions(it => 
		it.map(line => {
			val l = line.split("\\|")
        	LineitemProj(parseBigInt(l(0)), l(1).toInt, l(4).toDouble)
		}), true).repartition(lparts)
  }

  def loadLineitemProj5(path: String = s"file:///$datapath/lineitem.tbl"):RDD[LineitemProj5] = {
	spark.sparkContext.textFile(path, minPartitions = lparts).mapPartitions(it => 
		it.map(line => {
			val l = line.split("\\|")
        	LineitemProj5(parseBigInt(l(0)), l(2).toInt)
		}), true).repartition(lparts)
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

    spark.read.schema(schema)
      .option("delimiter", "|")
      .csv(s"file:///$datapath/lineitem.tbl")
      .as[Lineitem]

  }

  def loadSupplier():RDD[Supplier] = {
    spark.sparkContext.textFile(s"file:///$datapath/supplier.tbl",minPartitions = parts).map(line => {
                    val l = line.split("\\|")
                    Supplier(l(0).toInt, l(1), l(2), l(3).toInt, l(4), l(5).toDouble, l(6))})
  }

  def loadSupplierProj():RDD[SupplierProj] = {
    spark.sparkContext.textFile(s"file:///$datapath/supplier.tbl",minPartitions = parts).map(line => {
                    val l = line.split("\\|")
                    SupplierProj(l(0).toInt, l(1), l(3).toInt)})
  }

  def loadRegion():RDD[Region] = {
    spark.sparkContext.textFile(s"file:///$datapath/region.tbl",minPartitions = parts).mapPartitions(it =>
      it.map(line => {
                    val l = line.split("\\|")
                    Region(l(0).toInt, l(1), l(2))}), true).repartition(parts)
  }

  def loadNation():RDD[Nation] = {
    spark.sparkContext.textFile(s"file:///$datapath/nation.tbl",minPartitions = parts).mapPartitions(it =>
      it.map(line => {
                    val l = line.split("\\|")
                    Nation(l(0).toInt, l(1), l(2).toInt, l(3))}), true).repartition(parts)
  }

}

object TPCHLoader {
  def apply(spark: SparkSession): TPCHLoader = new TPCHLoader(spark)
}
