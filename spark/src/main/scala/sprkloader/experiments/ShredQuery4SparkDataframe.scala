
package sprkloader.experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.HashMap
import sprkloader._
import sprkloader.PairRDDOperations._
import sprkloader.DictRDDOperations._
import sprkloader.TopRDD._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// case class Part1(p_partkey: Int, p_name: String, p_retailprice: Double)
// case class Line1(_KEY: Int, l_partkey: Int, l_quantity: Double)
// case class Cust1(c_name: String, c_orders: Int)
// case class Order1(_KEY: Int, o_orderdate: String, o_parts: Int)
// case class PL1(_KEY: Int, p_name: String, total: Double)
// case class OParts(p_name: String, total: Double)
// case class COrders1(_KEY: Int, o_orderdate: String, o_parts: Seq[OParts])
// case class COrders2(o_orderdate: String, o_parts: Seq[OParts])
// case class Top(c_name: String, c_orders: Seq[COrders2])
// case class KeyL(_1: Int, o_parts: Seq[Lineitem])
// case class KeyO(_1: Int, c_orders: Seq[TmpO])
// case class TmpO(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String, o_parts: Seq[Lineitem])
// case class TmpC(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String, c_orders: Seq[Order])

object ShredQuery4SparkDataframe extends App {
 override def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master)
    .setAppName("ShredQuery4SparkDataframe"+sf)
    .set("spark.sql.shuffle.partitions", Config.lparts.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
   import spark.implicits._
   import org.apache.spark.sql.expressions.scalalang._
val IBag_L__D = tpch.loadLineitemDF()
IBag_L__D.cache
IBag_L__D.count
val IBag_P__D = tpch.loadPartDF()
IBag_P__D.cache
IBag_P__D.count
val IBag_C__D = tpch.loadCustomersDF()
IBag_C__D.cache
IBag_C__D.count
val IBag_O__D = tpch.loadOrdersDF()
IBag_O__D.cache
IBag_O__D.count
implicit val ncode0 = Encoders.product[Part1]
implicit val ncode1 = Encoders.product[Line1]
implicit val ncode2 = Encoders.product[Cust1]
implicit val ncode3 = Encoders.product[Order1]
implicit val ncode4 = Encoders.product[PL1]
implicit val ncode5 = Encoders.product[OParts]
implicit val ncode6 = Encoders.product[COrders1]
implicit val ncode7 = Encoders.product[COrders2]
implicit val ncode8 = Encoders.product[Top]

val IBag_Query1_1 = IBag_C__D.select("c_name", "c_custkey")
  .withColumnRenamed("c_custkey", "c_orders").as[Cust1]
IBag_Query1_1.cache
IBag_Query1_1.count

val IDict_Query1_1_c_orders_1 = IBag_O__D.select("o_custkey", "o_orderdate", "o_orderkey")
  .withColumnRenamed("o_orderkey", "o_parts")
  .withColumnRenamed("o_custkey", "_KEY").as[Order1]
IDict_Query1_1_c_orders_1.repartition($"_KEY").cache
IDict_Query1_1_c_orders_1.count

val IDict_Query1_1_c_orders_1_o_parts_1 = IBag_L__D
  .select("l_orderkey", "l_partkey", "l_quantity")
  .withColumnRenamed("l_orderkey", "_KEY").as[Line1]
IDict_Query1_1_c_orders_1_o_parts_1.repartition($"_KEY").cache
IDict_Query1_1_c_orders_1_o_parts_1.count

    def f = {
 
var start0 = System.currentTimeMillis()
val MBag_Query1_1 = IBag_Query1_1

val MDict_Query1_1_c_orders_1 = IDict_Query1_1_c_orders_1

val p = IBag_P__D.select("p_partkey", "p_name", "p_retailprice").as[Part1]
val pl = IDict_Query1_1_c_orders_1_o_parts_1
  .join(p, p("p_partkey") === IDict_Query1_1_c_orders_1_o_parts_1("l_partkey"))
  .drop("p_partkey", "l_partkey")
  .withColumn("total", $"p_retailprice"*$"l_quantity").as[PL1]
// val pl2 = pl.select(pl("_KEY"), pl("p_name"), pl("l_quantity")*pl("p_retailprice")).as[PL1]
val MDict_Query1_1_c_orders_1_o_parts_1 = pl.groupByKey(x => (x._KEY, x.p_name))
  .agg(typed.sum[PL1](_.total)).mapPartitions(it =>
    it.map{case ((ok, pname), tot) => PL1(ok, pname, tot)})
// MDict_Query1_1_c_orders_1_o_parts_1.cache
MDict_Query1_1_c_orders_1_o_parts_1.count

var end0 = System.currentTimeMillis() - start0
println("Shred,Standard,Query4,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
  
var start1 = System.currentTimeMillis()

val dict2 = IDict_Query1_1_c_orders_1.groupByKey(x => x.o_parts)
val dict1 = MDict_Query1_1_c_orders_1_o_parts_1.groupByKey(x => x._KEY)
val dict3 = dict2.cogroup(dict1)( 
  (key, orders, lines) => {
    val oparts = lines.map(l => OParts(l.p_name, l.total)).toSeq
    orders.map(o => COrders1(o._KEY, o.o_orderdate, oparts))
  }).groupByKey(x => x._KEY)

val result = IBag_Query1_1.groupByKey(x => x.c_orders).cogroup(dict3)(
  (key, custs, orders) => {
    val corders = orders.map(o => COrders2(o.o_orderdate, o.o_parts)).toSeq
    custs.map(c => Top(c.c_name, corders))
  })
// result.count
    // result.rdd.flatMap{
    //   c =>
    //     if (c.c_orders.isEmpty) List((c.c_name, null, null, null))
    //     else c.c_orders.flatMap{
    //       o =>
    //         if (o.o_parts.isEmpty) List((c.c_name, o.o_orderdate, null, null))
    //         else o.o_parts.map(p => (c.c_name, o.o_orderdate, p.p_name, p.total))
    //      }
    //   }.collect.foreach(println(_))

var end1 = System.currentTimeMillis() - start1
println("Shred,Standard,Query1,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)
  
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("Shred,Standard,Query1,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
