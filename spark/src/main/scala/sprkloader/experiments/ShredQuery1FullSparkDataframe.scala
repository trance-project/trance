
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

// case class KeyL(_1: Int, o_parts: Seq[Lineitem])
// case class KeyO(_1: Int, c_orders: Seq[TmpO])
case class TmpO(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String, o_parts: Seq[Lineitem])
case class TmpC(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String, c_orders: Seq[TmpO])

object ShredQuery1FullSparkDataframe extends App {
 override def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master)
    .setAppName("ShredQuery1FullSparkDataframe"+sf)
    .set("spark.sql.shuffle.partitions", Config.lparts.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
   import spark.implicits._
val IBag_L__D = tpch.loadLineitemDF()
IBag_L__D.cache
IBag_L__D.count
// val IBag_P__D = tpch.loadPartDF()
// IBag_P__D.cache
// IBag_P__D.count
val IBag_C__D = tpch.loadCustomersDF()
IBag_C__D.cache
IBag_C__D.count

val IBag_O__D = tpch.loadOrdersDF()
IBag_O__D.cache
IBag_O__D.count
implicit val ncodec = Encoders.product[TmpC]
implicit val ncode1 = Encoders.product[TmpO]
// implicit val ncode2 = Encoders.product[KeyL]
// implicit val ncode3 = Encoders.product[KeyO]

    def f = {
 
var start0 = System.currentTimeMillis()
val MBag_Query1_1 = IBag_C__D

val MDict_Query1_1_c_orders_1 = IBag_O__D

val MDict_Query1_1_c_orders_1_o_parts_1 = IBag_L__D

var end0 = System.currentTimeMillis() - start0
println("Shred,Standard,Query1,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
  
var start1 = System.currentTimeMillis()


val dict2 = MDict_Query1_1_c_orders_1.groupByKey(x => x.o_orderkey)
val dict1 = MDict_Query1_1_c_orders_1_o_parts_1.groupByKey(x => x.l_orderkey)
val dict3 = dict2.cogroup(dict1)( 
  (key, orders, lines) => {
    val oparts = lines.toSeq
    orders.map(o => TmpO(o.o_shippriority, o.o_orderdate, o.o_custkey,
   o.o_orderpriority, o.o_clerk, o.o_orderstatus, o.o_totalprice, o.o_orderkey, o.o_comment, oparts))
  }).groupByKey(x => x.o_custkey)

val result = MBag_Query1_1.groupByKey(x => x.c_custkey).cogroup(dict3)(
  (key, custs, orders) => {
    val corders = orders.toSeq
    custs.map(c => TmpC(c.c_acctbal, c.c_name, c.c_nationkey, c.c_custkey, 
    c.c_comment, c.c_address, c.c_mktsegment, c.c_phone, corders))
  })

result.count
    // result.rdd.flatMap{
    //   c =>
    //     if (c.c_orders.isEmpty) List((c.c_name, null, null, null))
    //     else c.c_orders.flatMap{
    //       o =>
    //         if (o.o_parts.isEmpty) List((c.c_name, o.o_orderdate, null, null))
    //         else o.o_parts.map(p => (c.c_name, o.o_orderdate, p.l_partkey, p.l_quantity))
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
