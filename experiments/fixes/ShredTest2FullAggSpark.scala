
package sprkloader.experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.scalalang._
import sprkloader._
import sprkloader.SkewDataset._
case class Record950c522e19ae4f31b4b6643f63c14c6e(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Int, c_mktsegment: String, c_phone: String)
case class Record486664516b1442cc90ef0f88f8fd3474(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Recordc3db8f801447402a9258a190e07e323e(p_retailprice: Double, p_partkey: Int)
case class Recordbff33515b1c846d5bdd629c8edcf41f7(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Int, _1: Int, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record8fecd9d6b7534165a1b69e1a1465c5e1(_1: Int, l_partkey: Int, p_partkey: Int, l_quantity: Double, p_retailprice: Double)
case class Oproj(_LABEL: Int, o_parts: Int)
case class Lproj(_1: Int, l_partkey: Int, l_quantity: Double)
case class OLproj(_LABEL: Int, o_parts: Int, _1: Option[Int], total: Option[Double])
case class OL2proj(_LABEL: Int, total: Option[Double])
case class CProj(_LABEL: Int, total: Option[Double], c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Int, c_mktsegment: String, c_phone: String)
case class C2Proj(total: Option[Double], c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String)
case class Record160de2c13d4d463a89fab0cbe1e35232(_1: Int, total: Double)
case class Record2207945f9b844b1a813606e81d7e7bae(_1: Int)
case class Recorddc5924b533d44a19a9a1826a31443505(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Int, c_mktsegment: String, c_phone: String)
case class Record47353cafbd3941a5be43b09615956955(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Int, _1: Int, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record37fca3c3d6be42cf82399afed8b07871(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, _1: Int, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
object ShredTest2FullAggSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master)
     .setAppName("ShredTest2FullAggSpark"+sf)
     .set("spark.sql.shuffle.partitions", Config.lparts.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   
implicit val encoderRecord486664516b1442cc90ef0f88f8fd3474: Encoder[Record486664516b1442cc90ef0f88f8fd3474] = Encoders.product[Record486664516b1442cc90ef0f88f8fd3474]
implicit val encoderRecord160de2c13d4d463a89fab0cbe1e35232: Encoder[Record160de2c13d4d463a89fab0cbe1e35232] = Encoders.product[Record160de2c13d4d463a89fab0cbe1e35232]
   import spark.implicits._
   val tpch = TPCHLoader(spark)
val IBag_L__D = tpch.loadLineitemDF()
IBag_L__D.cache
IBag_L__D.count
val IBag_P__D = tpch.loadPartDF()
IBag_P__D.cache
IBag_P__D.count
val IBag_C__D = tpch.loadCustomerDF()
IBag_C__D.cache
IBag_C__D.count
val IBag_O__D = tpch.loadOrderDF()
IBag_O__D.cache
IBag_O__D.count

   val x8 = IBag_C__D
 .withColumn("c_orders", $"c_custkey").as[Recorddc5924b533d44a19a9a1826a31443505]
 
val x9 = x8
val MBag_Test2Full_1 = x9
//MBag_Test2Full_1.print
//MBag_Test2Full_1.cache
//MBag_Test2Full_1.count
val x11 = IBag_O__D
 .withColumn("o_parts", $"o_orderkey")
 .withColumn("_1", $"o_custkey").as[Record47353cafbd3941a5be43b09615956955]
 
val x12 = x11
val MDict_Test2Full_1_c_orders_1 = x12.repartition($"_1")
//MDict_Test2Full_1_c_orders_1.print
//MDict_Test2Full_1_c_orders_1.cache
//MDict_Test2Full_1_c_orders_1.count
val x14 = IBag_L__D
 .withColumn("_1", $"l_orderkey").as[Record37fca3c3d6be42cf82399afed8b07871]
 
val x15 = x14
val MDict_Test2Full_1_c_orders_1_o_parts_1 = x15.repartition($"_1")
//MDict_Test2Full_1_c_orders_1_o_parts_1.print
//MDict_Test2Full_1_c_orders_1_o_parts_1.cache
//MDict_Test2Full_1_c_orders_1_o_parts_1.count



val IBag_Test2Full__D = MBag_Test2Full_1
IBag_Test2Full__D.cache
IBag_Test2Full__D.count
val IDict_Test2Full__D_c_orders = MDict_Test2Full_1_c_orders_1
IDict_Test2Full__D_c_orders.cache
IDict_Test2Full__D_c_orders.count
val IDict_Test2Full__D_c_orders_o_parts = MDict_Test2Full_1_c_orders_1_o_parts_1
IDict_Test2Full__D_c_orders_o_parts.cache
IDict_Test2Full__D_c_orders_o_parts.count
 def f = {
 
 
var start0 = System.currentTimeMillis()
val x35 = MBag_Test2Full_1

val x38 = MDict_Test2Full_1_c_orders_1.select("_1", "o_parts").withColumnRenamed("_1", "_LABEL").as[Oproj]

val x39 = MDict_Test2Full_1_c_orders_1_o_parts_1.select("_1", "l_partkey", "l_quantity").as[Lproj]

val x43 = IBag_P__D.select("p_retailprice", "p_partkey")
            .as[Recordc3db8f801447402a9258a190e07e323e]
 
val x47 = x39.equiJoin[Recordc3db8f801447402a9258a190e07e323e](x43, Seq("l_partkey","p_partkey"), "inner")
 .as[Record8fecd9d6b7534165a1b69e1a1465c5e1]
 
val x53 = x47.reduceByKey(x59 => 
 Record2207945f9b844b1a813606e81d7e7bae(x59._1), x => x.p_retailprice match { case _ => x.p_retailprice * x.l_quantity}).mapPartitions(
     it => it.map{ case (x59, x60) => Record160de2c13d4d463a89fab0cbe1e35232(x59._1, x60) }).as[Record160de2c13d4d463a89fab0cbe1e35232]

val x54 = x38.equiJoin[Record160de2c13d4d463a89fab0cbe1e35232](x53, Seq("_1", "_LABEL"), "left_outer").as[OLproj]
  .drop("o_parts", "_1").as[OL2proj]

val x55 = x35.equiJoin[OL2proj](x54, Seq("_LABEL", "c_orders"), "left_outer").as[CProj]
  .drop("_LABEL", "c_orders").as[C2Proj]

val x56 = x55
val MDict_Test2FullAgg_1_c_orders_1 = x56//.repartition($"_1")
// MDict_Test2FullAgg_1_c_orders_1.print
//MDict_Test2FullAgg_1_c_orders_1.cache
MDict_Test2FullAgg_1_c_orders_1.count




var end0 = System.currentTimeMillis() - start0
println("Shred,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("Shred,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
