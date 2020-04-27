
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
case class Recordd06e5d335bc246baa03621dc24e0caef(c_name: String, totals: Int)
case class Record749cf8f3bd6a41a3a685ec7a8bd38058(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Record39ddbf0bc1524d63b53f03908b043505(p_retailprice: Double, p_partkey: Int)
case class Recordccb2d13a38944177b7bf97ce6365d3e2(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Int, _1: Int, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record51971207c6404f7db67cca6c58f7bb2c(o_shippriority: Int, l_returnflag: String, p_name: String, l_comment: String, l_linestatus: String, o_orderdate: String, l_shipmode: String, o_custkey: Int, l_shipinstruct: String, l_quantity: Double, o_orderpriority: String, p_retailprice: Double, o_parts: Int, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, _1: Int, l_extendedprice: Double, o_clerk: String, o_orderstatus: String, l_partkey: Int, l_discount: Double, l_commitdate: String, p_partkey: Int, l_suppkey: Int, o_totalprice: Double, o_orderkey: Int, l_orderkey: Int, o_comment: String)
case class Recorda09d7cac129745cd895eebfe7f2757f8(_1: Int, total: Double)
case class Record23e8bb9313274d8f85279eb70ce55dd0(_1: Int)
case class Record41762315f4594de6ba9801bba5c05deb(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Int, c_mktsegment: String, c_phone: String)
case class Record8a3715cbb92d4e1a9d22d84e1a4da6d5(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Int, _1: Int, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record266a376ec261432fb6ebe58dd3008f00(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, _1: Int, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Oproj1(_LABEL: Int, o_parts: Int)
case class Lproj1(_1: Int, l_partkey: Int, l_quantity: Double)
case class PLproj1(_1: Int, l_partkey: Int, l_quantity: Double, p_partkey: Int, p_retailprice: Double)
case class OLproj1(_1: Option[Int], _LABEL: Int, o_parts: Int, total: Option[Double])
case class OL2proj1(_LABEL: Int, total: Option[Double])
case class Cproj1(_LABEL: Int, totals: Int, c_name: String, total: Option[Double])
case class C2proj1(c_name: String, total: Option[Double])
object ShredTest2AggSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master)
     .setAppName("ShredTest2AggSpark"+sf)
     .set("spark.sql.shuffle.partitions", Config.lparts.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   
implicit val encoderRecord749cf8f3bd6a41a3a685ec7a8bd38058: Encoder[Record749cf8f3bd6a41a3a685ec7a8bd38058] = Encoders.product[Record749cf8f3bd6a41a3a685ec7a8bd38058]
implicit val encoderRecorda09d7cac129745cd895eebfe7f2757f8: Encoder[Recorda09d7cac129745cd895eebfe7f2757f8] = Encoders.product[Recorda09d7cac129745cd895eebfe7f2757f8]
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
 .withColumn("c_orders", $"c_custkey").as[Record41762315f4594de6ba9801bba5c05deb]
 
val x9 = x8
val MBag_Test2Full_1 = x9
//MBag_Test2Full_1.print
//MBag_Test2Full_1.cache
//MBag_Test2Full_1.count
val x11 = IBag_O__D
 .withColumn("o_parts", $"o_orderkey")
 .withColumn("_1", $"o_custkey").as[Record8a3715cbb92d4e1a9d22d84e1a4da6d5]
 
val x12 = x11
val MDict_Test2Full_1_c_orders_1 = x12.repartition($"_1")
//MDict_Test2Full_1_c_orders_1.print
//MDict_Test2Full_1_c_orders_1.cache
//MDict_Test2Full_1_c_orders_1.count
val x14 = IBag_L__D
 .withColumn("_1", $"l_orderkey").as[Record266a376ec261432fb6ebe58dd3008f00]
 
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
val x35 = MBag_Test2Full_1.select("c_name", "c_orders")
 .withColumnRenamed("c_orders", "totals").as[Recordd06e5d335bc246baa03621dc24e0caef]
 
// val x36 = x35
// val MBag_Test2Agg_1 = x36
// //MBag_Test2Agg_1.print
// //MBag_Test2Agg_1.cache
// MBag_Test2Agg_1.count
// val x38 = MDict_Test2Full_1_c_orders_1 
// val x41 = x38.equiJoin[Record749cf8f3bd6a41a3a685ec7a8bd38058](MDict_Test2Full_1_c_orders_1_o_parts_1, Seq("o_parts","_1"), "left_outer")
//  .as[Recordccb2d13a38944177b7bf97ce6365d3e2]

// val x43 = IBag_P__D.select("p_name", "p_retailprice", "p_partkey")
//             .as[Record39ddbf0bc1524d63b53f03908b043505]
 
// val x47 = x41.equiJoin[Record39ddbf0bc1524d63b53f03908b043505](x43, Seq("l_partkey","p_partkey"), "inner")
//  .as[Record51971207c6404f7db67cca6c58f7bb2c]
 
// val x53 = x47.reduceByKey(x59 => 
//  Record23e8bb9313274d8f85279eb70ce55dd0(x59._1, x59.p_name), x => x.total match { case _ => x.total}).mapPartitions(
//      it => it.map{ case (x59, x60) => Recorda09d7cac129745cd895eebfe7f2757f8(x59._1, x59.p_name, x60) })
// val x54 = x53
// val MDict_Test2Agg_1_totals_1 = x54.repartition($"_1")
// //MDict_Test2Agg_1_totals_1.print
// //MDict_Test2Agg_1_totals_1.cache
// MDict_Test2Agg_1_totals_1.count

val x38 = MDict_Test2Full_1_c_orders_1.select("_1", "o_parts").withColumnRenamed("_1", "_LABEL").as[Oproj1]

val x39 = MDict_Test2Full_1_c_orders_1_o_parts_1.select("_1", "l_partkey", "l_quantity").as[Lproj1]

val x43 = IBag_P__D.select("p_retailprice", "p_partkey")
            .as[Record39ddbf0bc1524d63b53f03908b043505]
 
val x47 = x39.equiJoin[Record39ddbf0bc1524d63b53f03908b043505](x43, Seq("l_partkey","p_partkey"), "inner")
 .as[PLproj1]
 
val x53 = x47.reduceByKey(x59 => 
 Record23e8bb9313274d8f85279eb70ce55dd0(x59._1), x => x.p_retailprice match { case _ => x.p_retailprice * x.l_quantity}).mapPartitions(
     it => it.map{ case (x59, x60) => Recorda09d7cac129745cd895eebfe7f2757f8(x59._1, x60) }).as[Recorda09d7cac129745cd895eebfe7f2757f8]

val x54 = x38.equiJoin[Recorda09d7cac129745cd895eebfe7f2757f8](x53, Seq("_1", "_LABEL"), "left_outer").as[OLproj1]
  .drop("o_parts", "_1").as[OL2proj1]

val x55 = x35.equiJoin[OL2proj1](x54, Seq("_LABEL", "totals"), "left_outer").as[Cproj1]
  .drop("_LABEL", "totals").as[C2proj1]

val x56 = x55.reduceByKey(x => x.c_name, x => x.total match { case Some(t) => t; case _ => 0.0})
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
