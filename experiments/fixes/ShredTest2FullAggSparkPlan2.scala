
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

 case class Oproj2(_LABEL: Int, o_parts: Int)
case class Lproj2(_1: Int, l_partkey: Int, l_quantity: Double)
case class OLproj2(_LABEL: Int, o_parts: Int, _1: Option[Int], l_partkey: Option[Int], l_quantity: Option[Double])
case class OL2proj2(_LABEL: Int, l_partkey: Option[Int], l_quantity: Option[Double])
case class COLproj2(_LABEL: Int, l_partkey: Option[Int], l_quantity: Option[Double], c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Int, c_mktsegment: String, c_phone: String)
case class COL2proj2(l_partkey: Option[Int], l_quantity: Option[Double], c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String)
case class COL3proj2(p_partkey: Option[Int], p_retailprice: Option[Double], l_partkey: Option[Int], l_quantity: Option[Double], c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String)
case class Cproj2(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String)
case class C2proj2(total: Double, c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String)

object ShredTest2FullAggSparkPlan2 {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master)
     .setAppName("ShredTest2FullAggSparkPlan2"+sf)
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

val x38 = MDict_Test2Full_1_c_orders_1.select("_1", "o_parts").withColumnRenamed("_1", "_LABEL").as[Oproj2]

val x39 = MDict_Test2Full_1_c_orders_1_o_parts_1.select("_1", "l_partkey", "l_quantity").as[Lproj2]

val x40 = x38.equiJoin[Lproj2](x39, Seq("_1", "o_parts"), "left_outer").as[OLproj2]
  .drop("_1", "o_parts").as[OL2proj2]

val x41 = x35.equiJoin[OL2proj2](x40, Seq("_LABEL", "c_orders"), "left_outer").as[COLproj2]
  .drop("LABEL", "c_orders").as[COL2proj2]

val x43 = IBag_P__D.select("p_retailprice", "p_partkey")
            .as[Recordc3db8f801447402a9258a190e07e323e]
 
val x47 = x41.equiJoin[Recordc3db8f801447402a9258a190e07e323e](x43, Seq("l_partkey","p_partkey"), "left_outer")
 .as[COL3proj2]
 
val x53 = x47.reduceByKey(x59 => 
 Cproj2(x59.c_acctbal, x59.c_name, x59.c_nationkey, x59.c_custkey, x59.c_comment, x59.c_address, x59.c_mktsegment, x59.c_phone), 
  x => (x.p_retailprice, x.l_quantity) match { case (Some(r), Some(q)) => r * q; case _ => 0.0}).mapPartitions(
     it => it.map{ case (x59, x60) => C2proj2(x60, x59.c_acctbal, x59.c_name, x59.c_nationkey, x59.c_custkey, x59.c_comment, x59.c_address, x59.c_mktsegment, x59.c_phone) 
    }).as[C2proj2]

val x56 = x53
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
