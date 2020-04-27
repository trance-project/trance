
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
case class Recordb8484513eaed470d947b7c67ca2d438c(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Seq[Record597f65dcd7dc4785975c7d4f56645da6], c_mktsegment: String, c_phone: String, index: Long)
case class Record661b36b58e3640a89de49309079847d3(c_acctbal: Double, c_name: String, o_parts: Option[Seq[Recorddba06c3f790c4c5a873e3cbdff7bed13]], c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String, index: Long)
case class Record1e82904b718e4be7896aa7b2d4d81a7d(c_acctbal: Double, l_quantity: Option[Double], c_name: String, c_nationkey: Int, l_partkey: Option[Int], c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String, index: Long)
case class Record076fd338f4244cc1a2640803edf70c39(p_retailprice: Double, p_partkey: Int)
case class Record1cd22b16a2e84a8d8d045aa2880a0006(c_acctbal: Double, l_quantity: Option[Double], p_retailprice: Option[Double], c_name: String, c_nationkey: Int, l_partkey: Option[Int], p_partkey: Option[Int], c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String, index: Long)
case class Recorde9c0a56e3f9f4a508b59fb0281cddbbd(p_name: Option[String], total: Option[Double])
case class Record88e622bbb1544121b281c47b53140dd4(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String, index: Long)
case class Record9bf4771e2f404968bdb00523065721f4(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String, index: Long)
case class Record9bf4771e2f404968bdb00523065721f42(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String, total: Double)
case class Record3b583adac75249bab27d1996ceabdbe9(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Double, c_mktsegment: String, c_phone: String)
case class Recorddba06c3f790c4c5a873e3cbdff7bed13(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Record597f65dcd7dc4785975c7d4f56645da6(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Seq[Recorddba06c3f790c4c5a873e3cbdff7bed13], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record77aa6994ca8f4cdc8283f295fa2420f0(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Seq[Record597f65dcd7dc4785975c7d4f56645da6], c_mktsegment: String, c_phone: String)
object Test2FullAggSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master)
     .setAppName("Test2FullAggSpark"+sf)
     .set("spark.sql.shuffle.partitions", Config.lparts.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   implicit val encoderRecorddba06c3f790c4c5a873e3cbdff7bed13: Encoder[Recorddba06c3f790c4c5a873e3cbdff7bed13] = Encoders.product[Recorddba06c3f790c4c5a873e3cbdff7bed13]
implicit val encoderRecord597f65dcd7dc4785975c7d4f56645da6: Encoder[Record597f65dcd7dc4785975c7d4f56645da6] = Encoders.product[Record597f65dcd7dc4785975c7d4f56645da6]
implicit val encoderRecorde9c0a56e3f9f4a508b59fb0281cddbbd: Encoder[Recorde9c0a56e3f9f4a508b59fb0281cddbbd] = Encoders.product[Recorde9c0a56e3f9f4a508b59fb0281cddbbd]
   import spark.implicits._
   val tpch = TPCHLoader(spark)
val L = tpch.loadLineitemDF()
L.cache
L.count
val P = tpch.loadPartDF()
P.cache
P.count
val C = tpch.loadCustomerDF()
C.cache
C.count
val O = tpch.loadOrderDF()
O.cache
O.count

   val x125 = O 
val x127 = L 
val x133 = x125.cogroup(x127.groupByKey(x129 => x129.l_orderkey), x128 => x128.o_orderkey)(
   (_, x146, x147) => {
     val x148 = x147.map(x129 => Recorddba06c3f790c4c5a873e3cbdff7bed13(x129.l_returnflag, x129.l_comment, x129.l_linestatus, x129.l_shipmode, x129.l_shipinstruct, x129.l_quantity, x129.l_receiptdate, x129.l_linenumber, x129.l_tax, x129.l_shipdate, x129.l_extendedprice, x129.l_partkey, x129.l_discount, x129.l_commitdate, x129.l_suppkey, x129.l_orderkey)).toSeq
     x146.map(x131 => Record597f65dcd7dc4785975c7d4f56645da6(x131.o_shippriority, x131.o_orderdate, x131.o_custkey, x131.o_orderpriority, x148, x131.o_clerk, x131.o_orderstatus, x131.o_totalprice, x131.o_orderkey, x131.o_comment))
 }).as[Record597f65dcd7dc4785975c7d4f56645da6]
val x134 = x133
val orders = x134
//orders.print
orders.cache
//orders.count
val x136 = C 
val x138 = orders 
val x144 = x136.cogroup(x138.groupByKey(x140 => x140.o_custkey), x139 => x139.c_custkey)(
   (_, x149, x150) => {
     val x151 = x150.map(x140 => Record597f65dcd7dc4785975c7d4f56645da6(x140.o_shippriority, x140.o_orderdate, x140.o_custkey, x140.o_orderpriority, x140.o_parts, x140.o_clerk, x140.o_orderstatus, x140.o_totalprice, x140.o_orderkey, x140.o_comment)).toSeq
     x149.map(x142 => Record77aa6994ca8f4cdc8283f295fa2420f0(x142.c_acctbal, x142.c_name, x142.c_nationkey, x142.c_custkey, x142.c_comment, x142.c_address, x151, x142.c_mktsegment, x142.c_phone))
 }).as[Record77aa6994ca8f4cdc8283f295fa2420f0]
val x145 = x144
val Test2Full = x145
//Test2Full.print
Test2Full.cache
Test2Full.count




def f = { 
 
 val x179 = Test2Full 
val x182 = x179.withColumn("index", monotonically_increasing_id())
 .as[Recordb8484513eaed470d947b7c67ca2d438c].flatMap{
   case x205 => if (x205.c_orders.isEmpty) Seq(Record661b36b58e3640a89de49309079847d3(x205.c_acctbal, x205.c_name, None, x205.c_nationkey, x205.c_custkey, x205.c_comment, x205.c_address, x205.c_mktsegment, x205.c_phone, x205.index)) 
     else x205.c_orders.map( x206 => Record661b36b58e3640a89de49309079847d3(x205.c_acctbal, x205.c_name, Some(x206.o_parts), x205.c_nationkey, x205.c_custkey, x205.c_comment, x205.c_address, x205.c_mktsegment, x205.c_phone, x205.index) )
}.as[Record661b36b58e3640a89de49309079847d3]
 
val x186 = x182.withColumn("index", monotonically_increasing_id())
 .as[Record661b36b58e3640a89de49309079847d3].flatMap{
   case x207 => x207.o_parts match {
     case None => Seq(Record1e82904b718e4be7896aa7b2d4d81a7d(x207.c_acctbal, None, x207.c_name, x207.c_nationkey, None, x207.c_custkey, x207.c_comment, x207.c_address, x207.c_mktsegment, x207.c_phone, x207.index))
     case Some(bag) if bag.isEmpty => Seq(Record1e82904b718e4be7896aa7b2d4d81a7d(x207.c_acctbal, None, x207.c_name, x207.c_nationkey, None, x207.c_custkey, x207.c_comment, x207.c_address, x207.c_mktsegment, x207.c_phone, x207.index))
     case Some(bag) => bag.map( x208 => Record1e82904b718e4be7896aa7b2d4d81a7d(x207.c_acctbal, Some(x208.l_quantity), x207.c_name, x207.c_nationkey, Some(x208.l_partkey), x207.c_custkey, x207.c_comment, x207.c_address, x207.c_mktsegment, x207.c_phone, x207.index) )
 }}.as[Record1e82904b718e4be7896aa7b2d4d81a7d]
 
val x188 = P.select("p_retailprice", "p_partkey")
            .as[Record076fd338f4244cc1a2640803edf70c39]
 
val x193 = x186.equiJoin[Record076fd338f4244cc1a2640803edf70c39](x188, Seq("l_partkey","p_partkey"), "left_outer")
 .as[Record1cd22b16a2e84a8d8d045aa2880a0006]
 
val x200 = x193.reduceByKey(x209 => 
 Record88e622bbb1544121b281c47b53140dd4(x209.c_acctbal, x209.c_name, x209.c_nationkey, x209.c_custkey, x209.c_comment, x209.c_address, x209.c_mktsegment, x209.c_phone, x209.index), x => x.p_retailprice match {
       case Some(t) => t * x.l_quantity.get; case _ => 0.0
   }).mapPartitions(
     it => it.map{ case (x209, x210) => Record9bf4771e2f404968bdb00523065721f42(x209.c_acctbal, x209.c_name, x209.c_nationkey, x209.c_custkey, x209.c_comment, x209.c_address, x209.c_mktsegment, x209.c_phone, x210)}
  ).as[Record9bf4771e2f404968bdb00523065721f42]

// val x203 = x200.reduceByKey(x210 => x210._1, x210 => x210._2.total match { case Some(t) => t; case _ => 0.0 }).mapPartitions{ it => 
//   it.map{
//     case (x201,x202) => Record3b583adac75249bab27d1996ceabdbe9(x201.c_acctbal, x201.c_name, x201.c_nationkey, x201.c_custkey, x201.c_comment, x201.c_address, x202, x201.c_mktsegment, x201.c_phone)
//   }}.as[Record3b583adac75249bab27d1996ceabdbe9]
 
// val x204 = x203
val x204 = x200
val Test2FullAgg = x204
Test2FullAgg.print
//Test2FullAgg.cache
Test2FullAgg.count


}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start 
   println("Flat++,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
