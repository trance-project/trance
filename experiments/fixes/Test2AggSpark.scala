
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
case class Recordbf66611b9ca24de9b2434bfe700cd861(c_name: String, c_orders: Seq[Recordd1ba9d3e3a244d089e85ee48845fc377])
case class Record2f16914b45cd459a87cdc95d26b3f25b(c_name: String, c_orders: Seq[Recordd1ba9d3e3a244d089e85ee48845fc377])
case class Recordd2c865d09f804e5cb8f95c26264a8451(c_name: String, o_parts: Option[Seq[Recordaef12fe3a9cf41f1bfd92cf38e2f54ab]])
case class Recordc34ba3db7d9b40f5b7bc9c57cf26cf3b(c_name: String, o_parts: Option[Seq[Recordaef12fe3a9cf41f1bfd92cf38e2f54ab]])
case class Record54aad39a9bb14ae59cd3d49082fd9459(c_name: String, l_quantity: Option[Double], l_partkey: Option[Int])
case class Record58ab4a1caedd465fb69f1db4f962ea25(p_retailprice: Double, p_partkey: Int)
case class Record0a6aa55018ef4676884ed36114e41906(l_quantity: Option[Double], p_retailprice: Option[Double], c_name: String, l_partkey: Option[Int], p_partkey: Option[Int])
case class Record0b5f05a9b1e54de6be60188410b7e9b9(p_name: Option[String], total: Option[Double])
case class Record0eed505a4edb45fdac3c303d429f7a62(c_name: String)
case class Record4e302ce861f04e4f8d13150b2b32ba40(c_name: String, total: Double)
case class Record366d20744e9c4f40a0a6422201a930fa(c_name: String, c_orders: Double)
case class Recordaef12fe3a9cf41f1bfd92cf38e2f54ab(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Recordd1ba9d3e3a244d089e85ee48845fc377(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Seq[Recordaef12fe3a9cf41f1bfd92cf38e2f54ab], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record37e49313d720407191755cedfe030d11(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Seq[Recordd1ba9d3e3a244d089e85ee48845fc377], c_mktsegment: String, c_phone: String)
object Test2AggSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master)
     .setAppName("Test2AggSpark"+sf)
     .set("spark.sql.shuffle.partitions", Config.lparts.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   implicit val encoderRecordaef12fe3a9cf41f1bfd92cf38e2f54ab: Encoder[Recordaef12fe3a9cf41f1bfd92cf38e2f54ab] = Encoders.product[Recordaef12fe3a9cf41f1bfd92cf38e2f54ab]
implicit val encoderRecordd1ba9d3e3a244d089e85ee48845fc377: Encoder[Recordd1ba9d3e3a244d089e85ee48845fc377] = Encoders.product[Recordd1ba9d3e3a244d089e85ee48845fc377]
implicit val encoderRecord0b5f05a9b1e54de6be60188410b7e9b9: Encoder[Record0b5f05a9b1e54de6be60188410b7e9b9] = Encoders.product[Record0b5f05a9b1e54de6be60188410b7e9b9]
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

   val x20 = O 
val x22 = L 
val x28 = x20.cogroup(x22.groupByKey(x24 => x24.l_orderkey), x23 => x23.o_orderkey)(
   (_, x41, x42) => {
     val x43 = x42.map(x24 => Recordaef12fe3a9cf41f1bfd92cf38e2f54ab(x24.l_returnflag, x24.l_comment, x24.l_linestatus, x24.l_shipmode, x24.l_shipinstruct, x24.l_quantity, x24.l_receiptdate, x24.l_linenumber, x24.l_tax, x24.l_shipdate, x24.l_extendedprice, x24.l_partkey, x24.l_discount, x24.l_commitdate, x24.l_suppkey, x24.l_orderkey)).toSeq
     x41.map(x26 => Recordd1ba9d3e3a244d089e85ee48845fc377(x26.o_shippriority, x26.o_orderdate, x26.o_custkey, x26.o_orderpriority, x43, x26.o_clerk, x26.o_orderstatus, x26.o_totalprice, x26.o_orderkey, x26.o_comment))
 }).as[Recordd1ba9d3e3a244d089e85ee48845fc377]
val x29 = x28
val orders = x29
//orders.print
orders.cache
//orders.count
val x31 = C 
val x33 = orders 
val x39 = x31.cogroup(x33.groupByKey(x35 => x35.o_custkey), x34 => x34.c_custkey)(
   (_, x44, x45) => {
     val x46 = x45.map(x35 => Recordd1ba9d3e3a244d089e85ee48845fc377(x35.o_shippriority, x35.o_orderdate, x35.o_custkey, x35.o_orderpriority, x35.o_parts, x35.o_clerk, x35.o_orderstatus, x35.o_totalprice, x35.o_orderkey, x35.o_comment)).toSeq
     x44.map(x37 => Record37e49313d720407191755cedfe030d11(x37.c_acctbal, x37.c_name, x37.c_nationkey, x37.c_custkey, x37.c_comment, x37.c_address, x46, x37.c_mktsegment, x37.c_phone))
 }).as[Record37e49313d720407191755cedfe030d11]
val x40 = x39
val Test2Full = x40
//Test2Full.print
Test2Full.cache
Test2Full.count




def f = { 
 
 val x74 = Test2Full.select("c_name", "c_orders")
            .as[Recordbf66611b9ca24de9b2434bfe700cd861]
 
val x77 = x74//.withColumn("index", monotonically_increasing_id())
 .as[Record2f16914b45cd459a87cdc95d26b3f25b].flatMap{
   case x100 => if (x100.c_orders.isEmpty) Seq(Recordd2c865d09f804e5cb8f95c26264a8451(x100.c_name, None)) 
     else x100.c_orders.map( x101 => Recordd2c865d09f804e5cb8f95c26264a8451(x100.c_name, Some(x101.o_parts)) )
}.as[Recordd2c865d09f804e5cb8f95c26264a8451]
 
val x81 = x77//.withColumn("index", monotonically_increasing_id())
 .as[Recordc34ba3db7d9b40f5b7bc9c57cf26cf3b].flatMap{
   case x102 => x102.o_parts match {
     case None => Seq(Record54aad39a9bb14ae59cd3d49082fd9459(x102.c_name, None, None))
     case Some(bag) if bag.isEmpty => Seq(Record54aad39a9bb14ae59cd3d49082fd9459(x102.c_name, None, None))
     case Some(bag) => bag.map( x103 => Record54aad39a9bb14ae59cd3d49082fd9459(x102.c_name, Some(x103.l_quantity), Some(x103.l_partkey)) )
 }}.as[Record54aad39a9bb14ae59cd3d49082fd9459]
 
val x83 = P.select("p_retailprice", "p_partkey")
            .as[Record58ab4a1caedd465fb69f1db4f962ea25]
 
val x88 = x81.equiJoin[Record58ab4a1caedd465fb69f1db4f962ea25](x83, Seq("l_partkey","p_partkey"), "left_outer")
 .as[Record0a6aa55018ef4676884ed36114e41906]
 
val x95 = x88.reduceByKey(x104 => 
 Record0eed505a4edb45fdac3c303d429f7a62(x104.c_name), x => x.p_retailprice match {
       case Some(r) => r * x.l_quantity.get; case _ => 0.0
   }).mapPartitions(
     it => it.map{ case (x104, x105) => Record4e302ce861f04e4f8d13150b2b32ba40(x104.c_name, x105)
 })
// val x98 = x95.reduceByKey(x105 => x105._1, x105 => x105._2.total match { case Some(r) => r; case _ => 0.0 }).mapPartitions{ it => it.map{
//  case (x96,x97) => Record366d20744e9c4f40a0a6422201a930fa(x96.c_name, x97)
// }}.as[Record366d20744e9c4f40a0a6422201a930fa]
 
// val x99 = x98
val x99 = x95
val Test2Agg = x99
// Test2Agg.print
//Test2Agg.cache
Test2Agg.count


}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start 
   println("Flat++,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
