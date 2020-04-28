
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
case class Recorda6fbeb5cb87c4b21939a8a72e4213de7(c_name: String, c_orders: Seq[Record75552e474f8a4e1886f61fbd4badb6e6])
case class Recordebbc19c949d842f4bb14f6dc0aa2acfd(c_name: String, c_orders: Seq[Record75552e474f8a4e1886f61fbd4badb6e6], index: Long)
case class Recordde8c8f90d0df4f22a814e33741541f4e(c_name: String, o_orderdate: Option[String], o_parts: Option[Seq[Record16a9a80c2e4a4b2486e4f3107296b75e]], index: Long)
case class Record8320edbaa0174a3d9ef1486f5dedd70a(index: Long, c_name: String, o_orderdate: Option[String], o_parts: Option[Seq[Record16a9a80c2e4a4b2486e4f3107296b75e]])
case class Record43d979413fd04667b53dcc55170f8065(o_orderdate: Option[String], l_quantity: Option[Double], c_name: String, l_partkey: Option[Int], index: Long)
case class Record839610e54360460dabe5c9f9c646fbd9(p_name: String, p_partkey: Int)
case class Recordcff7761a6de5442ea2b229368ca293de(p_name: Option[String], o_orderdate: Option[String], l_quantity: Option[Double], c_name: String, l_partkey: Option[Int], p_partkey: Option[Int], index: Long)
case class Record9c504e510cb94bd5952899d9da5d994f(p_name: Option[String], l_quantity: Option[Double])
case class Recorde2f85c23d4b24760a3c1f44be1b53393(index: Long, c_name: String, o_orderdate: Option[String], p_name: Option[String])
case class Record6c1c97b4287341b3a5857dfa013f8d9a(index: Long, c_name: String, o_orderdate: Option[String])
case class Recorda85064975f5547fda92ba809980813c4(p_name: Option[String], o_orderdate: Option[String], l_quantity: Option[Double], c_name: String, index: Long)
case class Recordae6d72d6ba614893ae0dae03638eeb27(c_name: String)
case class Record6806662558c34f89b699bff445ce0f6a(p_name: String, l_quantity: Double)
case class Record06145ec864c6451491ed22c899332fcf(o_orderdate: String, o_parts: Seq[Record6806662558c34f89b699bff445ce0f6a])
case class Record89625c5d215242619e114978b2a9f730(c_name: String, c_orders: Seq[Record06145ec864c6451491ed22c899332fcf])
case class Record16a9a80c2e4a4b2486e4f3107296b75e(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Record75552e474f8a4e1886f61fbd4badb6e6(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Seq[Record16a9a80c2e4a4b2486e4f3107296b75e], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record1b427e5f19144b9b882d8f1782a54d19(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Seq[Record75552e474f8a4e1886f61fbd4badb6e6], c_mktsegment: String, c_phone: String)
object Test2NNSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master)
     .setAppName("Test2NNSpark"+sf)
     .set("spark.sql.shuffle.partitions", Config.lparts.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   implicit val encoderRecord16a9a80c2e4a4b2486e4f3107296b75e: Encoder[Record16a9a80c2e4a4b2486e4f3107296b75e] = Encoders.product[Record16a9a80c2e4a4b2486e4f3107296b75e]
implicit val encoderRecord75552e474f8a4e1886f61fbd4badb6e6: Encoder[Record75552e474f8a4e1886f61fbd4badb6e6] = Encoders.product[Record75552e474f8a4e1886f61fbd4badb6e6]
implicit val encoderRecord9c504e510cb94bd5952899d9da5d994f: Encoder[Record9c504e510cb94bd5952899d9da5d994f] = Encoders.product[Record9c504e510cb94bd5952899d9da5d994f]
implicit val encoderRecord6806662558c34f89b699bff445ce0f6a: Encoder[Record6806662558c34f89b699bff445ce0f6a] = Encoders.product[Record6806662558c34f89b699bff445ce0f6a]
implicit val encoderRecord06145ec864c6451491ed22c899332fcf: Encoder[Record06145ec864c6451491ed22c899332fcf] = Encoders.product[Record06145ec864c6451491ed22c899332fcf]
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

   val x119 = O 
val x121 = L 
val x127 = x119.cogroup(x121.groupByKey(x123 => x123.l_orderkey), x122 => x122.o_orderkey)(
   (_, x140, x141) => {
     val x142 = x141.map(x123 => Record16a9a80c2e4a4b2486e4f3107296b75e(x123.l_returnflag, x123.l_comment, x123.l_linestatus, x123.l_shipmode, x123.l_shipinstruct, x123.l_quantity, x123.l_receiptdate, x123.l_linenumber, x123.l_tax, x123.l_shipdate, x123.l_extendedprice, x123.l_partkey, x123.l_discount, x123.l_commitdate, x123.l_suppkey, x123.l_orderkey)).toSeq
     x140.map(x125 => Record75552e474f8a4e1886f61fbd4badb6e6(x125.o_shippriority, x125.o_orderdate, x125.o_custkey, x125.o_orderpriority, x142, x125.o_clerk, x125.o_orderstatus, x125.o_totalprice, x125.o_orderkey, x125.o_comment))
 }).as[Record75552e474f8a4e1886f61fbd4badb6e6]
val x128 = x127
val orders = x128
//orders.print
orders.cache
//orders.count
val x130 = C 
val x132 = orders 
val x138 = x130.cogroup(x132.groupByKey(x134 => x134.o_custkey), x133 => x133.c_custkey)(
   (_, x143, x144) => {
     val x145 = x144.map(x134 => Record75552e474f8a4e1886f61fbd4badb6e6(x134.o_shippriority, x134.o_orderdate, x134.o_custkey, x134.o_orderpriority, x134.o_parts, x134.o_clerk, x134.o_orderstatus, x134.o_totalprice, x134.o_orderkey, x134.o_comment)).toSeq
     x143.map(x136 => Record1b427e5f19144b9b882d8f1782a54d19(x136.c_acctbal, x136.c_name, x136.c_nationkey, x136.c_custkey, x136.c_comment, x136.c_address, x145, x136.c_mktsegment, x136.c_phone))
 }).as[Record1b427e5f19144b9b882d8f1782a54d19]
val x139 = x138
val Test2Full = x139
//Test2Full.print
Test2Full.cache
Test2Full.count




def f = { 
 
 val x178 = Test2Full.select("c_name", "c_orders")
            .as[Recorda6fbeb5cb87c4b21939a8a72e4213de7]
 
val x181 = x178.withColumn("index", monotonically_increasing_id())
 .as[Recordebbc19c949d842f4bb14f6dc0aa2acfd].flatMap{
   case x209 => if (x209.c_orders.isEmpty) Seq(Recordde8c8f90d0df4f22a814e33741541f4e(x209.c_name, None, None, x209.index)) 
     else x209.c_orders.map( x210 => Recordde8c8f90d0df4f22a814e33741541f4e(x209.c_name, Some(x210.o_orderdate), Some(x210.o_parts), x209.index) )
}.as[Recordde8c8f90d0df4f22a814e33741541f4e]
 
val x185 = x181.withColumn("index", monotonically_increasing_id())
 .as[Record8320edbaa0174a3d9ef1486f5dedd70a].flatMap{
   case x211 => x211.o_parts match {
     case None => Seq(Record43d979413fd04667b53dcc55170f8065(x211.o_orderdate, None, x211.c_name, None, x211.index))
     case Some(bag) if bag.isEmpty => Seq(Record43d979413fd04667b53dcc55170f8065(x211.o_orderdate, None, x211.c_name, None, x211.index))
     case Some(bag) => bag.map( x212 => Record43d979413fd04667b53dcc55170f8065(x211.o_orderdate, Some(x212.l_quantity), x211.c_name, Some(x212.l_partkey), x211.index) )
 }}.as[Record43d979413fd04667b53dcc55170f8065]
 
val x187 = P.select("p_name", "p_partkey")
            .as[Record839610e54360460dabe5c9f9c646fbd9]
 
val x192 = x185.equiJoin[Record839610e54360460dabe5c9f9c646fbd9](x187, Seq("l_partkey","p_partkey"), "left_outer")
 .as[Recordcff7761a6de5442ea2b229368ca293de]
 
val x199 = x192.reduceByKey(x213 => 
 Recorde2f85c23d4b24760a3c1f44be1b53393(x213.index, x213.c_name, x213.o_orderdate, x213.p_name), x => x.l_quantity match {
       case Some(r) => r; case _ => 0.0
   }).mapPartitions(
     it => it.map{ case (x213, x214) => (Record6c1c97b4287341b3a5857dfa013f8d9a(x213.index, x213.c_name, x213.o_orderdate), Record9c504e510cb94bd5952899d9da5d994f(x213.p_name, Some(x214)))
 })

val x200 = x199.setGroups(_._1).mapGroups{
    case (x92, x93) => 
    val grps = x93.flatMap{
      x => x._2.p_name match {
        case None => Seq()
        case _ => Seq(Record6806662558c34f89b699bff445ce0f6a(x._2.p_name.get, x._2.l_quantity.get))
      }
    }.toSeq
    (x92, grps)
}

val x204 = x200
 .setGroups(x215 =>
   Recordae6d72d6ba614893ae0dae03638eeb27(x215._1.c_name)).mapGroups{
   case (x216, group) => 
     val ngroup = group.flatMap{
       x215 => x215._1.o_orderdate match {
         case None => Seq()
         case _ => Seq(Record06145ec864c6451491ed22c899332fcf(x215._1.o_orderdate.get,x215._2))
        }
     }.toSeq
   (x216, ngroup)
}
 
val x207 = x204.mapPartitions{ it => it.map{
 case (x205,x206) => Record89625c5d215242619e114978b2a9f730(x205.c_name, x206)
}}.as[Record89625c5d215242619e114978b2a9f730]
 
val x208 = x207
val Test2NN = x208
// Test2NN.print
//Test2NN.cache
  Test2NN.count

}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start 
   println("Flat++,2,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
