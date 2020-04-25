
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
case class Record659b90479b7c4244a70a9243943935d3(o_orderdate: String, o_parts: Seq[Record456c62ecd9b747ea8f443104bfb30b1e])
case class Record5b0dcca28b5e49768a0dd1c3e6bf3a5f(o_orderdate: String, o_parts: Seq[Record456c62ecd9b747ea8f443104bfb30b1e], index: Long)
case class Record8d5cadea9a4542c9b2c5a393373a1016(o_orderdate: String, l_quantity: Option[Double], l_partkey: Option[Int], index: Long)
case class Record6ff2841e8f1443519f5d80a3284e7138(p_name: String, p_partkey: Int)
case class Record8d0642c629dd4fbeab6af048db2fd890(p_name: Option[String], o_orderdate: String, l_quantity: Option[Double], l_partkey: Option[Int], p_partkey: Option[Int], index: Long)
case class Recordcfae396e1f314152907e01cd0fd3c73a(p_name: Option[String], l_quantity: Option[Double])
case class Recordc795d787adf04881a8782567e4047ae0(index: Long, o_orderdate: String, p_name: Option[String])
case class Record18d0c7ef55c04eefb002eaf2ea2ebdc1(index: Long, o_orderdate: String)
case class Recordf21c0d70b2e747f7abf61acf61d66ef9(o_orderdate: String, o_parts: Seq[Recordcfae396e1f314152907e01cd0fd3c73a])
case class Record456c62ecd9b747ea8f443104bfb30b1e(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Record7c0e640ae9794940b6665d6cb292060a(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Seq[Record456c62ecd9b747ea8f443104bfb30b1e], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
object Test1NNSkewSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master)
     .setAppName("Test1NNSkewSpark"+sf)
     .set("spark.sql.shuffle.partitions", Config.lparts.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   implicit val encoderRecord456c62ecd9b747ea8f443104bfb30b1e: Encoder[Record456c62ecd9b747ea8f443104bfb30b1e] = Encoders.product[Record456c62ecd9b747ea8f443104bfb30b1e]
implicit val encoderRecordcfae396e1f314152907e01cd0fd3c73a: Encoder[Recordcfae396e1f314152907e01cd0fd3c73a] = Encoders.product[Recordcfae396e1f314152907e01cd0fd3c73a]
   import spark.implicits._
   val tpch = TPCHLoader(spark)
val L_L = tpch.loadLineitemDF()
L_L.cache
L_L.count
val L_H = Seq.empty[Lineitem].toDS()
val L = (L_L, L_H)
val P_L = tpch.loadPartDF()
P_L.cache
P_L.count
val P_H = Seq.empty[Part].toDS()
val P = (P_L, P_H)
val O_L = tpch.loadOrderDF()
O_L.cache
O_L.count
val O_H = Seq.empty[Order].toDS()
val O = (O_L, O_H)

   val x2659 = O 
val x2661 = L 
val x2667 = x2659.cogroup(x2661.groupByKey(x2663 => x2663.l_orderkey), x2662 => x2662.o_orderkey)(
   (_, x2669, x2670) => {
     val x2671 = x2670.map(x2663 => Record456c62ecd9b747ea8f443104bfb30b1e(x2663.l_returnflag, x2663.l_comment, x2663.l_linestatus, x2663.l_shipmode, x2663.l_shipinstruct, x2663.l_quantity, x2663.l_receiptdate, x2663.l_linenumber, x2663.l_tax, x2663.l_shipdate, x2663.l_extendedprice, x2663.l_partkey, x2663.l_discount, x2663.l_commitdate, x2663.l_suppkey, x2663.l_orderkey)).toSeq
     x2669.map(x2665 => Record7c0e640ae9794940b6665d6cb292060a(x2665.o_shippriority, x2665.o_orderdate, x2665.o_custkey, x2665.o_orderpriority, x2671, x2665.o_clerk, x2665.o_orderstatus, x2665.o_totalprice, x2665.o_orderkey, x2665.o_comment))
 }).as[Record7c0e640ae9794940b6665d6cb292060a]
val x2668 = x2667
val Test1Full = x2668
//Test1Full.print
Test1Full.cache
Test1Full.count


def f = { 
 
 val x2693 = Test1Full.select("o_orderdate", "o_parts")
            .as[Record659b90479b7c4244a70a9243943935d3]
 
val x2696 = x2693.withColumn("index", monotonically_increasing_id())
 .as[Record5b0dcca28b5e49768a0dd1c3e6bf3a5f].flatMap{
   case x2713 => if (x2713.o_parts.isEmpty) Seq(Record8d5cadea9a4542c9b2c5a393373a1016(x2713.o_orderdate, None, None, x2713.index)) 
     else x2713.o_parts.map( x2714 => Record8d5cadea9a4542c9b2c5a393373a1016(x2713.o_orderdate, Some(x2714.l_quantity), Some(x2714.l_partkey), x2713.index) )
}.as[Record8d5cadea9a4542c9b2c5a393373a1016]
 
val x2698 = P.select("p_name", "p_partkey")
            .as[Record6ff2841e8f1443519f5d80a3284e7138]
 
val x2702 = x2696.equiJoin[Record6ff2841e8f1443519f5d80a3284e7138, Int](x2698, Seq("l_partkey","p_partkey"), "left_outer")
 .as[Record8d0642c629dd4fbeab6af048db2fd890]
 
val x2708 = x2702.reduceByKey(x2715 => 
 Recordc795d787adf04881a8782567e4047ae0(x2715.index, x2715.o_orderdate, x2715.p_name), x => x.l_quantity match {
       case Some(r) => r; case _ => 0.0
   }).mapPartitions(
     it => it.map{ case (x2715, x2716) => (Record18d0c7ef55c04eefb002eaf2ea2ebdc1(x2715.index, x2715.o_orderdate), Recordcfae396e1f314152907e01cd0fd3c73a(x2715.p_name, Some(x2716)))
 })
val x2711 = x2708.setGroups(_._1).mapGroups2{
  case (x92, x93) => 
    val grps = x93.map(_._2).toSeq
    Recordf21c0d70b2e747f7abf61acf61d66ef9(x92.o_orderdate, grps)
}.as[Recordf21c0d70b2e747f7abf61acf61d66ef9]

val x2712 = x2711
val Test1NN = x2712
//Test1NN.print
//Test1NN.cache
Test1NN.count


}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start 
   println("Flat++,Skew,1,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
