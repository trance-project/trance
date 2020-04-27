
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
case class Record0754effbeb924353a90298957302ca11(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Seq[Record3c338a6b55864fa8a85450fc8d745dc6], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String, index: Long)
case class Recordb68ecae152784f0a8a1b65af70f229cb(o_shippriority: Int, o_orderdate: String, o_custkey: Int, l_quantity: Option[Double], o_orderpriority: String, o_clerk: String, o_orderstatus: String, l_partkey: Option[Int], o_totalprice: Double, o_orderkey: Int, o_comment: String, index: Long)
case class Record06df90c768324199a6601b0230657241(p_name: String, p_partkey: Int)
case class Recorda07a97a8c33945c9a7743e0637080915(o_shippriority: Int, p_name: Option[String], o_orderdate: String, o_custkey: Int, l_quantity: Option[Double], o_orderpriority: String, o_clerk: String, o_orderstatus: String, l_partkey: Option[Int], p_partkey: Option[Int], o_totalprice: Double, o_orderkey: Int, o_comment: String, index: Long)
case class Record2510cc409daa499e8ee37fde93e2c559(p_name: Option[String], l_quantity: Option[Double])
case class Recordbb83b9431e9b49ffb93ca015ef4b53e2(o_shippriority: Int, p_name: Option[String], o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String, index: Long)
case class Record5712557655c34cfab8fd3d1a4301abf4(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String, index: Long)
case class Recordfaaed82c4b44420f969d68adb1dc1071(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Seq[Record2510cc409daa499e8ee37fde93e2c559], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record3c338a6b55864fa8a85450fc8d745dc6(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Recordb9df289500da4340be434ba2522a6c5f(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Seq[Record3c338a6b55864fa8a85450fc8d745dc6], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
object Test1FullNNSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master)
     .setAppName("Test1FullNNSpark"+sf)
     .set("spark.sql.shuffle.partitions", Config.lparts.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   implicit val encoderRecord3c338a6b55864fa8a85450fc8d745dc6: Encoder[Record3c338a6b55864fa8a85450fc8d745dc6] = Encoders.product[Record3c338a6b55864fa8a85450fc8d745dc6]
implicit val encoderRecord2510cc409daa499e8ee37fde93e2c559: Encoder[Record2510cc409daa499e8ee37fde93e2c559] = Encoders.product[Record2510cc409daa499e8ee37fde93e2c559]
   import spark.implicits._
   val tpch = TPCHLoader(spark)
val L = tpch.loadLineitemDF()
L.cache
L.count
val P = tpch.loadPartDF()
P.cache
P.count
val O = tpch.loadOrderDF()
O.cache
O.count

   val x11 = O 
val x13 = L 
val x19 = x11.cogroup(x13.groupByKey(x15 => x15.l_orderkey), x14 => x14.o_orderkey)(
   (_, x21, x22) => {
     val x23 = x22.map(x15 => Record3c338a6b55864fa8a85450fc8d745dc6(x15.l_returnflag, x15.l_comment, x15.l_linestatus, x15.l_shipmode, x15.l_shipinstruct, x15.l_quantity, x15.l_receiptdate, x15.l_linenumber, x15.l_tax, x15.l_shipdate, x15.l_extendedprice, x15.l_partkey, x15.l_discount, x15.l_commitdate, x15.l_suppkey, x15.l_orderkey)).toSeq
     x21.map(x17 => Recordb9df289500da4340be434ba2522a6c5f(x17.o_shippriority, x17.o_orderdate, x17.o_custkey, x17.o_orderpriority, x23, x17.o_clerk, x17.o_orderstatus, x17.o_totalprice, x17.o_orderkey, x17.o_comment))
 }).as[Recordb9df289500da4340be434ba2522a6c5f]
val x20 = x19
val Test1Full = x20
//Test1Full.print
Test1Full.cache
Test1Full.count


def f = { 
 
 val x45 = Test1Full 
val x48 = x45.withColumn("index", monotonically_increasing_id())
 .as[Record0754effbeb924353a90298957302ca11].flatMap{
   case x65 => if (x65.o_parts.isEmpty) Seq(Recordb68ecae152784f0a8a1b65af70f229cb(x65.o_shippriority, x65.o_orderdate, x65.o_custkey, None, x65.o_orderpriority, x65.o_clerk, x65.o_orderstatus, None, x65.o_totalprice, x65.o_orderkey, x65.o_comment, x65.index)) 
     else x65.o_parts.map( x66 => Recordb68ecae152784f0a8a1b65af70f229cb(x65.o_shippriority, x65.o_orderdate, x65.o_custkey, Some(x66.l_quantity), x65.o_orderpriority, x65.o_clerk, x65.o_orderstatus, Some(x66.l_partkey), x65.o_totalprice, x65.o_orderkey, x65.o_comment, x65.index) )
}.as[Recordb68ecae152784f0a8a1b65af70f229cb]
 
val x50 = P.select("p_name", "p_partkey")
            .as[Record06df90c768324199a6601b0230657241]
 
val x54 = x48.equiJoin[Record06df90c768324199a6601b0230657241](x50, Seq("l_partkey","p_partkey"), "left_outer")
 .as[Recorda07a97a8c33945c9a7743e0637080915]
 
val x60 = x54.reduceByKey(x67 => 
 Recordbb83b9431e9b49ffb93ca015ef4b53e2(x67.o_shippriority, x67.p_name, x67.o_orderdate, x67.o_custkey, x67.o_orderpriority, x67.o_clerk, x67.o_orderstatus, x67.o_totalprice, x67.o_orderkey, x67.o_comment, x67.index), x => x.l_quantity match {
       case Some(r) => r; case _ => 0.0
   }).mapPartitions(
     it => it.map{ case (x67, x68) => (Record5712557655c34cfab8fd3d1a4301abf4(x67.o_shippriority, x67.o_orderdate, x67.o_custkey, x67.o_orderpriority, x67.o_clerk, x67.o_orderstatus, x67.o_totalprice, x67.o_orderkey, x67.o_comment, x67.index), Record2510cc409daa499e8ee37fde93e2c559(x67.p_name, Some(x68)))
 }).groupByKey(_._1)

val x63 = x60.mapGroups{
    case (x61,x62) => 
    val grps = x62.flatMap{
      x => x._2.p_name match {
        case None => Seq()
        case _ => Seq(x._2)
      }
    }.toSeq
    Recordfaaed82c4b44420f969d68adb1dc1071(x61.o_shippriority, x61.o_orderdate, x61.o_custkey, x61.o_orderpriority, grps, x61.o_clerk, x61.o_orderstatus, x61.o_totalprice, x61.o_orderkey, x61.o_comment)
}.as[Recordfaaed82c4b44420f969d68adb1dc1071]

val x64 = x63
val Test1FullNN = x64
//Test1FullNN.print
//Test1FullNN.cache
Test1FullNN.count

}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start 
   println("Flat++,1,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
