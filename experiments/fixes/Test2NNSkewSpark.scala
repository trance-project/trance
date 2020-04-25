
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
case class Recordc0bf377e6fda42aca8cb958250f41f97(c_name: String, c_orders: Seq[Recordf44c333c0ab246ab85deb911acf1fa89])
case class Record9c3e23d8d67d476f8a6a5ed79c1175a8(c_name: String, c_orders: Seq[Recordf44c333c0ab246ab85deb911acf1fa89], index: Long)
case class Recordbc219a097a26475bb96d477c46ac05d6(c_name: String, o_orderdate: Option[String], o_parts: Option[Seq[Record2783d3f2698140c1a4f52a9ea5a2c304]], index: Long)
case class Recordc0fd559b89d148498d5c1519cf82319b(index: Long, c_name: String, o_orderdate: Option[String], o_parts: Option[Seq[Record2783d3f2698140c1a4f52a9ea5a2c304]])
case class Record37923ac6c9e8405ca776031becdd9e08(o_orderdate: Option[String], l_quantity: Option[Double], c_name: String, l_partkey: Option[Int], index: Long)
case class Recorda629fddc05bd49d8a1f300d85ef4f87c(p_name: String, p_partkey: Int)
case class Record478e002b194c43538a74ddb7a3d84af9(p_name: Option[String], o_orderdate: Option[String], l_quantity: Option[Double], c_name: String, l_partkey: Option[Int], p_partkey: Option[Int], index: Long)
case class Record71fe643713f94c3eadd565cd4b4a872e(p_name: Option[String], l_quantity: Option[Double])
case class Record05dfc9143d6b45deb21e7e9abb028aee(index: Long, c_name: String, o_orderdate: Option[String], p_name: Option[String])
case class Recordf858c4795b6a4601912bd25dc93011ad(index: Long, c_name: String, o_orderdate: Option[String])
case class Recordedb3cfd7ec1d438eb1b384f9d707f899(p_name: Option[String], o_orderdate: Option[String], l_quantity: Option[Double], c_name: String, index: Long)
case class Record46e10df2e49a46a89a68c7ca7db3029b(index: Long, c_name: String)
case class Record960ddd621fa248fcb250579d83001bfd(p_name: String, l_quantity: Double)
case class Record257ff94fccdf456dbd71132d71e8295a(o_orderdate: String, o_parts: Seq[Record960ddd621fa248fcb250579d83001bfd])
case class Record52ea61ca724a4a9781c78f91e4ba9453(c_name: String, c_orders: Seq[Record257ff94fccdf456dbd71132d71e8295a])
case class Record2783d3f2698140c1a4f52a9ea5a2c304(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Recordf44c333c0ab246ab85deb911acf1fa89(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Seq[Record2783d3f2698140c1a4f52a9ea5a2c304], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Recordac0bd7be2f074de297f7d7cbe64ad344(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Seq[Recordf44c333c0ab246ab85deb911acf1fa89], c_mktsegment: String, c_phone: String)
object Test2NNSkewSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master)
     .setAppName("Test2NNSkewSpark"+sf)
     .set("spark.sql.shuffle.partitions", Config.lparts.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   implicit val encoderRecord2783d3f2698140c1a4f52a9ea5a2c304: Encoder[Record2783d3f2698140c1a4f52a9ea5a2c304] = Encoders.product[Record2783d3f2698140c1a4f52a9ea5a2c304]
implicit val encoderRecordf44c333c0ab246ab85deb911acf1fa89: Encoder[Recordf44c333c0ab246ab85deb911acf1fa89] = Encoders.product[Recordf44c333c0ab246ab85deb911acf1fa89]
implicit val encoderRecord71fe643713f94c3eadd565cd4b4a872e: Encoder[Record71fe643713f94c3eadd565cd4b4a872e] = Encoders.product[Record71fe643713f94c3eadd565cd4b4a872e]
implicit val encoderRecord960ddd621fa248fcb250579d83001bfd: Encoder[Record960ddd621fa248fcb250579d83001bfd] = Encoders.product[Record960ddd621fa248fcb250579d83001bfd]
implicit val encoderRecord257ff94fccdf456dbd71132d71e8295a: Encoder[Record257ff94fccdf456dbd71132d71e8295a] = Encoders.product[Record257ff94fccdf456dbd71132d71e8295a]
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
val C_L = tpch.loadCustomerDF()
C_L.cache
C_L.count
val C_H = Seq.empty[Customer].toDS()
val C = (C_L, C_H)
val O_L = tpch.loadOrderDF()
O_L.cache
O_L.count
val O_H = Seq.empty[Order].toDS()
val O = (O_L, O_H)

   val x2668 = O 
val x2670 = L 
val x2676 = x2668.cogroup(x2670.groupByKey(x2672 => x2672.l_orderkey), x2671 => x2671.o_orderkey)(
   (_, x2689, x2690) => {
     val x2691 = x2690.map(x2672 => Record2783d3f2698140c1a4f52a9ea5a2c304(x2672.l_returnflag, x2672.l_comment, x2672.l_linestatus, x2672.l_shipmode, x2672.l_shipinstruct, x2672.l_quantity, x2672.l_receiptdate, x2672.l_linenumber, x2672.l_tax, x2672.l_shipdate, x2672.l_extendedprice, x2672.l_partkey, x2672.l_discount, x2672.l_commitdate, x2672.l_suppkey, x2672.l_orderkey)).toSeq
     x2689.map(x2674 => Recordf44c333c0ab246ab85deb911acf1fa89(x2674.o_shippriority, x2674.o_orderdate, x2674.o_custkey, x2674.o_orderpriority, x2691, x2674.o_clerk, x2674.o_orderstatus, x2674.o_totalprice, x2674.o_orderkey, x2674.o_comment))
 }).as[Recordf44c333c0ab246ab85deb911acf1fa89]
val x2677 = x2676
val orders = x2677
//orders.print
orders.cache
//orders.count
val x2679 = C 
val x2681 = orders 
val x2687 = x2679.cogroup(x2681.groupByKey(x2683 => x2683.o_custkey), x2682 => x2682.c_custkey)(
   (_, x2692, x2693) => {
     val x2694 = x2693.map(x2683 => Recordf44c333c0ab246ab85deb911acf1fa89(x2683.o_shippriority, x2683.o_orderdate, x2683.o_custkey, x2683.o_orderpriority, x2683.o_parts, x2683.o_clerk, x2683.o_orderstatus, x2683.o_totalprice, x2683.o_orderkey, x2683.o_comment)).toSeq
     x2692.map(x2685 => Recordac0bd7be2f074de297f7d7cbe64ad344(x2685.c_acctbal, x2685.c_name, x2685.c_nationkey, x2685.c_custkey, x2685.c_comment, x2685.c_address, x2694, x2685.c_mktsegment, x2685.c_phone))
 }).as[Recordac0bd7be2f074de297f7d7cbe64ad344]
val x2688 = x2687
val Test2Full = x2688
//Test2Full.print
Test2Full.cache
Test2Full.count




def f = { 
 
 val x2727 = Test2Full.select("c_name", "c_orders")
            .as[Recordc0bf377e6fda42aca8cb958250f41f97]
 
val x2730 = x2727.withColumn("index", monotonically_increasing_id())
 .as[Record9c3e23d8d67d476f8a6a5ed79c1175a8].flatMap{
   case x2758 => if (x2758.c_orders.isEmpty) Seq(Recordbc219a097a26475bb96d477c46ac05d6(x2758.c_name, None, None, x2758.index)) 
     else x2758.c_orders.map( x2759 => Recordbc219a097a26475bb96d477c46ac05d6(x2758.c_name, Some(x2759.o_orderdate), Some(x2759.o_parts), x2758.index) )
}.as[Recordbc219a097a26475bb96d477c46ac05d6]
 
val x2734 = x2730.withColumn("index", monotonically_increasing_id())
 .as[Recordc0fd559b89d148498d5c1519cf82319b].flatMap{
   case x2760 => x2760.o_parts match {
     case None => Seq(Record37923ac6c9e8405ca776031becdd9e08(x2760.o_orderdate, None, x2760.c_name, None, x2760.index))
     case Some(bag) if bag.isEmpty => Seq(Record37923ac6c9e8405ca776031becdd9e08(x2760.o_orderdate, None, x2760.c_name, None, x2760.index))
     case Some(bag) => bag.map( x2761 => Record37923ac6c9e8405ca776031becdd9e08(x2760.o_orderdate, Some(x2761.l_quantity), x2760.c_name, Some(x2761.l_partkey), x2760.index) )
 }}.as[Record37923ac6c9e8405ca776031becdd9e08]
 
val x2736 = P.select("p_name", "p_partkey")
            .as[Recorda629fddc05bd49d8a1f300d85ef4f87c]
 
val x2741 = x2734.equiJoin[Recorda629fddc05bd49d8a1f300d85ef4f87c, Int](x2736, Seq("l_partkey","p_partkey"), "left_outer")
 .as[Record478e002b194c43538a74ddb7a3d84af9]
 
val x2748 = x2741.reduceByKey(x2762 => 
 Record05dfc9143d6b45deb21e7e9abb028aee(x2762.index, x2762.c_name, x2762.o_orderdate, x2762.p_name), x => x.l_quantity match {
       case Some(r) => r; case _ => 0.0
   }).mapPartitions(
     it => it.map{ case (x2762, x2763) => (Recordf858c4795b6a4601912bd25dc93011ad(x2762.index, x2762.c_name, x2762.o_orderdate), Record71fe643713f94c3eadd565cd4b4a872e(x2762.p_name, Some(x2763)))
 })

val x2749 = x2748.setGroups(_._1).mapGroups{
    case (x92, x93) => 
    val grps = x93.flatMap{
      x => x._2.p_name match {
        case None => Seq()
        case _ => Seq(Record960ddd621fa248fcb250579d83001bfd(x._2.p_name.get, x._2.l_quantity.get))
      }
    }.toSeq
    (x92, grps)
}

val x2753 = x2749
 .setGroups(x2764 =>
   Record46e10df2e49a46a89a68c7ca7db3029b(x2764._1.index,x2764._1.c_name)).mapGroups{
   case (x2765, group) => 
     val ngroup = group.flatMap{
       x2764 => x2764._1.o_orderdate match {
         case None => Seq()
         case _ => Seq(Record257ff94fccdf456dbd71132d71e8295a(x2764._1.o_orderdate.get, x2764._2))
        }
     }.toSeq
   (x2765, ngroup)
}
 
val x2756 = x2753.mapPartitions{ it => it.map{
 case (x2754,x2755) => Record52ea61ca724a4a9781c78f91e4ba9453(x2754.c_name, x2755)
}}.as[Record52ea61ca724a4a9781c78f91e4ba9453]
 
val x2757 = x2756
val Test2NN = x2757
//Test2NN.print
Test2NN.count


}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start 
   println("Flat++,Skew,2,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
