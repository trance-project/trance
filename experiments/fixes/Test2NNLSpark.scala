
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
case class Record991a55f5eab342fda1831d91e748abac(c_name: String, c_orders: Seq[Recordcfbcb5f2489949db90f2051d51f8633f], index: Long)
case class Record2394f4543a954386b7cde32c6a2cd1fc(c_name: String, o_orderdate: Option[String], o_parts: Option[Seq[Record61ad3834daad481a89784d2d0985ab24]], index: Long)
case class Recordd97d2a4f22d34e6da40c2546209f0835(index: Long, c_name: String, o_orderdate: Option[String], o_parts: Option[Seq[Record61ad3834daad481a89784d2d0985ab24]])
case class Recordfc91e836de05428f8d7fdaa878c29038(o_orderdate: Option[String], l_quantity: Option[Double], c_name: String, l_partkey: Option[Int], index: Long)
case class Record455a2871dd3d40e58832af6a45e9e393(p_name: String, p_partkey: Int)
case class Record26acffae3f904ed99fb1f12b7b43aca4(p_name: Option[String], o_orderdate: Option[String], l_quantity: Option[Double], c_name: String, l_partkey: Option[Int], p_partkey: Option[Int], index: Long)
case class Record535ae9c95d344bac8d9adf3c6d2c68f6(p_name: Option[String], l_quantity: Option[Double])
case class Record4f1fd328bf85404e9f3eda22a364a4dc(index: Long, c_name: String, o_orderdate: Option[String], p_name: Option[String])
case class Recordeb43b384c9fe45f59a4e78951713b509(index: Long, c_name: String, o_orderdate: Option[String])
case class Record694b9cfe7681421abde05badded4d2f7(p_name: Option[String], o_orderdate: Option[String], l_quantity: Option[Double], c_name: String, index: Long)
case class Record518d1472ddd0488c888ed3cbd5c4fa9a(index: Long, c_name: String)
case class Recordddb52937494f4467aa5cdc6a240e6d6a(p_name: String, l_quantity: Double)
case class Recordb09bb034d5d24151a6427f0a4cfb1093(o_orderdate: String, o_parts: Seq[Recordddb52937494f4467aa5cdc6a240e6d6a])
case class Record3a3a5dd269f445de9d7f75f627cf53f3(c_name: String, c_orders: Seq[Recordb09bb034d5d24151a6427f0a4cfb1093])
case class Recordf2292dbfe057450288bbfa8b6de13a0c(c_name: String, c_custkey: Int)
case class Record6ab4d2d5929946168a5404a09c5fea6f(o_custkey: Int, o_orderdate: String, o_orderkey: Int)
case class Record79bb195616ce4f86a01ff0607e973e75(c_name: String, c_orders: Seq[Recordcfbcb5f2489949db90f2051d51f8633f])
case class Record61ad3834daad481a89784d2d0985ab24(l_partkey: Int, l_quantity: Double)
case class Recorda49d7aa4147d458093a3a33228780b95(l_partkey: Int, l_quantity: Double, l_orderkey: Int)
case class Recordbc8854815c2049c295168246becf8905(o_custkey: Int, o_orderdate: String, o_parts: Seq[Record61ad3834daad481a89784d2d0985ab24])
case class Recordcfbcb5f2489949db90f2051d51f8633f(o_orderdate: String, o_parts: Seq[Record61ad3834daad481a89784d2d0985ab24])
object Test2NNLSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master)
     .setAppName("Test2NNLSpark"+sf)
     .set("spark.sql.shuffle.partitions", Config.lparts.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   implicit val encoderRecord61ad3834daad481a89784d2d0985ab24: Encoder[Record61ad3834daad481a89784d2d0985ab24] = Encoders.product[Record61ad3834daad481a89784d2d0985ab24]
implicit val encoderRecordcfbcb5f2489949db90f2051d51f8633f: Encoder[Recordcfbcb5f2489949db90f2051d51f8633f] = Encoders.product[Recordcfbcb5f2489949db90f2051d51f8633f]
implicit val encoderRecord535ae9c95d344bac8d9adf3c6d2c68f6: Encoder[Record535ae9c95d344bac8d9adf3c6d2c68f6] = Encoders.product[Record535ae9c95d344bac8d9adf3c6d2c68f6]
implicit val encoderRecordddb52937494f4467aa5cdc6a240e6d6a: Encoder[Recordddb52937494f4467aa5cdc6a240e6d6a] = Encoders.product[Recordddb52937494f4467aa5cdc6a240e6d6a]
implicit val encoderRecordb09bb034d5d24151a6427f0a4cfb1093: Encoder[Recordb09bb034d5d24151a6427f0a4cfb1093] = Encoders.product[Recordb09bb034d5d24151a6427f0a4cfb1093]
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

   val x2736 = O.select("o_custkey", "o_orderdate", "o_orderkey")
            .as[Record6ab4d2d5929946168a5404a09c5fea6f]
 
val x2738 = L.select("l_partkey", "l_quantity", "l_orderkey")
            .as[Recorda49d7aa4147d458093a3a33228780b95]
 
val x2744 = x2736.cogroup(x2738.groupByKey(x2740 => x2740.l_orderkey), x2739 => x2739.o_orderkey)(
   (_, x2757, x2758) => {
     val x2759 = x2758.map(x2740 => Record61ad3834daad481a89784d2d0985ab24(x2740.l_partkey, x2740.l_quantity)).toSeq
     x2757.map(x2742 => Recordbc8854815c2049c295168246becf8905(x2742.o_custkey, x2742.o_orderdate, x2759))
 }).as[Recordbc8854815c2049c295168246becf8905]
val x2745 = x2744
val orders = x2745
//orders.print
orders.cache
//orders.count
val x2747 = C.select("c_name", "c_custkey")
            .as[Recordf2292dbfe057450288bbfa8b6de13a0c]
 
val x2749 = orders 
val x2755 = x2747.cogroup(x2749.groupByKey(x2751 => x2751.o_custkey), x2750 => x2750.c_custkey)(
   (_, x2760, x2761) => {
     val x2762 = x2761.map(x2751 => Recordcfbcb5f2489949db90f2051d51f8633f(x2751.o_orderdate, x2751.o_parts)).toSeq
     x2760.map(x2753 => Record79bb195616ce4f86a01ff0607e973e75(x2753.c_name, x2762))
 }).as[Record79bb195616ce4f86a01ff0607e973e75]
val x2756 = x2755
val Test2 = x2756
//Test2.print
Test2.cache
Test2.count




def f = { 
 
 val x2795 = Test2 
val x2798 = x2795.withColumn("index", monotonically_increasing_id())
 .as[Record991a55f5eab342fda1831d91e748abac].flatMap{
   case x2826 => if (x2826.c_orders.isEmpty) Seq(Record2394f4543a954386b7cde32c6a2cd1fc(x2826.c_name, None, None, x2826.index)) 
     else x2826.c_orders.map( x2827 => Record2394f4543a954386b7cde32c6a2cd1fc(x2826.c_name, Some(x2827.o_orderdate), Some(x2827.o_parts), x2826.index) )
}.as[Record2394f4543a954386b7cde32c6a2cd1fc]
 
val x2802 = x2798.withColumn("index", monotonically_increasing_id())
 .as[Recordd97d2a4f22d34e6da40c2546209f0835].flatMap{
   case x2828 => x2828.o_parts match {
     case None => Seq(Recordfc91e836de05428f8d7fdaa878c29038(x2828.o_orderdate, None, x2828.c_name, None, x2828.index))
     case Some(bag) if bag.isEmpty => Seq(Recordfc91e836de05428f8d7fdaa878c29038(x2828.o_orderdate, None, x2828.c_name, None, x2828.index))
     case Some(bag) => bag.map( x2829 => Recordfc91e836de05428f8d7fdaa878c29038(x2828.o_orderdate, Some(x2829.l_quantity), x2828.c_name, Some(x2829.l_partkey), x2828.index) )
 }}.as[Recordfc91e836de05428f8d7fdaa878c29038]
 
val x2804 = P.select("p_name", "p_partkey")
            .as[Record455a2871dd3d40e58832af6a45e9e393]
 
val x2809 = x2802.equiJoin[Record455a2871dd3d40e58832af6a45e9e393](x2804, Seq("l_partkey","p_partkey"), "left_outer")
 .as[Record26acffae3f904ed99fb1f12b7b43aca4]
 
val x2816 = x2809.reduceByKey(x2830 => 
 Record4f1fd328bf85404e9f3eda22a364a4dc(x2830.index, x2830.c_name, x2830.o_orderdate, x2830.p_name), x => x.l_quantity match {
       case Some(r) => r; case _ => 0.0
   }).mapPartitions(
     it => it.map{ case (x2830, x2831) => (Recordeb43b384c9fe45f59a4e78951713b509(x2830.index, x2830.c_name, x2830.o_orderdate), Record535ae9c95d344bac8d9adf3c6d2c68f6(x2830.p_name, Some(x2831)))
 })

val x2934 = x2816.setGroups(_._1).mapGroups{
    case (x92, x93) => 
    val grps = x93.flatMap{
      x => x._2.p_name match {
        case None => Seq()
        case _ => Seq(Recordddb52937494f4467aa5cdc6a240e6d6a(x._2.p_name.get, x._2.l_quantity.get))
      }
    }.toSeq
    (x92, grps)
}

val x2821 = x2934
 .setGroups(x2832 =>
   Record518d1472ddd0488c888ed3cbd5c4fa9a(x2832._1.index,x2832._1.c_name)).mapGroups{
   case (x2833, group) => 
     val ngroup = group.flatMap{
       x2832 => x2832._1.o_orderdate match {
         case None => Seq()
         case _ => Seq(Recordb09bb034d5d24151a6427f0a4cfb1093(x2832._1.o_orderdate.get,x2832._2))
        }
     }.toSeq
   (x2833, ngroup)
}
 
val x2824 = x2821.mapPartitions{ it => it.map{
 case (x2822,x2823) => Record3a3a5dd269f445de9d7f75f627cf53f3(x2822.c_name, x2823)
}}.as[Record3a3a5dd269f445de9d7f75f627cf53f3]
 
val x2825 = x2824
val Test2NNL = x2825
//Test2NNL.print
//Test2NNL.cache
Test2NNL.count


}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start 
   println("Flat++,Standard,2,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
