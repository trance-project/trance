
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
case class Record1cc445d93b8744a28ac24f2882947ec4(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Seq[Recordf2d079787ab94b6dadcf6338fd7f36b6], c_mktsegment: String, c_phone: String, index: Long)
case class Recordb39046c0a7484738bf64416ce18a324a(o_shippriority: Option[Int], c_acctbal: Double, o_orderdate: Option[String], o_custkey: Option[Int], o_orderpriority: Option[String], c_name: String, o_parts: Option[Seq[Recordcc04db435e0d4bef868932b8f4beed46]], c_nationkey: Int, o_clerk: Option[String], o_orderstatus: Option[String], c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, o_totalprice: Option[Double], o_orderkey: Option[Int], c_phone: String, o_comment: Option[String], index: Long)
case class Recordfff87ea9ec394d6e9da07da69133f73e(o_shippriority: Option[Int], c_acctbal: Double, o_orderdate: Option[String], o_custkey: Option[Int], l_quantity: Option[Double], o_orderpriority: Option[String], c_name: String, c_nationkey: Int, o_clerk: Option[String], o_orderstatus: Option[String], l_partkey: Option[Int], c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, o_totalprice: Option[Double], o_orderkey: Option[Int], c_phone: String, o_comment: Option[String], index: Long)
case class Record12e541671aa44b5f8f446de1907f620e(p_name: String, p_partkey: Int)
case class Record8d4d6bc4697d4bd18357ee7e732d6f97(o_shippriority: Option[Int], p_name: Option[String], c_acctbal: Double, o_orderdate: Option[String], o_custkey: Option[Int], l_quantity: Option[Double], o_orderpriority: Option[String], c_name: String, c_nationkey: Int, o_clerk: Option[String], o_orderstatus: Option[String], l_partkey: Option[Int], p_partkey: Option[Int], c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, o_totalprice: Option[Double], o_orderkey: Option[Int], c_phone: String, o_comment: Option[String], index: Long)
case class Recordd736840faf7b454f82c4fa5ce68faf1c(p_name: Option[String], l_quantity: Option[Double])
case class Record3c32fcdcbd7b42cf9d73909b8734adf6(o_shippriority: Option[Int], p_name: Option[String], c_acctbal: Double, o_orderdate: Option[String], o_custkey: Option[Int], o_orderpriority: Option[String], c_name: String, c_nationkey: Int, o_clerk: Option[String], o_orderstatus: Option[String], c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, o_totalprice: Option[Double], o_orderkey: Option[Int], c_phone: String, o_comment: Option[String], index: Long)
case class Recorda7a6e5a983aa4199a7280fb034c3a4c6(o_shippriority: Option[Int], c_acctbal: Double, o_orderdate: Option[String], o_custkey: Option[Int], o_orderpriority: Option[String], c_name: String, c_nationkey: Int, o_clerk: Option[String], o_orderstatus: Option[String], c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, o_totalprice: Option[Double], o_orderkey: Option[Int], c_phone: String, o_comment: Option[String], index: Long)
case class Record44ffe914372940d0915ac8d04e7a02bf(o_shippriority: Option[Int], p_name: Option[String], c_acctbal: Double, o_orderdate: Option[String], o_custkey: Option[Int], l_quantity: Option[Double], o_orderpriority: Option[String], c_name: String, c_nationkey: Int, o_clerk: Option[String], o_orderstatus: Option[String], c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, o_totalprice: Option[Double], o_orderkey: Option[Int], c_phone: String, o_comment: Option[String], index: Long)
case class Record0e4bfbe307d7439b866e6f25e6e76875(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String, index: Long)
case class Recordd9f66d25ce5a4f12b9191578f888d735(p_name: String, l_quantity: Double)
case class Recordb71ecb1ae054419ebaa999da1a6d97f5(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Seq[Recordd9f66d25ce5a4f12b9191578f888d735], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record8ce3a6c5af324abc9039b32d8a5a20df(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Seq[Recordb71ecb1ae054419ebaa999da1a6d97f5], c_mktsegment: String, c_phone: String)
case class Recordcc04db435e0d4bef868932b8f4beed46(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Recordf2d079787ab94b6dadcf6338fd7f36b6(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Seq[Recordcc04db435e0d4bef868932b8f4beed46], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record64d495938b0f45118b19bb0b269399c9(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Seq[Recordf2d079787ab94b6dadcf6338fd7f36b6], c_mktsegment: String, c_phone: String)
object Test2FullNNSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master)
     .setAppName("Test2FullNNSpark"+sf)
     .set("spark.sql.shuffle.partitions", Config.lparts.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   implicit val encoderRecordcc04db435e0d4bef868932b8f4beed46: Encoder[Recordcc04db435e0d4bef868932b8f4beed46] = Encoders.product[Recordcc04db435e0d4bef868932b8f4beed46]
implicit val encoderRecordf2d079787ab94b6dadcf6338fd7f36b6: Encoder[Recordf2d079787ab94b6dadcf6338fd7f36b6] = Encoders.product[Recordf2d079787ab94b6dadcf6338fd7f36b6]
implicit val encoderRecordd736840faf7b454f82c4fa5ce68faf1c: Encoder[Recordd736840faf7b454f82c4fa5ce68faf1c] = Encoders.product[Recordd736840faf7b454f82c4fa5ce68faf1c]
implicit val encoderRecordd9f66d25ce5a4f12b9191578f888d735: Encoder[Recordd9f66d25ce5a4f12b9191578f888d735] = Encoders.product[Recordd9f66d25ce5a4f12b9191578f888d735]
implicit val encoderRecordb71ecb1ae054419ebaa999da1a6d97f5: Encoder[Recordb71ecb1ae054419ebaa999da1a6d97f5] = Encoders.product[Recordb71ecb1ae054419ebaa999da1a6d97f5]
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
     val x43 = x42.map(x24 => Recordcc04db435e0d4bef868932b8f4beed46(x24.l_returnflag, x24.l_comment, x24.l_linestatus, x24.l_shipmode, x24.l_shipinstruct, x24.l_quantity, x24.l_receiptdate, x24.l_linenumber, x24.l_tax, x24.l_shipdate, x24.l_extendedprice, x24.l_partkey, x24.l_discount, x24.l_commitdate, x24.l_suppkey, x24.l_orderkey)).toSeq
     x41.map(x26 => Recordf2d079787ab94b6dadcf6338fd7f36b6(x26.o_shippriority, x26.o_orderdate, x26.o_custkey, x26.o_orderpriority, x43, x26.o_clerk, x26.o_orderstatus, x26.o_totalprice, x26.o_orderkey, x26.o_comment))
 }).as[Recordf2d079787ab94b6dadcf6338fd7f36b6]
val x29 = x28
val orders = x29
//orders.print
orders.cache
//orders.count
val x31 = C 
val x33 = orders 
val x39 = x31.cogroup(x33.groupByKey(x35 => x35.o_custkey), x34 => x34.c_custkey)(
   (_, x44, x45) => {
     val x46 = x45.map(x35 => Recordf2d079787ab94b6dadcf6338fd7f36b6(x35.o_shippriority, x35.o_orderdate, x35.o_custkey, x35.o_orderpriority, x35.o_parts, x35.o_clerk, x35.o_orderstatus, x35.o_totalprice, x35.o_orderkey, x35.o_comment)).toSeq
     x44.map(x37 => Record64d495938b0f45118b19bb0b269399c9(x37.c_acctbal, x37.c_name, x37.c_nationkey, x37.c_custkey, x37.c_comment, x37.c_address, x46, x37.c_mktsegment, x37.c_phone))
 }).as[Record64d495938b0f45118b19bb0b269399c9]
val x40 = x39
val Test2Full = x40
//Test2Full.print
Test2Full.cache
Test2Full.count




def f = { 
 
 val x79 = Test2Full 
val x82 = x79.withColumn("index", monotonically_increasing_id())
 .as[Record1cc445d93b8744a28ac24f2882947ec4].flatMap{
   case x110 => if (x110.c_orders.isEmpty) Seq(Recordb39046c0a7484738bf64416ce18a324a(None, x110.c_acctbal, None, None, None, x110.c_name, None, x110.c_nationkey, None, None, x110.c_custkey, x110.c_comment, x110.c_address, x110.c_mktsegment, None, None, x110.c_phone, None, x110.index)) 
     else x110.c_orders.map( x111 => Recordb39046c0a7484738bf64416ce18a324a(Some(x111.o_shippriority), x110.c_acctbal, Some(x111.o_orderdate), Some(x111.o_custkey), Some(x111.o_orderpriority), x110.c_name, Some(x111.o_parts), x110.c_nationkey, Some(x111.o_clerk), Some(x111.o_orderstatus), x110.c_custkey, x110.c_comment, x110.c_address, x110.c_mktsegment, Some(x111.o_totalprice), Some(x111.o_orderkey), x110.c_phone, Some(x111.o_comment), x110.index) )
}.as[Recordb39046c0a7484738bf64416ce18a324a]
 
val x86 = x82.withColumn("index", monotonically_increasing_id())
 .as[Recordb39046c0a7484738bf64416ce18a324a].flatMap{
   case x112 => x112.o_parts match {
     case None => Seq(Recordfff87ea9ec394d6e9da07da69133f73e(x112.o_shippriority, x112.c_acctbal, x112.o_orderdate, x112.o_custkey, None, x112.o_orderpriority, x112.c_name, x112.c_nationkey, x112.o_clerk, x112.o_orderstatus, None, x112.c_custkey, x112.c_comment, x112.c_address, x112.c_mktsegment, x112.o_totalprice, x112.o_orderkey, x112.c_phone, x112.o_comment, x112.index))
     case Some(bag) if bag.isEmpty => Seq(Recordfff87ea9ec394d6e9da07da69133f73e(x112.o_shippriority, x112.c_acctbal, x112.o_orderdate, x112.o_custkey, None, x112.o_orderpriority, x112.c_name, x112.c_nationkey, x112.o_clerk, x112.o_orderstatus, None, x112.c_custkey, x112.c_comment, x112.c_address, x112.c_mktsegment, x112.o_totalprice, x112.o_orderkey, x112.c_phone, x112.o_comment, x112.index))
     case Some(bag) => bag.map( x113 => Recordfff87ea9ec394d6e9da07da69133f73e(x112.o_shippriority, x112.c_acctbal, x112.o_orderdate, x112.o_custkey, Some(x113.l_quantity), x112.o_orderpriority, x112.c_name, x112.c_nationkey, x112.o_clerk, x112.o_orderstatus, Some(x113.l_partkey), x112.c_custkey, x112.c_comment, x112.c_address, x112.c_mktsegment, x112.o_totalprice, x112.o_orderkey, x112.c_phone, x112.o_comment, x112.index) )
 }}.as[Recordfff87ea9ec394d6e9da07da69133f73e]
 
val x88 = P.select("p_name", "p_partkey")
            .as[Record12e541671aa44b5f8f446de1907f620e]
 
val x93 = x86.equiJoin[Record12e541671aa44b5f8f446de1907f620e](x88, Seq("l_partkey","p_partkey"), "left_outer")
 .as[Record8d4d6bc4697d4bd18357ee7e732d6f97]
 
val x100 = x93.reduceByKey(x114 => 
 Record3c32fcdcbd7b42cf9d73909b8734adf6(x114.o_shippriority, x114.p_name, x114.c_acctbal, x114.o_orderdate, x114.o_custkey, x114.o_orderpriority, x114.c_name, x114.c_nationkey, x114.o_clerk, x114.o_orderstatus, x114.c_custkey, x114.c_comment, x114.c_address, x114.c_mktsegment, x114.o_totalprice, x114.o_orderkey, x114.c_phone, x114.o_comment, x114.index), x => x.l_quantity match {
       case Some(r) => r; case _ => 0.0
   }).mapPartitions(
     it => it.map{ case (x114, x115) => (Recorda7a6e5a983aa4199a7280fb034c3a4c6(x114.o_shippriority, x114.c_acctbal, x114.o_orderdate, x114.o_custkey, x114.o_orderpriority, x114.c_name, x114.c_nationkey, x114.o_clerk, x114.o_orderstatus, x114.c_custkey, x114.c_comment, x114.c_address, x114.c_mktsegment, x114.o_totalprice, x114.o_orderkey, x114.c_phone, x114.o_comment, x114.index), Recordd736840faf7b454f82c4fa5ce68faf1c(x114.p_name, Some(x115)))
 })

val x101 = x100.setGroups(_._1).mapGroups{
    case (x92, x93) => 
    val grps = x93.flatMap{
      x => x._2.p_name match {
        case None => Seq()
        case _ => Seq(Recordd9f66d25ce5a4f12b9191578f888d735(x._2.p_name.get, x._2.l_quantity.get))
      }
    }.toSeq
    (x92, grps)
}

val x105 = x101
 .setGroups(x116 =>
   Record0e4bfbe307d7439b866e6f25e6e76875(x116._1.c_acctbal,x116._1.c_name,x116._1.c_nationkey,x116._1.c_custkey,x116._1.c_comment,x116._1.c_address,x116._1.c_mktsegment,x116._1.c_phone,x116._1.index)).mapGroups{
   case (x117, group) => 
     val ngroup = group.flatMap{
       x116 => x116._1.o_shippriority match {
         case None => Seq()
         case _ => Seq(Recordb71ecb1ae054419ebaa999da1a6d97f5(x116._1.o_shippriority.get,x116._1.o_orderdate.get,x116._1.o_custkey.get,x116._1.o_orderpriority.get,x116._2,x116._1.o_clerk.get,x116._1.o_orderstatus.get,x116._1.o_totalprice.get,x116._1.o_orderkey.get,x116._1.o_comment.get))
        }
     }.toSeq
   (x117, ngroup)
}
 
val x108 = x105.mapPartitions{ it => it.map{
 case (x106,x107) => Record8ce3a6c5af324abc9039b32d8a5a20df(x106.c_acctbal, x106.c_name, x106.c_nationkey, x106.c_custkey, x106.c_comment, x106.c_address, x107, x106.c_mktsegment, x106.c_phone)
}}.as[Record8ce3a6c5af324abc9039b32d8a5a20df]
 
val x109 = x108
val Test2FullNN = x109
//Test2FullNN.print
//Test2FullNN.cache
  Test2FullNN.count

}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start 
   println("Flat++,2,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
