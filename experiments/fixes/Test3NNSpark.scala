
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
case class Record0fd3540a104942058664e57d69420fe3(n_name: String, n_custs: Seq[Recorde23133e514a844ac807637edbe9b5007])
case class Record1d28ab741a084689b2889dd05fd44ad7(n_name: String, n_custs: Seq[Recorde23133e514a844ac807637edbe9b5007], index: Long)
case class Record2f52c28e036f48ac9e4a19f2aea119f2(n_name: String, c_name: Option[String], c_orders: Option[Seq[Recordd0e3b4511efc4d01816783a53e6c4990]], index: Long)
case class Record358d5222973049e49979565b40481af5(index: Long, index1: Long, n_name: String, c_name: Option[String], c_orders: Option[Seq[Recordd0e3b4511efc4d01816783a53e6c4990]])
case class Record57f45d08c2684e438750f631b6a24dbd(o_orderdate: Option[String], c_name: Option[String], o_parts: Option[Seq[Record42c13ccfb96945e7990ac8d8192f2e6a]], n_name: String, index: Long, index1: Long)
case class Record57f45d08c2684e438750f631b6a24dbdA(o_orderdate: Option[String], c_name: Option[String], o_parts: Option[Seq[Record42c13ccfb96945e7990ac8d8192f2e6a]], n_name: String, index: Long, index1: Long, index2: Long)
case class Record5ee9f7ab681c4cc5866e17b8f04b35a5(o_orderdate: Option[String], l_quantity: Option[Double], c_name: Option[String], n_name: String, l_partkey: Option[Int], index: Long, index1: Long, index2: Long)
case class Recorddf6e28f411ef4b28ada4cfa230c72dde(p_name: String, p_partkey: Int)
case class Record6250663d38fc40dea19978ca88bd171c(p_name: Option[String], o_orderdate: Option[String], l_quantity: Option[Double], c_name: Option[String], n_name: String, l_partkey: Option[Int], p_partkey: Option[Int], index: Long, index1: Long, index2: Long)
case class Recorde62b4ecc4c1c46ff8ee045569bbf258e(p_name: Option[String], l_quantity: Option[Double])
case class Record4f9cd145e58f436093807b190c00902e(p_name: Option[String], o_orderdate: Option[String], c_name: Option[String], n_name: String, index: Long, index1: Long, index2: Long)
case class Recordfe8a0cf5994c41329fd51308e3e5e21a(o_orderdate: Option[String], c_name: Option[String], n_name: String, index: Long, index1: Long, index2: Long)
case class Record272c16b906454fa6a9614b259a1c8a9e(p_name: Option[String], o_orderdate: Option[String], l_quantity: Option[Double], c_name: Option[String], n_name: String, index: Long, index1: Long, index2: Long)
case class Record770824482bbc4a59bdab1da7e7256cb2(index: Long, index1: Long, n_name: String, c_name: Option[String])
case class Record6065b7e8011847668f4b29570b1b337d(p_name: String, l_quantity: Double)
case class Recordb6fb98f29da847f2ba649bf2167f183a(o_orderdate: String, o_parts: Seq[Record6065b7e8011847668f4b29570b1b337d])
case class Record8b54e42d1bd141738108b4000e31324f(index: Long, n_name: String)
case class Record99e1b70c95244b15afb7ed8bb595f356(c_name: String, c_orders: Seq[Recordb6fb98f29da847f2ba649bf2167f183a])
case class Record4f51f68a1f0645159b82a9a34ce9b1a3(n_name: String, n_custs: Seq[Record99e1b70c95244b15afb7ed8bb595f356])
case class Record42c13ccfb96945e7990ac8d8192f2e6a(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Recordd0e3b4511efc4d01816783a53e6c4990(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Seq[Record42c13ccfb96945e7990ac8d8192f2e6a], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Recorde23133e514a844ac807637edbe9b5007(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Seq[Recordd0e3b4511efc4d01816783a53e6c4990], c_mktsegment: String, c_phone: String)
case class Record39ad03e37f27414faa98b10d7a81ad75(n_regionkey: Int, n_nationkey: Int, n_custs: Seq[Recorde23133e514a844ac807637edbe9b5007], n_name: String, n_comment: String)
object Test3NNSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master)
     .setAppName("Test3NNSpark"+sf)
     .set("spark.sql.shuffle.partitions", Config.lparts.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   implicit val encoderRecord42c13ccfb96945e7990ac8d8192f2e6a: Encoder[Record42c13ccfb96945e7990ac8d8192f2e6a] = Encoders.product[Record42c13ccfb96945e7990ac8d8192f2e6a]
implicit val encoderRecordd0e3b4511efc4d01816783a53e6c4990: Encoder[Recordd0e3b4511efc4d01816783a53e6c4990] = Encoders.product[Recordd0e3b4511efc4d01816783a53e6c4990]
implicit val encoderRecorde23133e514a844ac807637edbe9b5007: Encoder[Recorde23133e514a844ac807637edbe9b5007] = Encoders.product[Recorde23133e514a844ac807637edbe9b5007]
implicit val encoderRecorde62b4ecc4c1c46ff8ee045569bbf258e: Encoder[Recorde62b4ecc4c1c46ff8ee045569bbf258e] = Encoders.product[Recorde62b4ecc4c1c46ff8ee045569bbf258e]
implicit val encoderRecord6065b7e8011847668f4b29570b1b337d: Encoder[Record6065b7e8011847668f4b29570b1b337d] = Encoders.product[Record6065b7e8011847668f4b29570b1b337d]
implicit val encoderRecordb6fb98f29da847f2ba649bf2167f183a: Encoder[Recordb6fb98f29da847f2ba649bf2167f183a] = Encoders.product[Recordb6fb98f29da847f2ba649bf2167f183a]
implicit val encoderRecord99e1b70c95244b15afb7ed8bb595f356: Encoder[Record99e1b70c95244b15afb7ed8bb595f356] = Encoders.product[Record99e1b70c95244b15afb7ed8bb595f356]
   import spark.implicits._
   val tpch = TPCHLoader(spark)
val N = tpch.loadNationDF()
N.cache
N.count
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

   val x29 = O 
val x31 = L 
val x37 = x29.cogroup(x31.groupByKey(x33 => x33.l_orderkey), x32 => x32.o_orderkey)(
   (_, x61, x62) => {
     val x63 = x62.map(x33 => Record42c13ccfb96945e7990ac8d8192f2e6a(x33.l_returnflag, x33.l_comment, x33.l_linestatus, x33.l_shipmode, x33.l_shipinstruct, x33.l_quantity, x33.l_receiptdate, x33.l_linenumber, x33.l_tax, x33.l_shipdate, x33.l_extendedprice, x33.l_partkey, x33.l_discount, x33.l_commitdate, x33.l_suppkey, x33.l_orderkey)).toSeq
     x61.map(x35 => Recordd0e3b4511efc4d01816783a53e6c4990(x35.o_shippriority, x35.o_orderdate, x35.o_custkey, x35.o_orderpriority, x63, x35.o_clerk, x35.o_orderstatus, x35.o_totalprice, x35.o_orderkey, x35.o_comment))
 }).as[Recordd0e3b4511efc4d01816783a53e6c4990]
val x38 = x37
val orders = x38
//orders.print
orders.cache
//orders.count
val x40 = C 
val x42 = orders 
val x48 = x40.cogroup(x42.groupByKey(x44 => x44.o_custkey), x43 => x43.c_custkey)(
   (_, x64, x65) => {
     val x66 = x65.map(x44 => Recordd0e3b4511efc4d01816783a53e6c4990(x44.o_shippriority, x44.o_orderdate, x44.o_custkey, x44.o_orderpriority, x44.o_parts, x44.o_clerk, x44.o_orderstatus, x44.o_totalprice, x44.o_orderkey, x44.o_comment)).toSeq
     x64.map(x46 => Recorde23133e514a844ac807637edbe9b5007(x46.c_acctbal, x46.c_name, x46.c_nationkey, x46.c_custkey, x46.c_comment, x46.c_address, x66, x46.c_mktsegment, x46.c_phone))
 }).as[Recorde23133e514a844ac807637edbe9b5007]
val x49 = x48
val customers = x49
//customers.print
customers.cache
//customers.count
val x51 = N 
val x53 = customers 
val x59 = x51.cogroup(x53.groupByKey(x55 => x55.c_nationkey), x54 => x54.n_nationkey)(
   (_, x67, x68) => {
     val x69 = x68.map(x55 => Recorde23133e514a844ac807637edbe9b5007(x55.c_acctbal, x55.c_name, x55.c_nationkey, x55.c_custkey, x55.c_comment, x55.c_address, x55.c_orders, x55.c_mktsegment, x55.c_phone)).toSeq
     x67.map(x57 => Record39ad03e37f27414faa98b10d7a81ad75(x57.n_regionkey, x57.n_nationkey, x69, x57.n_name, x57.n_comment))
 }).as[Record39ad03e37f27414faa98b10d7a81ad75]
val x60 = x59
val Test3Full = x60
//Test3Full.print
Test3Full.cache
Test3Full.count






def f = { 
 
 val x115 = Test3Full.select("n_name", "n_custs")
            .as[Record0fd3540a104942058664e57d69420fe3]
 
val x118 = x115.withColumn("index", monotonically_increasing_id())
 .as[Record1d28ab741a084689b2889dd05fd44ad7].flatMap{
   case x159 => if (x159.n_custs.isEmpty) Seq(Record2f52c28e036f48ac9e4a19f2aea119f2(x159.n_name, None, None, x159.index)) 
     else x159.n_custs.map( x160 => Record2f52c28e036f48ac9e4a19f2aea119f2(x159.n_name, Some(x160.c_name), Some(x160.c_orders), x159.index) )
}.as[Record2f52c28e036f48ac9e4a19f2aea119f2]

val x122 = x118.withColumn("index1", monotonically_increasing_id())
 .as[Record358d5222973049e49979565b40481af5].flatMap{
   case x161 => x161.c_orders match {
     case None => Seq(Record57f45d08c2684e438750f631b6a24dbd(None, x161.c_name, None, x161.n_name, x161.index, x161.index1))
     case Some(bag) if bag.isEmpty => Seq(Record57f45d08c2684e438750f631b6a24dbd(None, x161.c_name, None, x161.n_name, x161.index, x161.index1))
     case Some(bag) => bag.map( x162 => Record57f45d08c2684e438750f631b6a24dbd(Some(x162.o_orderdate), x161.c_name, Some(x162.o_parts), x161.n_name, x161.index, x161.index1) )
 }}.as[Record57f45d08c2684e438750f631b6a24dbd]

val x127 = x122.withColumn("index2", monotonically_increasing_id())
 .as[Record57f45d08c2684e438750f631b6a24dbdA].flatMap{
   case x163 => x163.o_parts match {
     case None => Seq(Record5ee9f7ab681c4cc5866e17b8f04b35a5(x163.o_orderdate, None, x163.c_name, x163.n_name, None, x163.index, x163.index1, x163.index2))
     case Some(bag) if bag.isEmpty => Seq(Record5ee9f7ab681c4cc5866e17b8f04b35a5(x163.o_orderdate, None, x163.c_name, x163.n_name, None, x163.index, x163.index1, x163.index2))
     case Some(bag) => bag.map( x164 => Record5ee9f7ab681c4cc5866e17b8f04b35a5(x163.o_orderdate, Some(x164.l_quantity), x163.c_name, x163.n_name, Some(x164.l_partkey), x163.index, x163.index1, x163.index2) )
 }}.as[Record5ee9f7ab681c4cc5866e17b8f04b35a5]

val x129 = P.select("p_name", "p_partkey")
            .as[Recorddf6e28f411ef4b28ada4cfa230c72dde]
 
val x135 = x127.equiJoin[Recorddf6e28f411ef4b28ada4cfa230c72dde](x129, Seq("l_partkey","p_partkey"), "left_outer")
 .as[Record6250663d38fc40dea19978ca88bd171c]
 
val x143 = x135.reduceByKey(x165 => 
 Record4f9cd145e58f436093807b190c00902e(x165.p_name, x165.o_orderdate, x165.c_name, x165.n_name, x165.index, x165.index1, x165.index2), x => x.l_quantity match {
       case Some(r) => r; case _ => 0.0
   }).mapPartitions(
     it => it.map{ case (x165, x166) => (Recordfe8a0cf5994c41329fd51308e3e5e21a(x165.o_orderdate, x165.c_name, x165.n_name, x165.index, x165.index1, x165.index2), Recorde62b4ecc4c1c46ff8ee045569bbf258e(x165.p_name, Some(x166)))
 })

val x144 = x143.setGroups(_._1).mapGroups{
    case (x92, x93) => 
    val grps = x93.flatMap{
      x => x._2.p_name match {
        case None => Seq()
        case _ => Seq(Record6065b7e8011847668f4b29570b1b337d(x._2.p_name.get, x._2.l_quantity.get))
      }
    }.toSeq
    (x92, grps)
}

val x149 = x144
 .setGroups(x167 =>
   Record770824482bbc4a59bdab1da7e7256cb2(x167._1.index, x167._1.index1,x167._1.n_name,x167._1.c_name)).mapGroups{
   case (x168, group) => 
     val ngroup = group.flatMap{
       x167 => x167._1.o_orderdate match {
         case None => Seq()
         case _ => Seq(Recordb6fb98f29da847f2ba649bf2167f183a(x167._1.o_orderdate.get, x167._2))
        }
     }.toSeq
   (x168, ngroup)
}
 
val x154 = x149
 .setGroups(x169 =>
   Record8b54e42d1bd141738108b4000e31324f(x169._1.index,x169._1.n_name)).mapGroups{
   case (x170, group) => 
     val ngroup = group.flatMap{
       x169 => x169._1.c_name match {
         case None => Seq()
         case _ => Seq(Record99e1b70c95244b15afb7ed8bb595f356(x169._1.c_name.get,x169._2.map(x152 => Recordb6fb98f29da847f2ba649bf2167f183a(x152.o_orderdate,x152.o_parts))))
        }
     }.toSeq
   (x170, ngroup)
}
 
val x157 = x154.mapPartitions{ it => it.map{
 case (x155,x156) => Record4f51f68a1f0645159b82a9a34ce9b1a3(x155.n_name, x156)
}}.as[Record4f51f68a1f0645159b82a9a34ce9b1a3]
 
val x158 = x157
val Test3NN = x158
// Test3NN.print
//Test3NN.cache
Test3NN.count

}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start 
   println("Flat++,3,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
