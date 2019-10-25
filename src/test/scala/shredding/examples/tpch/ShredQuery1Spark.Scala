
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record393(p_partkey: Int, p_name: String)
case class Record394(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record395(key: Int, value: Iterable[Record394])
case class Record396(c__Fc_custkey: Int)
case class Record397(c_name: String, c_orders: Record396)
case class Record398(o__Fo_orderkey: Int)
case class Record399(o_orderdate: String, o_parts: Record398)
case class Record400(p_name: String, l_qty: Double)
object ShredQuery1Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1Spark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L__F = 3
val L__D_1 = tpch.loadLineitem
L__D_1.cache
L__D_1.count
val P__F = 4
val P__D_1 = tpch.loadPart
P__D_1.cache
P__D_1.count
val C__F = 1
val C__D_1 = tpch.loadCustomers
C__D_1.cache
C__D_1.count
val O__F = 2
val O__D_1 = tpch.loadOrders
O__D_1.cache
O__D_1.count

   def f = { 
 val x315 = L__D_1 
val x317 = x315 
val x321 = x317.flatMap{ case x318 => x318 match {
   case null => List((x318, null))
   case _ =>
   val x319 = x318._2 
x319 match {
     case x320 => x320.map{ case v2 => (x318, v2) }
  }
 }} 
val x322 = P__D_1 
val x327 = x322.map(x323 => { val x324 = x323.p_partkey 
val x325 = x323.p_name 
val x326 = Record393(x324, x325) 
x326 }) 
val x331 = x327.map{ case c => (x321.head, c) } 
val x342 = x331.map{ case ((x332, x333), x334) => 
   val x335 = x334.p_partkey 
val x336 = x333.l_orderkey 
val x337 = x334.p_name 
val x338 = x333.l_quantity 
val x339 = Record394(x336, x337, x338) 
val x340 = List(x339) 
val x341 = Record395(x335, x340) 
x341 
} 
val ljp__D_1 = x342
val x343 = ljp__D_1
//ljp__D_1.collect.foreach(println(_))
val x344 = C__D_1 
val x346 = x344 
val x350 = x346.flatMap{ case x347 => x347 match {
   case null => List((x347, null))
   case _ =>
   val x348 = x347._2 
x348 match {
     case x349 => x349.map{ case v2 => (x347, v2) }
  }
 }} 
val x357 = x350.map{ case (x351, x352) => 
   val x353 = x352.c_name 
val x354 = x352.c_custkey 
val x355 = Record396(x354) 
val x356 = Record397(x353, x355) 
x356 
} 
val M_flat1 = x357
val x358 = M_flat1
//M_flat1.collect.foreach(println(_))
val x359 = O__D_1 
val x361 = x359 
val x365 = x361.flatMap{ case x362 => x362 match {
   case null => List((x362, null))
   case _ =>
   val x363 = x362._2 
x363 match {
     case x364 => x364.map{ case v2 => (x362, v2) }
  }
 }} 
val x374 = x365.map{ case (x366, x367) => 
   val x368 = x367.o_orderdate 
val x369 = x367.o_orderkey 
val x370 = Record398(x369) 
val x371 = Record399(x368, x370) 
x371 
}.filter((x366, x367) => {val x372 = x367.o_custkey 
val x373 = x372 == c__Fc_custkey 
x373}) 
val M_flat2 = x374
val x375 = M_flat2
//M_flat2.collect.foreach(println(_))
val x376 = ljp__D_1 
val x378 = x376 
val x382 = x378.flatMap{ case x379 => x379 match {
   case null => List((x379, null))
   case _ =>
   val x380 = x379._2 
x380 match {
     case x381 => x381.map{ case v2 => (x379, v2) }
  }
 }} 
val x390 = x382.map{ case (x383, x384) => 
   val x385 = x384.p_name 
val x386 = x384.l_qty 
val x387 = Record400(x385, x386) 
x387 
}.filter((x383, x384) => {val x388 = x384.l_orderkey 
val x389 = x388 == o__Fo_orderkey 
x389}) 
val M_flat3 = x390
val x391 = M_flat3
//M_flat3.collect.foreach(println(_))
x391.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0
   println("ShredQuery1Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
