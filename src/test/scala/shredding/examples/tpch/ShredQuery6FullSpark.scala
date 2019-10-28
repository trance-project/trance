
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record399(s_name: String, s_suppkey: Int)
case class Record400(s__Fs_suppkey: Int)
case class Record401(s_name: String, customers2: Record400)
case class Record402(l_suppkey: Int, l_orderkey: Int)
case class Record403(o_custkey: Int, o_orderkey: Int)
case class Record404(c_name: String, c_custkey: Int)
case class Record406(c_name2: String)
case class Record407(_1: Int, _2: Iterable[Record406])
case class Record445(c_name: String)
case class Record446(c__Fc_name: String)
case class Record447(c_name: String, suppliers: Record446)
case class Record448(s_name: String)
case class Record449(_1: String, _2: Iterable[Record448])
object ShredQuery6FullSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6FullSpark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L__F = 3
val L__D_1 = tpch.loadLineitem
L__D_1.cache
L__D_1.count
val C__F = 1
val C__D_1 = tpch.loadCustomers
C__D_1.cache
C__D_1.count
val O__F = 2
val O__D_1 = tpch.loadOrders
O__D_1.cache
O__D_1.count
val S__F = 6
val S__D_1 = tpch.loadSupplier
S__D_1.cache
S__D_1.count

//   val Query2Full = {
 val x347 = S__D_1.map(x343 => { val x344 = x343.s_name 
val x345 = x343.s_suppkey 
val x346 = Record399(x344, x345) 
x346 }) 
val x353 = x347.map{ case x348 => 
   val x349 = x348.s_name 
val x350 = x348.s_suppkey 
val x351 = Record400(x350) 
val x352 = Record401(x349, x351) 
x352 
} 
val M_flat1 = x353
val x354 = M_flat1
//M_flat1.collect.foreach(println(_))
val x359 = L__D_1.map(x355 => { val x356 = x355.l_suppkey 
val x357 = x355.l_orderkey 
val x358 = Record402(x356, x357) 
x358 }) 
val x360 = O__D_1 
val x365 = x360.map(x361 => { val x362 = x361.o_custkey 
val x363 = x361.o_orderkey 
val x364 = Record403(x362, x363) 
x364 }) 
val x370 = { val out1 = x359.map{ case x366 => ({val x368 = x366.l_orderkey 
x368}, x366) }
  val out2 = x365.map{ case x367 => ({val x369 = x367.o_orderkey 
x369}, x367) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x371 = C__D_1 
val x376 = x371.map(x372 => { val x373 = x372.c_name 
val x374 = x372.c_custkey 
val x375 = Record404(x373, x374) 
x375 }) 
val x382 = { val out1 = x370.map{ case (x377, x378) => ({val x380 = x378.o_custkey 
x380}, (x377, x378)) }
  val out2 = x376.map{ case x379 => ({val x381 = x379.c_custkey 
x381}, x379) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x391 = x382.flatMap{ case ((x383, x384), x385) => val x390 = (x384,x385) 
x390 match {
   case (_,null) => Nil 
   case x389 => List(({val x386 = (x383) 
x386}, {val x387 = x385.c_name 
val x388 = Record406(x387) 
x388}))
 }
}.groupByKey() 
val x396 = x391.map{ case (x392, x393) => 
   val x394 = x392.l_suppkey 
val x395 = Record407(x394, x393) 
x395 
} 
val M_flat2 = x396
val x397 = M_flat2
//M_flat2.collect.foreach(println(_))
//x397
//}

val Query2Full__D_1 = M_flat1
Query2Full__D_1.cache
Query2Full__D_1.count
val Query2Full__D_2customers2_1 = M_flat2
Query2Full__D_2customers2_1.cache
Query2Full__D_2customers2_1.count

def f = { 
 val x417 = C__D_1.map(x414 => { val x415 = x414.c_name 
val x416 = Record445(x415) 
x416 }) 
val x422 = x417.map{ case x418 => 
   val x419 = x418.c_name 
val x420 = Record446(x419) 
val x421 = Record447(x419, x420) 
x421 
} 
val M_flat1 = x422
val x423 = M_flat1
//M_flat1.collect.foreach(println(_))
val x425 = Query2Full__D_1 
val x427 = Query2Full__D_2customers2_1 
val x430 = x427 
val x434 = { val out1 = x425.map{ case x431 => ({val x433 = x431.customers2 
x433}, x431) }
  val out2 = x430.flatMap(x432 => x432._2.map{case v2 => (Record400(x432._1), v2)})
  out1.join(out2).map{ case (k, v) => v }
} 
val x442 = x434.map{ case (x435, x436) => 
   val x437 = x436.c_name2 
val x438 = x435.s_name 
val x439 = Record448(x438) 
val x440 = List(x439) 
val x441 = Record449(x437, x440) 
x441 
} 
val M_flat2 = x442
val x443 = M_flat2
//M_flat2.collect.foreach(println(_))
x443.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery6FullSpark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
