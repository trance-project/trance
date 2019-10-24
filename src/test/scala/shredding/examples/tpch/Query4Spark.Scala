
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record365(c_name: String, c_custkey: Int)
case class Record366(o_orderkey: Int, o_orderdate: String, o_custkey: Int)
case class Record368(o_orderkey: Int, o_orderdate: String)
case class Record369(c_name: String, c_orders: Iterable[Record368])
case class Record431(l_quantity: Double, l_orderkey: Int, l_partkey: Int)
case class Record432(p_name: String, p_partkey: Int)
case class Record434(l_orderkey: Int, p_name: String)
case class Record436(c_name: String, o_orderdate: String, p_name: String)
object Query4Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query4Spark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L = tpch.loadLineitem
L.cache
L.count
val P = tpch.loadPart
P.cache
P.count
val C = tpch.loadCustomers
C.cache
C.count
val O = tpch.loadOrders
O.cache
O.count

   val CustOrders = {
 val x339 = C.map(x335 => { val x336 = x335.c_name 
val x337 = x335.c_custkey 
val x338 = Record365(x336, x337) 
x338 }) 
val x345 = O.map(x340 => { val x341 = x340.o_orderkey 
val x342 = x340.o_orderdate 
val x343 = x340.o_custkey 
val x344 = Record366(x341, x342, x343) 
x344 }) 
val x350 = { val out1 = x339.map{ case x346 => ({val x348 = x346.c_custkey 
x348}, x346) }
  val out2 = x345.map{ case x347 => ({val x349 = x347.o_custkey 
x349}, x347) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x359 = x350.map{ case (x351, x352) => val x358 = (x352) 
x358 match {
   case (null) => ({val x353 = (x351) 
x353}, null) 
   case x357 => ({val x353 = (x351) 
x353}, {val x354 = x352.o_orderkey 
val x355 = x352.o_orderdate 
val x356 = Record368(x354, x355) 
x356})
 }
}.groupByKey() 
val x364 = x359.map{ case (x360, x361) => 
   val x362 = x360.c_name 
val x363 = Record369(x362, x361) 
x363 
} 
x364
}
CustOrders.cache
CustOrders.count
def f = { 
 val x384 = L.map(x379 => { val x380 = x379.l_quantity 
val x381 = x379.l_orderkey 
val x382 = x379.l_partkey 
val x383 = Record431(x380, x381, x382) 
x383 }) 
val x389 = P.map(x385 => { val x386 = x385.p_name 
val x387 = x385.p_partkey 
val x388 = Record432(x386, x387) 
x388 }) 
val x394 = { val out1 = x384.map{ case x390 => ({val x392 = x390.l_partkey 
x392}, x390) }
  val out2 = x389.map{ case x391 => ({val x393 = x391.p_partkey 
x393}, x391) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x403 = x394.map{ case (x395, x396) => 
   ({val x397 = x395.l_orderkey 
val x398 = x396.p_name 
val x399 = Record434(x397, x398) 
x399}, {val x400 = x395.l_quantity 
x400})
}.reduceByKey(_ + _) 
val partcnts = x403
val x404 = partcnts
//partcnts.collect.foreach(println(_))
val x406 = CustOrders 
val x410 = x406.flatMap{ case x407 => x407 match {
   case null => List((x407, null))
   case _ =>
   val x408 = x407.c_orders 
x408 match {
     case x409 => x409.map{ case v2 => (x407, v2) }
  }
 }} 
val x412 = partcnts 
val x418 = { val out1 = x410.map{ case (x413, x414) => ({val x416 = x414.o_orderkey 
x416}, (x413, x414)) }
  val out2 = x412.map{ case x415 => ({val x417 = x415.l_orderkey 
x417}, x415) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x429 = x418.map{ case ((x419, x420), x421) => 
   ({val x422 = x419.c_name 
val x423 = x420.o_orderdate 
val x424 = x421.p_name 
val x425 = Record436(x422, x423, x424) 
x425}, {val x426 = x421._2 
x426})
}.reduceByKey(_ + _) 
x429.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis()
   println("Query4Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
