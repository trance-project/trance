
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record382(c_name: String, c_custkey: Int)
case class Record383(o_orderkey: Int, o_orderdate: String, o_custkey: Int)
case class Record385(o_orderkey: Int, o_orderdate: String)
case class Record386(c_name: String, c_orders: Iterable[Record385])
case class Record464(l_quantity: Double, l_orderkey: Int, l_partkey: Int)
case class Record465(p_name: String, p_partkey: Int)
case class Record467(l_orderkey: Int, p_name: String)
case class Record469(c_name: String, o_orderdate: String, p_name: String)
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
 val x356 = C.map(x352 => { val x353 = x352.c_name 
val x354 = x352.c_custkey 
val x355 = Record382(x353, x354) 
x355 }) 
val x362 = O.map(x357 => { val x358 = x357.o_orderkey 
val x359 = x357.o_orderdate 
val x360 = x357.o_custkey 
val x361 = Record383(x358, x359, x360) 
x361 }) 
val x367 = { val out1 = x356.map{ case x363 => ({val x365 = x363.c_custkey 
x365}, x363) }
  val out2 = x362.map{ case x364 => ({val x366 = x364.o_custkey 
x366}, x364) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x376 = x367.map{ case (x368, x369) => val x375 = (x369) 
x375 match {
   case (null) => ({val x370 = (x368) 
x370}, null) 
   case x374 => ({val x370 = (x368) 
x370}, {val x371 = x369.o_orderkey 
val x372 = x369.o_orderdate 
val x373 = Record385(x371, x372) 
x373})
 }
}.groupByKey() 
val x381 = x376.map{ case (x377, x378) => 
   val x379 = x377.c_name 
val x380 = Record386(x379, x378) 
x380 
} 
x381
}
CustOrders.cache
CustOrders.count
def f = { 
 val x417 = L.map(x412 => { val x413 = x412.l_quantity 
val x414 = x412.l_orderkey 
val x415 = x412.l_partkey 
val x416 = Record464(x413, x414, x415) 
x416 }) 
val x422 = P.map(x418 => { val x419 = x418.p_name 
val x420 = x418.p_partkey 
val x421 = Record465(x419, x420) 
x421 }) 
val x427 = { val out1 = x417.map{ case x423 => ({val x425 = x423.l_partkey 
x425}, x423) }
  val out2 = x422.map{ case x424 => ({val x426 = x424.p_partkey 
x426}, x424) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x436 = x427.map{ case (x428, x429) => 
   ({val x430 = x428.l_orderkey 
val x431 = x429.p_name 
val x432 = Record467(x430, x431) 
x432}, {val x433 = x428.l_quantity 
x433})
}.reduceByKey(_ + _) 
val partcnts = x436
val x437 = partcnts
//partcnts.collect.foreach(println(_))
val x439 = CustOrders 
val x443 = x439.flatMap{ case x440 => x440 match {
   case null => List((x440, null))
   case _ =>
   val x441 = x440.c_orders 
x441 match {
     case x442 => x442.map{ case v2 => (x440, v2) }
  }
 }} 
val x445 = partcnts 
val x451 = { val out1 = x443.map{ case (x446, x447) => ({val x449 = x447.o_orderkey 
x449}, (x446, x447)) }
  val out2 = x445.map{ case x448 => ({val x450 = x448.l_orderkey 
x450}, x448) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x462 = x451.map{ case ((x452, x453), x454) => 
   ({val x455 = x452.c_name 
val x456 = x453.o_orderdate 
val x457 = x454.p_name 
val x458 = Record469(x455, x456, x457) 
x458}, {val x459 = x454._2 
x459})
}.reduceByKey(_ + _) 
x462.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis()
   println("Query4Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
