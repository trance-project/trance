
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record297(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record298(p_name: String, p_partkey: Int)
case class Record299(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record300(c_name: String, c_custkey: Int)
case class Record301(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record303(p_name: String, l_qty: Double)
case class Record305(o_orderdate: String, o_parts: Iterable[Record303])
case class Record306(c_name: String, c_orders: Iterable[Record305])
object Query1Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query1Spark"+sf)
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

   def f = { 
 val x229 = L.map(x224 => { val x225 = x224.l_orderkey 
val x226 = x224.l_quantity 
val x227 = x224.l_partkey 
val x228 = Record297(x225, x226, x227) 
x228 }) 
val x234 = P.map(x230 => { val x231 = x230.p_name 
val x232 = x230.p_partkey 
val x233 = Record298(x231, x232) 
x233 }) 
val x239 = { val out1 = x229.map{ case x235 => ({val x237 = x235.l_partkey 
x237}, x235) }
  val out2 = x234.map{ case x236 => ({val x238 = x236.p_partkey 
x238}, x236) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x246 = x239.map{ case (x240, x241) => 
   val x242 = x240.l_orderkey 
val x243 = x241.p_name 
val x244 = x240.l_quantity 
val x245 = Record299(x242, x243, x244) 
x245 
} 
val ljp = x246
val x247 = ljp
//ljp.collect.foreach(println(_))
val x252 = C.map(x248 => { val x249 = x248.c_name 
val x250 = x248.c_custkey 
val x251 = Record300(x249, x250) 
x251 }) 
val x258 = O.map(x253 => { val x254 = x253.o_orderdate 
val x255 = x253.o_orderkey 
val x256 = x253.o_custkey 
val x257 = Record301(x254, x255, x256) 
x257 }) 
val x263 = { val out1 = x252.map{ case x259 => ({val x261 = x259.c_custkey 
x261}, x259) }
  val out2 = x258.map{ case x260 => ({val x262 = x260.o_custkey 
x262}, x260) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x265 = ljp 
val x271 = { val out1 = x263.map{ case (x266, x267) => ({val x269 = x267.o_orderkey 
x269}, (x266, x267)) }
  val out2 = x265.map{ case x268 => ({val x270 = x268.l_orderkey 
x270}, x268) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x281 = x271.flatMap{ case ((x272, x273), x274) => val x280 = (x274) 
x280 match {
   case (null) => Nil 
   case x279 => List(({val x275 = (x272,x273) 
x275}, {val x276 = x274.p_name 
val x277 = x274.l_qty 
val x278 = Record303(x276, x277) 
x278}))
 }
}.groupByKey() 
val x290 = x281.flatMap{ case ((x282, x283), x284) => val x289 = (x283,x284) 
x289 match {
   case (_,null) => Nil 
   case x288 => List(({val x285 = (x282) 
x285}, {val x286 = x283.o_orderdate 
val x287 = Record305(x286, x284) 
x287}))
 }
}.groupByKey() 
val x295 = x290.map{ case (x291, x292) => 
   val x293 = x291.c_name 
val x294 = Record306(x293, x292) 
x294 
} 
x295.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("Query1Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
