
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record298(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record299(p_name: String, p_partkey: Int)
case class Record300(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record301(c_name: String, c_custkey: Int)
case class Record302(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record304(p_name: String, l_qty: Double)
case class Record306(o_orderdate: String, o_parts: Iterable[Record304])
case class Record307(c_name: String, c_orders: Iterable[Record306])
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
 val x230 = L.map(x225 => { val x226 = x225.l_orderkey 
val x227 = x225.l_quantity 
val x228 = x225.l_partkey 
val x229 = Record298(x226, x227, x228) 
x229 }) 
val x235 = P.map(x231 => { val x232 = x231.p_name 
val x233 = x231.p_partkey 
val x234 = Record299(x232, x233) 
x234 }) 
val x240 = { val out1 = x230.map{ case x236 => ({val x238 = x236.l_partkey 
x238}, x236) }
  val out2 = x235.map{ case x237 => ({val x239 = x237.p_partkey 
x239}, x237) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x247 = x240.map{ case (x241, x242) => 
   val x243 = x241.l_orderkey 
val x244 = x242.p_name 
val x245 = x241.l_quantity 
val x246 = Record300(x243, x244, x245) 
x246 
} 
val ljp = x247
val x248 = ljp
//ljp.collect.foreach(println(_))
val x253 = C.map(x249 => { val x250 = x249.c_name 
val x251 = x249.c_custkey 
val x252 = Record301(x250, x251) 
x252 }) 
val x259 = O.map(x254 => { val x255 = x254.o_orderdate 
val x256 = x254.o_orderkey 
val x257 = x254.o_custkey 
val x258 = Record302(x255, x256, x257) 
x258 }) 
val x264 = { val out1 = x253.map{ case x260 => ({val x262 = x260.c_custkey 
x262}, x260) }
  val out2 = x259.map{ case x261 => ({val x263 = x261.o_custkey 
x263}, x261) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x266 = ljp 
val x272 = { val out1 = x264.map{ case (x267, x268) => ({val x270 = x268.o_orderkey 
x270}, (x267, x268)) }
  val out2 = x266.map{ case x269 => ({val x271 = x269.l_orderkey 
x271}, x269) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x282 = x272.flatMap{ case ((x273, x274), x275) => val x281 = (x275) 
x281 match {
   case (null) => Nil 
   case x280 => List(({val x276 = (x273,x274) 
x276}, {val x277 = x275.p_name 
val x278 = x275.l_qty 
val x279 = Record304(x277, x278) 
x279}))
 }
}.groupByKey() 
val x291 = x282.flatMap{ case ((x283, x284), x285) => val x290 = (x284,x285) 
x290 match {
   case (_,null) => Nil 
   case x289 => List(({val x286 = (x283) 
x286}, {val x287 = x284.o_orderdate 
val x288 = Record306(x287, x285) 
x288}))
 }
}.groupByKey() 
val x296 = x291.map{ case (x292, x293) => 
   val x294 = x292.c_name 
val x295 = Record307(x294, x293) 
x295 
} 
x296.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("Query1Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
