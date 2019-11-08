
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record324(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record325(p_name: String, p_partkey: Int)
case class Record326(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record327(c_name: String, c_custkey: Int)
case class Record328(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record330(p_name: String, l_qty: Double)
case class Record332(o_orderdate: String, o_parts: Iterable[Record330])
case class Record333(c_name: String, c_orders: Iterable[Record332])
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
 val x256 = L.map(x251 => { val x252 = x251.l_orderkey 
val x253 = x251.l_quantity 
val x254 = x251.l_partkey 
val x255 = Record324(x252, x253, x254) 
x255 }) 
val x261 = P.map(x257 => { val x258 = x257.p_name 
val x259 = x257.p_partkey 
val x260 = Record325(x258, x259) 
x260 }) 
val x266 = { val out1 = x256.map{ case x262 => ({val x264 = x262.l_partkey 
x264}, x262) }
  val out2 = x261.map{ case x263 => ({val x265 = x263.p_partkey 
x265}, x263) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x273 = x266.map{ case (x267, x268) => 
   val x269 = x267.l_orderkey 
val x270 = x268.p_name 
val x271 = x267.l_quantity 
val x272 = Record326(x269, x270, x271) 
x272 
} 
val ljp = x273
val x274 = ljp
//ljp.collect.foreach(println(_))
val x279 = C.map(x275 => { val x276 = x275.c_name 
val x277 = x275.c_custkey 
val x278 = Record327(x276, x277) 
x278 }) 
val x285 = O.map(x280 => { val x281 = x280.o_orderdate 
val x282 = x280.o_orderkey 
val x283 = x280.o_custkey 
val x284 = Record328(x281, x282, x283) 
x284 }) 
val x290 = { val out1 = x279.map{ case x286 => ({val x288 = x286.c_custkey 
x288}, x286) }
  val out2 = x285.map{ case x287 => ({val x289 = x287.o_custkey 
x289}, x287) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x292 = ljp 
val x298 = { val out1 = x290.map{ case (x293, x294) => ({val x296 = x294.o_orderkey 
x296}, (x293, x294)) }
  val out2 = x292.map{ case x295 => ({val x297 = x295.l_orderkey 
x297}, x295) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x308 = x298.flatMap{ case ((x299, x300), x301) => val x307 = (x301) 
x307 match {
   case (null) => Nil 
   case x306 => List(({val x302 = (x299,x300) 
x302}, {val x303 = x301.p_name 
val x304 = x301.l_qty 
val x305 = Record330(x303, x304) 
x305}))
 }
}.groupByKey() 
val x317 = x308.flatMap{ case ((x309, x310), x311) => val x316 = (x310,x311) 
x316 match {
   case (_,null) => Nil 
   case x315 => List(({val x312 = (x309) 
x312}, {val x313 = x310.o_orderdate 
val x314 = Record332(x313, x311) 
x314}))
 }
}.groupByKey() 
val x322 = x317.map{ case (x318, x319) => 
   val x320 = x318.c_name 
val x321 = Record333(x320, x319) 
x321 
} 
x322.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("Query1Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
