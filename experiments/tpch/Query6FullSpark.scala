
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record318(o_orderkey: Int, o_custkey: Int)
case class Record319(c_name: String, c_custkey: Int)
case class Record320(o_orderkey: Int, c_name: String)
case class Record321(s_name: String, s_suppkey: Int)
case class Record322(l_orderkey: Int, l_suppkey: Int)
case class Record324(c_name2: String)
case class Record325(s_name: String, customers2: Iterable[Record324])
case class Record374(c_name: String)
case class Record376(s_name: String)
case class Record377(c_name: String, suppliers: Iterable[Record376])
object Query6FullSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query6FullSpark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L = tpch.loadLineitem
L.cache
L.count
val C = tpch.loadCustomers
C.cache
C.count
val O = tpch.loadOrders
O.cache
O.count
val S = tpch.loadSupplier
S.cache
S.count

   val Query2 = {
 val x262 = O.map(x258 => { val x259 = x258.o_orderkey 
val x260 = x258.o_custkey 
val x261 = Record318(x259, x260) 
x261 }) 
val x267 = C.map(x263 => { val x264 = x263.c_name 
val x265 = x263.c_custkey 
val x266 = Record319(x264, x265) 
x266 }) 
val x272 = { val out1 = x262.map{ case x268 => ({val x270 = x268.o_custkey 
x270}, x268) }
  val out2 = x267.map{ case x269 => ({val x271 = x269.c_custkey 
x271}, x269) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x278 = x272.map{ case (x273, x274) => 
   val x275 = x273.o_orderkey 
val x276 = x274.c_name 
val x277 = Record320(x275, x276) 
x277 
} 
val resultInner = x278
val x279 = resultInner
//resultInner.collect.foreach(println(_))
val x284 = S.map(x280 => { val x281 = x280.s_name 
val x282 = x280.s_suppkey 
val x283 = Record321(x281, x282) 
x283 }) 
val x289 = L.map(x285 => { val x286 = x285.l_orderkey 
val x287 = x285.l_suppkey 
val x288 = Record322(x286, x287) 
x288 }) 
val x294 = { val out1 = x284.map{ case x290 => ({val x292 = x290.s_suppkey 
x292}, x290) }
  val out2 = x289.map{ case x291 => ({val x293 = x291.l_suppkey 
x293}, x291) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x296 = resultInner 
val x302 = { val out1 = x294.map{ case (x297, x298) => ({val x300 = x298.l_orderkey 
x300}, (x297, x298)) }
  val out2 = x296.map{ case x299 => ({val x301 = x299.o_orderkey 
x301}, x299) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x311 = x302.flatMap{ case ((x303, x304), x305) => val x310 = (x304,x305) 
x310 match {
   case (_,null) => Nil 
   case x309 => List(({val x306 = (x303) 
x306}, {val x307 = x305.c_name 
val x308 = Record324(x307) 
x308}))
 }
}.groupByKey() 
val x316 = x311.map{ case (x312, x313) => 
   val x314 = x312.s_name 
val x315 = Record325(x314, x313) 
x315 
} 
x316
}
Query2.cache
Query2.count
def f = { 
 val x346 = C.map(x343 => { val x344 = x343.c_name 
val x345 = Record374(x344) 
x345 }) 
val x348 = Query2 
val x351 = { val out1 = x346.map{ case x349 => ({true}, x349) }
  val out2 = x348.map{ case x350 => ({true}, x350) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x356 = x351.flatMap{ case (x352, x353) => (x352, x353) match {
   case (_, null) => List(((x352, x353), null))
   case _ => 
   {val x354 = x353.customers2 
x354} match {
     case Nil => List(((x352, x353), null))
     case lst => lst.map{ case x355 => ((x352, x353), x355) }
  }
 }} 
val x368 = x356.flatMap{ case ((x357, x358), x359) => val x367 = (x358,x359) 
x367 match {
   case x363 if {val x364 = x359.c_name2 
val x365 = x357.c_name 
val x366 = x364 == x365 
x366} => List(({val x360 = (x357) 
x360}, {val x361 = x358.s_name 
val x362 = Record376(x361) 
x362}))
   case x363 => List(({val x360 = (x357) 
x360}, null))
 }    
}.groupByKey() 
val x373 = x368.map{ case (x369, x370) => 
   val x371 = x369.c_name 
val x372 = Record377(x371, x370) 
x372 
} 
x373.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("Query6FullSpark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
