
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record328(lbl: Unit)
case class Record329(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record330(p_name: String, p_partkey: Int)
case class Record331(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record332(c__Fc_custkey: Int)
case class Record333(c_name: String, c_orders: Record332)
case class Record334(lbl: Record332)
case class Record335(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record337(o__Fo_orderkey: Int)
case class Record338(o_orderdate: String, o_parts: Record337)
case class Record339(lbl: Record337)
case class Record341(p_name: String, l_qty: Double)
case class Record420(c2__Fc_name: String)
case class Record421(orders: String, customers: Record420)
case class Record422(lbl: Record420)
case class Record423(c_name: String, c_address: String)
case class Record425(name: String, address: String)
case class Record447(orders: String, customers: Iterable[Record425])
object ShredQuery4New2Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4New2Spark"+sf)
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

   val x215 = () 
val x216 = Record328(x215) 
val x217 = List(x216) 
val ljp_ctx1 = x217
val x218 = ljp_ctx1
//ljp_ctx1.collect.foreach(println(_))
val x224 = L__D_1.map(x219 => { val x220 = x219.l_orderkey 
val x221 = x219.l_quantity 
val x222 = x219.l_partkey 
val x223 = Record329(x220, x221, x222) 
x223 }) 
val x229 = P__D_1.map(x225 => { val x226 = x225.p_name 
val x227 = x225.p_partkey 
val x228 = Record330(x226, x227) 
x228 }) 
val x234 = { val out1 = x224.map{ case x230 => ({val x232 = x230.l_partkey 
x232}, x230) }
  val out2 = x229.map{ case x231 => ({val x233 = x231.p_partkey 
x233}, x231) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x241 = x234.map{ case (x235, x236) => 
   val x237 = x235.l_orderkey 
val x238 = x236.p_name 
val x239 = x235.l_quantity 
val x240 = Record331(x237, x238, x239) 
x240 
} 
val ljp__D_1 = x241
val x242 = ljp__D_1
//ljp__D_1.collect.foreach(println(_))
val M_ctx1 = x217
val x243 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x249 = C__D_1.map{ case x244 => 
   val x245 = x244.c_name 
val x246 = x244.c_custkey 
val x247 = Record332(x246) 
val x248 = Record333(x245, x247) 
x248 
} 
val M__D_1 = x249
val x250 = M__D_1
//M__D_1.collect.foreach(println(_))
val x252 = M__D_1 
val x256 = x252.map{ case x253 => 
   val x254 = x253.c_orders 
val x255 = Record334(x254) 
x255 
} 
val x257 = x256.distinct 
val c_orders_ctx1 = x257
val x258 = c_orders_ctx1
//c_orders_ctx1.collect.foreach(println(_))
val x260 = c_orders_ctx1 
val x266 = O__D_1.map(x261 => { val x262 = x261.o_orderdate 
val x263 = x261.o_orderkey 
val x264 = x261.o_custkey 
val x265 = Record335(x262, x263, x264) 
x265 }) 
val x272 = { val out1 = x260.map{ case x267 => ({val x269 = x267.lbl 
val x270 = x269.c__Fc_custkey 
x270}, x267) }
  val out2 = x266.map{ case x268 => ({val x271 = x268.o_custkey 
x271}, x268) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x282 = x272.flatMap{ case (x273, x274) => val x281 = (x274) 
x281 match {
   case (null) => Nil 
   case x280 => List(({val x275 = (x273) 
x275}, {val x276 = x274.o_orderdate 
val x277 = x274.o_orderkey 
val x278 = Record337(x277) 
val x279 = Record338(x276, x278) 
x279}))
 }
}.groupByLabel() 
val x287 = x282.map{ case (x283, x284) => 
   val x285 = x283.lbl 
val x286 = (x285, x284) 
x286 
} 
val c_orders__D_1 = x287
val x288 = c_orders__D_1
//c_orders__D_1.collect.foreach(println(_))
val x290 = c_orders__D_1 
val x294 = x290.flatMap{ 
 case x291 => {val x292 = x291._2 
x292}.map{ case v2 => (x291._1, v2) }
}
         
val x299 = x294.map{ case (x295, x296) => 
   val x297 = x296.o_parts 
val x298 = Record339(x297) 
x298 
} 
val x300 = x299.distinct 
val o_parts_ctx1 = x300
val x301 = o_parts_ctx1
//o_parts_ctx1.collect.foreach(println(_))
val x303 = o_parts_ctx1 
val x305 = ljp__D_1 
val x311 = { val out1 = x303.map{ case x306 => ({val x308 = x306.lbl 
val x309 = x308.o__Fo_orderkey 
x309}, x306) }
  val out2 = x305.map{ case x307 => ({val x310 = x307.l_orderkey 
x310}, x307) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x320 = x311.flatMap{ case (x312, x313) => val x319 = (x313) 
x319 match {
   case (null) => Nil 
   case x318 => List(({val x314 = (x312) 
x314}, {val x315 = x313.p_name 
val x316 = x313.l_qty 
val x317 = Record341(x315, x316) 
x317}))
 }
}.groupByLabel() 
val x325 = x320.map{ case (x321, x322) => 
   val x323 = x321.lbl 
val x324 = (x323, x322) 
x324 
} 
val o_parts__D_1 = x325
val x326 = o_parts__D_1
//o_parts__D_1.collect.foreach(println(_))
val Query1__D_1 = M__D_1
Query1__D_1.cache
Query1__D_1.count
val Query1__D_2c_orders_1 = c_orders__D_1
Query1__D_2c_orders_1.cache
Query1__D_2c_orders_1.count
val Query1__D_2c_orders_2o_parts_1 = o_parts__D_1
Query1__D_2c_orders_2o_parts_1.cache
Query1__D_2c_orders_2o_parts_1.count
 def f = {
 
var start0 = System.currentTimeMillis()
val x365 = () 
val x366 = Record328(x365) 
val x367 = List(x366) 
val M_ctx1 = x367
val x368 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x370 = Query1__D_1 
val x374 = { val out1 = x370.map{ case x371 => ({val x373 = x371.c_orders 
x373}, x371) }
out1.cogroup(Query1__D_2c_orders_1.flatMapValues(identity)).flatMap{ pair =>
 for (k <- pair._2._1.iterator; w <- pair._2._2.iterator) yield (k,w)
}
}
         
val x381 = x374.map{ case (x375, x376) => 
   val x377 = x376.o_orderdate 
val x378 = x375.c_name 
val x379 = Record420(x378) 
val x380 = Record421(x377, x379) 
x380 
} 
val M__D_1 = x381
val x382 = M__D_1
//M__D_1.collect.foreach(println(_))
val x384 = M__D_1 
val x388 = x384.map{ case x385 => 
   val x386 = x385.customers 
val x387 = Record422(x386) 
x387 
} 
val x389 = x388.distinct 
val customers_ctx1 = x389
val x390 = customers_ctx1
//customers_ctx1.collect.foreach(println(_))
val x392 = customers_ctx1 
val x397 = C__D_1.map(x393 => { val x394 = x393.c_name 
val x395 = x393.c_address 
val x396 = Record423(x394, x395) 
x396 }) 
val x403 = { val out1 = x392.map{ case x398 => ({val x400 = x398.lbl 
val x401 = x400.c2__Fc_name 
x401}, x398) }
  val out2 = x397.map{ case x399 => ({val x402 = x399.c_name 
x402}, x399) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x412 = x403.flatMap{ case (x404, x405) => val x411 = (x405) 
x411 match {
   case (null) => Nil 
   case x410 => List(({val x406 = (x404) 
x406}, {val x407 = x405.c_name 
val x408 = x405.c_address 
val x409 = Record425(x407, x408) 
x409}))
 }
}.groupByLabel() 
val x417 = x412.map{ case (x413, x414) => 
   val x415 = x413.lbl 
val x416 = (x415, x414) 
x416 
} 
val customers__D_1 = x417
val x418 = customers__D_1
//customers__D_1.collect.foreach(println(_))
x418.count
var end0 = System.currentTimeMillis() - start0
println("ShredQuery4New2Spark,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    

var start1 = System.currentTimeMillis()
val x435 = M__D_1 
val x439 = { val out1 = x435.map{ case x436 => ({val x438 = x436.customers 
x438}, x436) }
out1.cogroup(customers__D_1).flatMap{
 case (_, (left, x437)) => left.map{ case x436 => (x436, x437.flatten) }}
}
         
val x444 = x439.map{ case (x440, x441) => 
   val x442 = x440.orders 
val x443 = Record447(x442, x441) 
x443 
} 
val newM__D_1 = x444
val x445 = newM__D_1
//newM__D_1.collect.foreach(println(_))
x445.count
var end1 = System.currentTimeMillis() - start1
println("ShredQuery4New2Spark,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("ShredQuery4New2Spark"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
