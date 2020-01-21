
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record323(lbl: Unit)
case class Record324(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record325(p_name: String, p_partkey: Int)
case class Record326(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record327(c__Fc_custkey: Int)
case class Record328(c_name: String, c_orders: Record327)
case class Record329(lbl: Record327)
case class Record330(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record332(o__Fo_orderkey: Int)
case class Record333(o_orderdate: String, o_parts: Record332)
case class Record334(lbl: Record332)
case class Record336(p_name: String, l_qty: Double)
case class Record412(c2__Fc_orders: Record327)
case class Record413(c_name: String, totals: Record412)
case class Record414(lbl: Record412)
case class Record416(orderdate: String, pname: String)
case class Record438(orderdate: String, pname: String, _2: Double)
case class Record439(c_name: String, totals: Iterable[Record438])
object ShredQuery4NewSparkOpt {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4NewSparkOpt"+sf)
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

   val x210 = () 
val x211 = Record323(x210) 
val x212 = List(x211) 
val ljp_ctx1 = x212
val x213 = ljp_ctx1
//ljp_ctx1.collect.foreach(println(_))
val x219 = L__D_1.map(x214 => { val x215 = x214.l_orderkey 
val x216 = x214.l_quantity 
val x217 = x214.l_partkey 
val x218 = Record324(x215, x216, x217) 
x218 }) 
val x224 = P__D_1.map(x220 => { val x221 = x220.p_name 
val x222 = x220.p_partkey 
val x223 = Record325(x221, x222) 
x223 }) 
val x229 = { val out1 = x219.map{ case x225 => ({val x227 = x225.l_partkey 
x227}, x225) }
  val out2 = x224.map{ case x226 => ({val x228 = x226.p_partkey 
x228}, x226) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x236 = x229.map{ case (x230, x231) => 
   val x232 = x230.l_orderkey 
val x233 = x231.p_name 
val x234 = x230.l_quantity 
val x235 = Record326(x232, x233, x234) 
x235 
} 
val ljp__D_1 = x236
val x237 = ljp__D_1
//ljp__D_1.collect.foreach(println(_))
val M_ctx1 = x212
val x238 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x244 = C__D_1.map{ case x239 => 
   val x240 = x239.c_name 
val x241 = x239.c_custkey 
val x242 = Record327(x241) 
val x243 = Record328(x240, x242) 
x243 
} 
val M__D_1 = x244
val x245 = M__D_1
//M__D_1.collect.foreach(println(_))
val x247 = M__D_1 
val x251 = x247.map{ case x248 => 
   val x249 = x248.c_orders 
val x250 = Record329(x249) 
x250 
} 
val x252 = x251.distinct 
val c_orders_ctx1 = x252
val x253 = c_orders_ctx1
//c_orders_ctx1.collect.foreach(println(_))
val x255 = c_orders_ctx1 
val x261 = O__D_1.map(x256 => { val x257 = x256.o_orderdate 
val x258 = x256.o_orderkey 
val x259 = x256.o_custkey 
val x260 = Record330(x257, x258, x259) 
x260 }) 
val x267 = { val out1 = x255.map{ case x262 => ({val x264 = x262.lbl 
val x265 = x264.c__Fc_custkey 
x265}, x262) }
  val out2 = x261.map{ case x263 => ({val x266 = x263.o_custkey 
x266}, x263) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x277 = x267.flatMap{ case (x268, x269) => val x276 = (x269) 
x276 match {
   case (null) => Nil 
   case x275 => List(({val x270 = (x268) 
x270}, {val x271 = x269.o_orderdate 
val x272 = x269.o_orderkey 
val x273 = Record332(x272) 
val x274 = Record333(x271, x273) 
x274}))
 }
}.groupByLabel() 
val x282 = x277.map{ case (x278, x279) => 
   val x280 = x278.lbl 
val x281 = (x280, x279) 
x281 
} 
val c_orders__D_1 = x282
val x283 = c_orders__D_1
//c_orders__D_1.collect.foreach(println(_))
val x285 = c_orders__D_1 
val x289 = x285.flatMap{ 
 case x286 => {val x287 = x286._2 
x287}.map{ case v2 => (x286._1, v2) }
}
         
val x294 = x289.map{ case (x290, x291) => 
   val x292 = x291.o_parts 
val x293 = Record334(x292) 
x293 
} 
val x295 = x294.distinct 
val o_parts_ctx1 = x295
val x296 = o_parts_ctx1
//o_parts_ctx1.collect.foreach(println(_))
val x298 = o_parts_ctx1 
val x300 = ljp__D_1 
val x306 = { val out1 = x298.map{ case x301 => ({val x303 = x301.lbl 
val x304 = x303.o__Fo_orderkey 
x304}, x301) }
  val out2 = x300.map{ case x302 => ({val x305 = x302.l_orderkey 
x305}, x302) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x315 = x306.flatMap{ case (x307, x308) => val x314 = (x308) 
x314 match {
   case (null) => Nil 
   case x313 => List(({val x309 = (x307) 
x309}, {val x310 = x308.p_name 
val x311 = x308.l_qty 
val x312 = Record336(x310, x311) 
x312}))
 }
}.groupByLabel() 
val x320 = x315.map{ case (x316, x317) => 
   val x318 = x316.lbl 
val x319 = (x318, x317) 
x319 
} 
val o_parts__D_1 = x320
val x321 = o_parts__D_1
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
val x362 = () 
val x363 = Record323(x362) 
val x364 = List(x363) 
val M_ctx1 = x364
val x365 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x371 = Query1__D_1.map{ case x366 => 
   val x367 = x366.c_name 
val x368 = x366.c_orders 
val x369 = Record412(x368) 
val x370 = Record413(x367, x369) 
x370 
} 
val M__D_1 = x371
val x372 = M__D_1
//M__D_1.collect.foreach(println(_))
/**val x374 = M__D_1 
val x378 = x374.map{ case x375 => 
   val x376 = x375.totals 
val x377 = Record414(x376) 
x377 
} 
val x379 = x378.distinct 
val totals_ctx1 = x379
val x380 = totals_ctx1
//totals_ctx1.collect.foreach(println(_))
val x382 = totals_ctx1 
val x387 = { val out1 = x382.map{ case x383 => ({val x385 = x383.lbl 
val x386 = x385.c2__Fc_orders 
x386}, x383) }
out1.cogroup(Query1__D_2c_orders_1.flatMapValues(identity)).flatMap{ pair =>
 for (k <- pair._2._1.iterator; w <- pair._2._2.iterator) yield (k,w)
}
}**/
val x387 = Query1__D_2c_orders_1.flatMapValues(identity)
         
val x392 = { val out1 = x387.map{ case (x388, x389) => ({val x391 = x389.o_parts 
x391}, (x388, x389)) }
out1.cogroup(Query1__D_2c_orders_2o_parts_1.flatMapValues(identity)).flatMap{ pair =>
 for (k <- pair._2._1.iterator; w <- pair._2._2.iterator) yield (k,w)
}
}
         
val x403 = x392.flatMap{ case ((x393, x394), x395) => val x402 = (x393,x394,x395) 
x402 match {
   case (_,null,_) => Nil
case (_,_,null) => Nil
   case x401 => List(({val x396 = x394.o_orderdate 
val x397 = x395.p_name 
val x398 = Record416(x396, x397) 
val x399 = (x393,x398) 
x399}, {val x400 = x395.l_qty 
x400}))
 }
}.reduceByKey(_ + _) 
val x409 = x403.map{ case ((x404, x405), x406) => 
   val x407 = Record412(x404)//.lbl 
val x408 = (x407, Record438(x405.pname, x405.orderdate, x406)) 
x408 
}.groupByLabel() 
val totals__D_1 = x409
val x410 = totals__D_1
//totals__D_1.collect.foreach(println(_))
x410.count
var end0 = System.currentTimeMillis() - start0
println("ShredQuery4NewSparkOpt,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    

var start1 = System.currentTimeMillis()
val x426 = M__D_1 
val x430 = { val out1 = x426.map{ case x427 => ({val x429 = x427.totals 
x429}, x427) }
out1.cogroup(totals__D_1).flatMap{
 case (_, (left, x428)) => left.map{ case x427 => (x427, x428.flatten) }}
}
         
val x435 = x430.map{ case (x431, x432) => 
   val x433 = x431.c_name 
val x434 = Record439(x433, x432) 
x434 
} 
val newM__D_1 = x435
val x436 = newM__D_1
//newM__D_1.collect.foreach(println(_))
x436.count
var end1 = System.currentTimeMillis() - start1
println("ShredQuery4NewSparkOpt,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("ShredQuery4NewSparkOpt"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
