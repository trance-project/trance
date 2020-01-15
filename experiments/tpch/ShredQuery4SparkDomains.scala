
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record290(lbl: Unit)
case class Record291(c__Fc_custkey: Int)
case class Record292(c_name: String, corders: Record291)
case class Record293(lbl: Record291)
case class Record294(o_orderkey: Int, o_orderdate: String, o_custkey: Int)
case class Record296(o_orderkey: Int, o_orderdate: String)
case class Record408(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record409(p_name: String, p_partkey: Int)
case class Record410(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record411(customer__Fcorders: Record291)
case class Record412(c_name: String, partqty: Record411)
case class Record413(lbl: Record411)
case class Record415(orderdate: String, pname: String)
object ShredQuery4SparkDomains {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4SparkDomains"+sf)
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

   val x241 = () 
val x242 = Record290(x241) 
val x243 = List(x242) 
val M_ctx1 = x243
val x244 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x250 = C__D_1.map{ case x245 => 
   val x246 = x245.c_name 
val x247 = x245.c_custkey 
val x248 = Record291(x247) 
val x249 = Record292(x246, x248) 
x249 
} 
val M_flat1 = x250
val x251 = M_flat1
//M_flat1.collect.foreach(println(_))
val x253 = M_flat1 
val x257 = x253.map{ case x254 => 
   val x255 = x254.corders 
val x256 = Record293(x255) 
x256 
} 
val x258 = x257.distinct 
val M_ctx2 = x258
val x259 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x261 = M_ctx2 
val x267 = O__D_1.map(x262 => { val x263 = x262.o_orderkey 
val x264 = x262.o_orderdate 
val x265 = x262.o_custkey 
val x266 = Record294(x263, x264, x265) 
x266 }) 
val x273 = { val out1 = x261.map{ case x268 => ({val x270 = x268.lbl 
val x271 = x270.c__Fc_custkey 
x271}, x268) }
  val out2 = x267.map{ case x269 => ({val x272 = x269.o_custkey 
x272}, x269) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x282 = x273.flatMap{ case (x274, x275) => val x281 = (x275) 
x281 match {
   case (null) => Nil 
   case x280 => List(({val x276 = (x274) 
x276}, {val x277 = x275.o_orderkey 
val x278 = x275.o_orderdate 
val x279 = Record296(x277, x278) 
x279}))
 }
}.groupByLabel() 
val x287 = x282.map{ case (x283, x284) => 
   val x285 = x283.lbl 
val x286 = (x285, x284) 
x286 
} 
val M_flat2 = x287
val x288 = M_flat2
//M_flat2.collect.foreach(println(_))
val CustOrders__D_1 = M_flat1
CustOrders__D_1.cache
CustOrders__D_1.count
val CustOrders__D_2corders_1 = M_flat2
CustOrders__D_2corders_1.cache
CustOrders__D_2corders_1.count
def f = { 
 val x331 = () 
val x332 = Record290(x331) 
val x333 = List(x332) 
val M_ctx1 = x333
val x334 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x340 = L__D_1.map(x335 => { val x336 = x335.l_orderkey 
val x337 = x335.l_quantity 
val x338 = x335.l_partkey 
val x339 = Record408(x336, x337, x338) 
x339 }) 
val x345 = P__D_1.map(x341 => { val x342 = x341.p_name 
val x343 = x341.p_partkey 
val x344 = Record409(x342, x343) 
x344 }) 
val x350 = { val out1 = x340.map{ case x346 => ({val x348 = x346.l_partkey 
x348}, x346) }
  val out2 = x345.map{ case x347 => ({val x349 = x347.p_partkey 
x349}, x347) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x357 = x350.map{ case (x351, x352) => 
   val x353 = x351.l_orderkey 
val x354 = x352.p_name 
val x355 = x351.l_quantity 
val x356 = Record410(x353, x354, x355) 
x356 
} 
val parts__D_1 = x357
val x358 = parts__D_1
//parts__D_1.collect.foreach(println(_))
val x364 = CustOrders__D_1.map{ case x359 => 
   val x360 = x359.c_name 
val x361 = x359.corders 
val x362 = Record411(x361) 
val x363 = Record412(x360, x362) 
x363 
} 
val M_flat1 = x364
val x365 = M_flat1
//M_flat1.collect.foreach(println(_))
val x367 = M_flat1 
val x371 = x367.map{ case x368 => 
   val x369 = x368.partqty 
val x370 = Record413(x369) 
x370 
} 
val x372 = x371.distinct 
val M_ctx2 = x372
val x373 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x375 = M_ctx2 
val x380 = { val out1 = x375.map{ case x376 => ({val x378 = x376.lbl 
val x379 = x378.customer__Fcorders 
x379}, x376) }
  val out2 = CustOrders__D_2corders_1.flatMapValues(identity)
  out1.lookup(out2)
} 
val x382 = parts__D_1 
val x388 = { val out1 = x380.map{ case (x383, x384) => ({val x386 = x384.o_orderkey 
x386}, (x383, x384)) }
  val out2 = x382.map{ case x385 => ({val x387 = x385.l_orderkey 
x387}, x385) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x399 = x388.flatMap{ case ((x389, x390), x391) => val x398 = (x389,x390,x391) 
x398 match {
   case (_,null,_) => Nil
case (_,_,null) => Nil
   case x397 => List(({val x392 = x390.o_orderdate 
val x393 = x391.p_name 
val x394 = Record415(x392, x393) 
val x395 = (x389,x394) 
x395}, {val x396 = x391.l_qty 
x396}))
 }
}.reduceByKey(_ + _) 
val x405 = x399.map{ case ((x400, x401), x402) => 
   val x403 = x400.lbl 
val x404 = (x403, (x401, x402)) 
x404 
} 
val M_flat2 = x405
val x406 = M_flat2
//M_flat2.collect.foreach(println(_))
x406.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery4SparkDomains"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
