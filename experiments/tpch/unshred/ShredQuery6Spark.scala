
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record338(lbl: Unit)
case class Record339(o_orderkey: Int, o_custkey: Int)
case class Record340(c_name: String, c_custkey: Int)
case class Record341(o_orderkey: Int, c_name: String)
case class Record342(s__Fs_suppkey: Int)
case class Record343(s_name: String, customers2: Record342)
case class Record344(lbl: Record342)
case class Record345(l_orderkey: Int, l_suppkey: Int)
case class Record347(c_name2: String)
case class Record430(c_name: String, s_name: String)
case class Record431(c__Fc_name: String)
case class Record432(c_name: String, suppliers: Record431)
case class Record433(lbl: Record431)
case class Record435(s_name: String)
case class Record457(c_name: String, suppliers: Iterable[Record435])
object ShredQuery6Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6Spark"+sf)
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

   val x259 = () 
val x260 = Record338(x259) 
val x261 = List(x260) 
val resultInner_ctx1 = x261
val x262 = resultInner_ctx1
//resultInner_ctx1.collect.foreach(println(_))
val x267 = O__D_1.map(x263 => { val x264 = x263.o_orderkey 
val x265 = x263.o_custkey 
val x266 = Record339(x264, x265) 
x266 }) 
val x272 = C__D_1.map(x268 => { val x269 = x268.c_name 
val x270 = x268.c_custkey 
val x271 = Record340(x269, x270) 
x271 }) 
val x277 = { val out1 = x267.map{ case x273 => ({val x275 = x273.o_custkey 
x275}, x273) }
  val out2 = x272.map{ case x274 => ({val x276 = x274.c_custkey 
x276}, x274) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x283 = x277.map{ case (x278, x279) => 
   val x280 = x278.o_orderkey 
val x281 = x279.c_name 
val x282 = Record341(x280, x281) 
x282 
} 
val resultInner__D_1 = x283
val x284 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val M_ctx1 = x261
val x285 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x291 = S__D_1.map{ case x286 => 
   val x287 = x286.s_name 
val x288 = x286.s_suppkey 
val x289 = Record342(x288) 
val x290 = Record343(x287, x289) 
x290 
} 
val M__D_1 = x291
val x292 = M__D_1
//M__D_1.collect.foreach(println(_))
val x294 = M__D_1 
val x298 = x294.map{ case x295 => 
   val x296 = x295.customers2 
val x297 = Record344(x296) 
x297 
} 
val x299 = x298.distinct 
val customers2_ctx1 = x299
val x300 = customers2_ctx1
//customers2_ctx1.collect.foreach(println(_))
val x302 = customers2_ctx1 
val x307 = L__D_1.map(x303 => { val x304 = x303.l_orderkey 
val x305 = x303.l_suppkey 
val x306 = Record345(x304, x305) 
x306 }) 
val x313 = { val out1 = x302.map{ case x308 => ({val x310 = x308.lbl 
val x311 = x310.s__Fs_suppkey 
x311}, x308) }
  val out2 = x307.map{ case x309 => ({val x312 = x309.l_suppkey 
x312}, x309) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x315 = resultInner__D_1 
val x321 = { val out1 = x313.map{ case (x316, x317) => ({val x319 = x317.l_orderkey 
x319}, (x316, x317)) }
  val out2 = x315.map{ case x318 => ({val x320 = x318.o_orderkey 
x320}, x318) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x330 = x321.flatMap{ case ((x322, x323), x324) => val x329 = (x323,x324) 
x329 match {
   case (_,null) => Nil 
   case x328 => List(({val x325 = (x322) 
x325}, {val x326 = x324.c_name 
val x327 = Record347(x326) 
x327}))
 }
}.groupByLabel() 
val x335 = x330.map{ case (x331, x332) => 
   val x333 = x331.lbl 
val x334 = (x333, x332) 
x334 
} 
val customers2__D_1 = x335
val x336 = customers2__D_1
//customers2__D_1.collect.foreach(println(_))
val Query2__D_1 = M__D_1
Query2__D_1.cache
Query2__D_1.count
val Query2__D_2customers2_1 = customers2__D_1
Query2__D_2customers2_1.cache
Query2__D_2customers2_1.count
 def f = {
 
var start0 = System.currentTimeMillis()
val x373 = () 
val x374 = Record338(x373) 
val x375 = List(x374) 
val cflat_ctx1 = x375
val x376 = cflat_ctx1
//cflat_ctx1.collect.foreach(println(_))
val x378 = Query2__D_1 
val x382 = { val out1 = x378.map{ case x379 => ({val x381 = x379.customers2 
x381}, x379) }
out1.cogroup(Query2__D_2customers2_1.flatMapValues(identity)).flatMap{ pair =>
 for (k <- pair._2._1.iterator; w <- pair._2._2.iterator) yield (k,w)
}
}
         
val x388 = x382.map{ case (x383, x384) => 
   val x385 = x384.c_name2 
val x386 = x383.s_name 
val x387 = Record430(x385, x386) 
x387 
} 
val cflat__D_1 = x388
val x389 = cflat__D_1
//cflat__D_1.collect.foreach(println(_))
val M_ctx1 = x375
val x390 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x395 = C__D_1.map{ case x391 => 
   val x392 = x391.c_name 
val x393 = Record431(x392) 
val x394 = Record432(x392, x393) 
x394 
} 
val M__D_1 = x395
val x396 = M__D_1
//M__D_1.collect.foreach(println(_))
val x398 = M__D_1 
val x402 = x398.map{ case x399 => 
   val x400 = x399.suppliers 
val x401 = Record433(x400) 
x401 
} 
val x403 = x402.distinct 
val suppliers_ctx1 = x403
val x404 = suppliers_ctx1
//suppliers_ctx1.collect.foreach(println(_))
val x406 = suppliers_ctx1 
val x408 = cflat__D_1 
val x414 = { val out1 = x406.map{ case x409 => ({val x411 = x409.lbl 
val x412 = x411.c__Fc_name 
x412}, x409) }
  val out2 = x408.map{ case x410 => ({val x413 = x410.c_name 
x413}, x410) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x422 = x414.flatMap{ case (x415, x416) => val x421 = (x416) 
x421 match {
   case (null) => Nil 
   case x420 => List(({val x417 = (x415) 
x417}, {val x418 = x416.s_name 
val x419 = Record435(x418) 
x419}))
 }
}.groupByLabel() 
val x427 = x422.map{ case (x423, x424) => 
   val x425 = x423.lbl 
val x426 = (x425, x424) 
x426 
} 
val suppliers__D_1 = x427
val x428 = suppliers__D_1
//suppliers__D_1.collect.foreach(println(_))
x428.count
var end0 = System.currentTimeMillis() - start0
println("ShredQuery6Spark,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    

var start1 = System.currentTimeMillis()
val x445 = M__D_1 
val x449 = { val out1 = x445.map{ case x446 => ({val x448 = x446.suppliers 
x448}, x446) }
out1.cogroup(suppliers__D_1).flatMap{
 case (_, (left, x447)) => left.map{ case x446 => (x446, x447.flatten) }}
}
         
val x454 = x449.map{ case (x450, x451) => 
   val x452 = x450.c_name 
val x453 = Record457(x452, x451) 
x453 
} 
val newM__D_1 = x454
val x455 = newM__D_1
//newM__D_1.collect.foreach(println(_))
x455.count
var end1 = System.currentTimeMillis() - start1
println("ShredQuery6Spark,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("ShredQuery6Spark"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
