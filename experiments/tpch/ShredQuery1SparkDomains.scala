
package experiments
/**
ljp = For l in L Union
  For p in P Union
    If (l.l_partkey = p.p_partkey)
    Then Sng((l_orderkey := l.l_orderkey, p_name := p.p_name, l_qty := l.l_quantity))

For c in C Union
  Sng((c_name := c.c_name, c_orders := For o in O Union
    If (o.o_custkey = c.c_custkey)
    Then Sng((o_orderdate := o.o_orderdate, o_parts := For lp in ljp Union
      If (lp.l_orderkey = o.o_orderkey)
      Then Sng((p_name := lp.p_name, l_qty := lp.l_qty))))))
**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record434(lbl: Unit)
case class Record435(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record436(p_name: String, p_partkey: Int)
case class Record437(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record438(c__Fc_custkey: Int)
case class Record439(c_name: String, c_orders: Record438)
case class Record440(lbl: Record438)
case class Record441(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record443(o__Fo_orderkey: Int)
case class Record444(o_orderdate: String, o_parts: Record443)
case class Record445(lbl: Record443)
case class Record447(p_name: String, l_qty: Double)
object ShredQuery1SparkDomains {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1SparkDomains"+sf)
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

   def f = { 
 val x317 = () 
val x318 = Record434(x317) 
val x319 = List(x318) 
val M_ctx1 = x319
val x320 = M_ctx1
//M_ctx1.collect.foreach(println(_))
/**
M_flat1 :=  REDUCE[ (c_name := x306.c_name,c_orders := (c__Fc_custkey := x306.c_custkey)) / true ](C__D._1)
**/

val x321 = L__D_1 
val x327 = x321.map(x322 => { val x323 = x322.l_orderkey 
val x324 = x322.l_quantity 
val x325 = x322.l_partkey 
val x326 = Record435(x323, x324, x325) 
x326 }) 
val x328 = P__D_1 
val x333 = x328.map(x329 => { val x330 = x329.p_name 
val x331 = x329.p_partkey 
val x332 = Record436(x330, x331) 
x332 }) 
val x338 = { val out1 = x327.map{ case x334 => ({val x336 = x334.l_partkey 
x336}, x334) }
  val out2 = x333.map{ case x335 => ({val x337 = x335.p_partkey 
x337}, x335) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x345 = x338.map{ case (x339, x340) => 
   val x341 = x339.l_orderkey 
val x342 = x340.p_name 
val x343 = x339.l_quantity 
val x344 = Record437(x341, x342, x343) 
x344 
} 
val ljp__D_1 = x345
val x346 = ljp__D_1
//ljp__D_1.collect.foreach(println(_))
val x347 = C__D_1 
val x353 = x347.map{ case x348 => 
   val x349 = x348.c_name 
val x350 = x348.c_custkey 
val x351 = Record438(x350) 
val x352 = Record439(x349, x351) 
x352 
} 
val M_flat1 = x353
val x354 = M_flat1
//M_flat1.collect.foreach(println(_))
val x356 = M_flat1 
val x360 = x356.map{ case x357 => 
   val x358 = x357.c_orders 
val x359 = Record440(x358) 
x359 
} 
val x361 = x360.distinct 
val M_ctx2 = x361
val x362 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x364 = M_ctx2 

/**

M_flat2 :=  REDUCE[ (_1 := x309.lbl,_2 := x315) / true ]( <-- (x309,x315) -- NEST[ U / (o_orderdate := x310.o_orderdate,o_parts := (o__Fo_orderkey := x310.o_orderkey)) / (x309), true / (x310) ]( <-- (x309,x310) -- (
 <-- (x309) -- SELECT[ true, x309 ](M_ctx2)) JOIN[x309.lbl.c__Fc_custkey = x310.o_custkey](

   <-- (x310) -- SELECT[ true, (o_orderdate := x310.o_orderdate,o_orderkey := x310.o_orderkey,o_custkey := x310.o_custkey) ](O__D._1))))
**/

val x365 = O__D_1 
val x371 = x365.map(x366 => { val x367 = x366.o_orderdate 
val x368 = x366.o_orderkey 
val x369 = x366.o_custkey 
val x370 = Record441(x367, x368, x369) 
x370 }) 
val x377 = { val out1 = x364.map{ case x372 => ({val x374 = x372.lbl 
val x375 = x374.c__Fc_custkey 
x375}, x372) }
  val out2 = x371.map{ case x373 => ({val x376 = x373.o_custkey 
x376}, x373) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x387 = x377.flatMap{ case (x378, x379) => val x386 = (x379) 
x386 match {
   case (null) => Nil 
   case x385 => List(({val x380 = (x378) 
x380}, {val x381 = x379.o_orderdate 
val x382 = x379.o_orderkey 
val x383 = Record443(x382) 
val x384 = Record444(x381, x383) 
x384}))
 }
}.groupByLabel() 
val x392 = x387.map{ case (x388, x389) => 
   val x390 = x388.lbl 
val x391 = (x390, x389) 
x391 
} 
val M_flat2 = x392
val x393 = M_flat2
//M_flat2.collect.foreach(println(_))
val x395 = M_flat2 
val x399 = x395.flatMap{ case x396 => x396 match {
   case null => List((x396, null))
   case _ =>
   val x397 = x396._2 
x397 match {
     case x398 => x398.map{ case v2 => (x396, v2) }
  }
 }} 
val x404 = x399.map{ case (x400, x401) => 
   val x402 = x401.o_parts 
val x403 = Record445(x402) 
x403 
} 
val x405 = x404.distinct 
val M_ctx3 = x405
val x406 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x408 = M_ctx3 

/**
M_flat3 :=  REDUCE[ (_1 := x313.lbl,_2 := x316) / true ]( <-- (x313,x316) -- NEST[ U / (p_name := x314.p_name,l_qty := x314.l_qty) / (x313), true / (x314) ]( <-- (x313,x314) -- (
 <-- (x313) -- SELECT[ true, x313 ](M_ctx3)) JOIN[x313.lbl.o__Fo_orderkey = x314.l_orderkey](

   <-- (x314) -- SELECT[ true, x314 ](ljp__D._1))))
**/

val x409 = ljp__D_1 
val x411 = x409 
val x417 = { val out1 = x408.map{ case x412 => ({val x414 = x412.lbl 
val x415 = x414.o__Fo_orderkey 
x415}, x412) }
  val out2 = x411.map{ case x413 => ({val x416 = x413.l_orderkey 
x416}, x413) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x426 = x417.flatMap{ case (x418, x419) => val x425 = (x419) 
x425 match {
   case (null) => Nil 
   case x424 => List(({val x420 = (x418) 
x420}, {val x421 = x419.p_name 
val x422 = x419.l_qty 
val x423 = Record447(x421, x422) 
x423}))
 }
}.groupByLabel() 
val x431 = x426.map{ case (x427, x428) => 
   val x429 = x427.lbl 
val x430 = (x429, x428) 
x430 
} 
val M_flat3 = x431
val x432 = M_flat3
//M_flat3.collect.foreach(println(_))
x432.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery1SparkDomains"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
