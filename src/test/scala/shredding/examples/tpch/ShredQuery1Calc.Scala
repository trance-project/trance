
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record416(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record417(c_name: String, c_orders: Record416)
case class Record424(o__F: Orders, P__F: Int, L__F: Int)
case class Record425(o_orderdate: String, o_parts: Record424)
case class Record434(p_name: String, l_qty: Double)
case class RecM_flat2(_1: Record416, _2: List[Record425])
case class RecM_flat3(_1: Record424, _2: List[Record434])
case class RecM_flat1(_1: Q1Flat, _2: List[Record417])
case class RecM_ctx3(lbl: Record424)
case class RecM_ctx2(lbl: Record416)
case class RecM_ctx1(lbl: Q1Flat)
object ShredQuery1Calc {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C__F = 1
val C__D = (List((C__F, TPCHLoader.loadCustomer[Customer].toList)), ())
val O__F = 2
val O__D = (List((O__F, TPCHLoader.loadOrders[Orders].toList)), ())
val L__F = 3
val L__D = (List((L__F, TPCHLoader.loadLineitem[Lineitem].toList)), ())
val P__F = 4
val P__D = (List((P__F, TPCHLoader.loadPart[Part].toList)), ())
    var end0 = System.currentTimeMillis() - start0
    def f(){
      val x309 = Q1Flat(P__F, C__F, L__F, O__F)
val x310 = RecM_ctx1(x309)
val x311 = List(x310)
val M_ctx1 = x311
val x312 = M_ctx1
val x332 = M_ctx1.flatMap(x313 => { 
    val x314 = x313.lbl
    val x315 = C__D._1
    val x329 = x315.flatMap(x316 => { 
      if({val x317 = x313.lbl
      val x318 = x317.C__F
      val x319 = C__D._1
      val x320 = x318 == x319
      x320}) {  val x321 = x316.c_name
        val x322 = x313.lbl
        val x323 = x322.P__F
        val x324 = x322.L__F
        val x325 = x322.O__F
        val x326 = Record416(x323, x324, x325, x316)
        val x327 = Record417(x321, x326)
        val x328 = List(x327)
        x328} else {  Nil}})
    val x330 = RecM_flat1(x314, x329)
    val x331 = List(x330)
    x331})
val M_flat1 = x332
val x333 = M_flat1
val x341 = M_flat1.flatMap(x334 => { 
    val x335 = x334._2
    val x340 = x335.flatMap(x336 => { 
        val x337 = x336.c_orders
        val x338 = RecM_ctx2(x337)
        val x339 = List(x338)
        x339})
    x340})
val x342 = x341.distinct
val M_ctx2 = x342
val x343 = M_ctx2
val x367 = M_ctx2.flatMap(x344 => { 
    val x345 = x344.lbl
    val x346 = O__D._1
    val x364 = x346.flatMap(x347 => { 
      if({val x348 = x344.lbl
      val x349 = x348.O__F
      val x350 = O__D._1
      val x351 = x349 == x350
      val x352 = x347.o_custkey
      val x353 = x348.c__F
      val x354 = x353.c_custkey
      val x355 = x352 == x354
      val x356 = x351 && x355
      x356}) {  val x357 = x347.o_orderdate
        val x358 = x344.lbl
        val x359 = x358.P__F
        val x360 = x358.L__F
        val x361 = Record424(x347, x359, x360)
        val x362 = Record425(x357, x361)
        val x363 = List(x362)
        x363} else {  Nil}})
    val x365 = RecM_flat2(x345, x364)
    val x366 = List(x365)
    x366})
val M_flat2 = x367
val x368 = M_flat2
val x376 = M_flat2.flatMap(x369 => { 
    val x370 = x369._2
    val x375 = x370.flatMap(x371 => { 
        val x372 = x371.o_parts
        val x373 = RecM_ctx3(x372)
        val x374 = List(x373)
        x374})
    x375})
val x377 = x376.distinct
val M_ctx3 = x377
val x378 = M_ctx3
val x410 = M_ctx3.flatMap(x379 => { 
    val x380 = x379.lbl
    val x381 = L__D._1
    val x407 = x381.flatMap(x382 => { 
      if({val x383 = x379.lbl
      val x384 = x383.L__F
      val x385 = L__D._1
      val x386 = x384 == x385
      val x387 = x382.l_orderkey
      val x388 = x383.o__F
      val x389 = x388.o_orderkey
      val x390 = x387 == x389
      val x391 = x386 && x390
      x391}) {  val x392 = P__D._1
        val x406 = x392.flatMap(x393 => { 
          if({val x394 = x379.lbl
          val x395 = x394.P__F
          val x396 = P__D._1
          val x397 = x395 == x396
          val x398 = x382.l_partkey
          val x399 = x393.p_partkey
          val x400 = x398 == x399
          val x401 = x397 && x400
          x401}) {  val x402 = x393.p_name
            val x403 = x382.l_quantity
            val x404 = Record434(x402, x403)
            val x405 = List(x404)
            x405} else {  Nil}})
        x406} else {  Nil}})
    val x408 = RecM_flat3(x380, x407)
    val x409 = List(x408)
    x409})
val M_flat3 = x410
val x411 = M_flat3
val x412 = (x312,x333,x343,x368,x378,x411)
x412 
    }
    var time = List[Long]()
    for (i <- 1 to 5) {
      var start = System.currentTimeMillis()
      f
      var end = System.currentTimeMillis() - start
      time = time :+ end
    }
    val avg = (time.sum/5)
    println(end0+","+avg)
 }
}
