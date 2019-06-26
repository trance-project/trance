
package experiments
/** Generated code **/
import shredding.examples.tpch._
object ShredQuery1Calc {
 case class Record415(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record416(c_name: String, c_orders: Record415)
case class Record423(o__F: Orders, P__F: Int, L__F: Int)
case class Record424(o_orderdate: String, o_parts: Record423)
case class Record434(p_name: String, l_qty: Double)
case class RecM_flat2(_1: Record415, _2: List[Record424])
case class RecM_flat3(_1: Record423, _2: List[Record434])
case class RecM_flat1(_1: Q1Flat, _2: List[Record416])
case class RecM_ctx3(lbl: Record423)
case class RecM_ctx2(lbl: Record415)
case class RecM_ctx1(lbl: Q1Flat)
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
    var time = List[Long]()
    for (i <- 1 to 5) {
      var start = System.currentTimeMillis()
      f(C__F, C__D, O__F, O__D, L__F, L__D, P__F, P__D)
      var end = System.currentTimeMillis() - start
      time = time :+ end
    }
    val avg = (time.sum/5)
    println(end0+","+avg)
 }
 def f(C__F: Int, C__D: (List[(Int, List[Customer])], Unit),
                     O__F: Int, O__D: (List[(Int, List[Orders])], Unit),
                     L__F: Int, L__D: (List[(Int, List[Lineitem])], Unit),
                     P__F: Int, P__D: (List[(Int, List[Part])], Unit)){
  val x296 = Q1Flat(P__F, C__F, L__F, O__F)
val x297 = RecM_ctx1(x296)
val x298 = List(x297)
val M_ctx1 = x298
val x299 = M_ctx1
val x322 = M_ctx1.flatMap(x300 => { 
    val x301 = x300.lbl
    val x302 = C__D._1
    val x319 = x302.flatMap(x303 => { 
      if({val x304 = x300.lbl
      val x305 = x304.C__F
      val x306 = x303._1
      val x307 = x305 == x306
      x307}) {  val x308 = x303._2
        val x318 = x308.flatMap(x309 => { 
            val x310 = x309.c_name
            val x311 = x300.lbl
            val x312 = x311.P__F
            val x313 = x311.L__F
            val x314 = x311.O__F
            val x315 = Record415(x312, x313, x314, x309)
            val x316 = Record416(x310, x315)
            val x317 = List(x316)
            x317})
        x318} else {  Nil}})
    val x320 = RecM_flat1(x301, x319)
    val x321 = List(x320)
    x321})
val M_flat1 = x322
val x323 = M_flat1
val x331 = M_flat1.flatMap(x324 => { 
    val x325 = x324._2
    val x330 = x325.flatMap(x326 => { 
        val x327 = x326.c_orders
        val x328 = RecM_ctx2(x327)
        val x329 = List(x328)
        x329})
    x330})
val x332 = x331.distinct
val M_ctx2 = x332
val x333 = M_ctx2
val x360 = M_ctx2.flatMap(x334 => { 
    val x335 = x334.lbl
    val x336 = O__D._1
    val x357 = x336.flatMap(x337 => { 
      if({val x338 = x334.lbl
      val x339 = x338.O__F
      val x340 = x337._1
      val x341 = x339 == x340
      x341}) {  val x342 = x337._2
        val x356 = x342.flatMap(x343 => { 
          if({val x344 = x343.o_custkey
          val x345 = x334.lbl
          val x346 = x345.c__F
          val x347 = x346.c_custkey
          val x348 = x344 == x347
          x348}) {  val x349 = x343.o_orderdate
            val x350 = x334.lbl
            val x351 = x350.P__F
            val x352 = x350.L__F
            val x353 = Record423(x343, x351, x352)
            val x354 = Record424(x349, x353)
            val x355 = List(x354)
            x355} else {  Nil}})
        x356} else {  Nil}})
    val x358 = RecM_flat2(x335, x357)
    val x359 = List(x358)
    x359})
val M_flat2 = x360
val x361 = M_flat2
val x369 = M_flat2.flatMap(x362 => { 
    val x363 = x362._2
    val x368 = x363.flatMap(x364 => { 
        val x365 = x364.o_parts
        val x366 = RecM_ctx3(x365)
        val x367 = List(x366)
        x367})
    x368})
val x370 = x369.distinct
val M_ctx3 = x370
val x371 = M_ctx3
val x408 = M_ctx3.flatMap(x372 => { 
    val x373 = x372.lbl
    val x374 = L__D._1
    val x405 = x374.flatMap(x375 => { 
      if({val x376 = x372.lbl
      val x377 = x376.L__F
      val x378 = x375._1
      val x379 = x377 == x378
      x379}) {  val x380 = x375._2
        val x404 = x380.flatMap(x381 => { 
          if({val x382 = x381.l_orderkey
          val x383 = x372.lbl
          val x384 = x383.o__F
          val x385 = x384.o_orderkey
          val x386 = x382 == x385
          x386}) {  val x387 = P__D._1
            val x403 = x387.flatMap(x388 => { 
              if({val x389 = x372.lbl
              val x390 = x389.P__F
              val x391 = x388._1
              val x392 = x390 == x391
              x392}) {  val x393 = x388._2
                val x402 = x393.flatMap(x394 => { 
                  if({val x395 = x381.l_partkey
                  val x396 = x394.p_partkey
                  val x397 = x395 == x396
                  x397}) {  val x398 = x394.p_name
                    val x399 = x381.l_quantity
                    val x400 = Record434(x398, x399)
                    val x401 = List(x400)
                    x401} else {  Nil}})
                x402} else {  Nil}})
            x403} else {  Nil}})
        x404} else {  Nil}})
    val x406 = RecM_flat3(x373, x405)
    val x407 = List(x406)
    x407})
val M_flat3 = x408
val x409 = M_flat3
val x410 = (x299,x323,x333,x361,x371,x409)
x410
 }
}
