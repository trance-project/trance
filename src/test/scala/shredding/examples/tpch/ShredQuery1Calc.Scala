
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record430(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record431(c_name: String, c_orders: Record430)
case class Record438(o__F: Orders, P__F: Int, L__F: Int)
case class Record439(o_orderdate: String, o_parts: Record438)
case class Record449(p_name: String, l_qty: Double)
case class RecM_flat2(_1: Record430, _2: List[Record439])
case class RecM_flat3(_1: Record438, _2: List[Record449])
case class RecM_flat1(_1: Q1Flat, _2: List[Record431])
case class RecM_ctx3(lbl: Record438)
case class RecM_ctx2(lbl: Record430)
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
      val x311 = Q1Flat(P__F, C__F, L__F, O__F)
val x312 = RecM_ctx1(x311)
val x313 = List(x312)
val M_ctx1 = x313
val x314 = M_ctx1
val x337 = M_ctx1.flatMap(x315 => { 
    val x316 = x315.lbl
    val x317 = C__D._1
    val x334 = x317.flatMap(x318 => { 
      if({val x319 = x315.lbl
      val x320 = x319.C__F
      val x321 = x318._1
      val x322 = x320 == x321
      x322}) {  val x323 = x318._2
        val x333 = x323.flatMap(x324 => { 
            val x325 = x324.c_name
            val x326 = x315.lbl
            val x327 = x326.P__F
            val x328 = x326.L__F
            val x329 = x326.O__F
            val x330 = Record430(x327, x328, x329, x324)
            val x331 = Record431(x325, x330)
            val x332 = List(x331)
            x332})
        x333} else {  Nil}})
    val x335 = RecM_flat1(x316, x334)
    val x336 = List(x335)
    x336})
val M_flat1 = x337
val x338 = M_flat1
val x346 = M_flat1.flatMap(x339 => { 
    val x340 = x339._2
    val x345 = x340.flatMap(x341 => { 
        val x342 = x341.c_orders
        val x343 = RecM_ctx2(x342)
        val x344 = List(x343)
        x344})
    x345})
val x347 = x346.distinct
val M_ctx2 = x347
val x348 = M_ctx2
val x375 = M_ctx2.flatMap(x349 => { 
    val x350 = x349.lbl
    val x351 = O__D._1
    val x372 = x351.flatMap(x352 => { 
      if({val x353 = x349.lbl
      val x354 = x353.O__F
      val x355 = x352._1
      val x356 = x354 == x355
      x356}) {  val x357 = x352._2
        val x371 = x357.flatMap(x358 => { 
          if({val x359 = x358.o_custkey
          val x360 = x349.lbl
          val x361 = x360.c__F
          val x362 = x361.c_custkey
          val x363 = x359 == x362
          x363}) {  val x364 = x358.o_orderdate
            val x365 = x349.lbl
            val x366 = x365.P__F
            val x367 = x365.L__F
            val x368 = Record438(x358, x366, x367)
            val x369 = Record439(x364, x368)
            val x370 = List(x369)
            x370} else {  Nil}})
        x371} else {  Nil}})
    val x373 = RecM_flat2(x350, x372)
    val x374 = List(x373)
    x374})
val M_flat2 = x375
val x376 = M_flat2
val x384 = M_flat2.flatMap(x377 => { 
    val x378 = x377._2
    val x383 = x378.flatMap(x379 => { 
        val x380 = x379.o_parts
        val x381 = RecM_ctx3(x380)
        val x382 = List(x381)
        x382})
    x383})
val x385 = x384.distinct
val M_ctx3 = x385
val x386 = M_ctx3
val x423 = M_ctx3.flatMap(x387 => { 
    val x388 = x387.lbl
    val x389 = L__D._1
    val x420 = x389.flatMap(x390 => { 
      if({val x391 = x387.lbl
      val x392 = x391.L__F
      val x393 = x390._1
      val x394 = x392 == x393
      x394}) {  val x395 = x390._2
        val x419 = x395.flatMap(x396 => { 
          if({val x397 = x396.l_orderkey
          val x398 = x387.lbl
          val x399 = x398.o__F
          val x400 = x399.o_orderkey
          val x401 = x397 == x400
          x401}) {  val x402 = P__D._1
            val x418 = x402.flatMap(x403 => { 
              if({val x404 = x387.lbl
              val x405 = x404.P__F
              val x406 = x403._1
              val x407 = x405 == x406
              x407}) {  val x408 = x403._2
                val x417 = x408.flatMap(x409 => { 
                  if({val x410 = x396.l_partkey
                  val x411 = x409.p_partkey
                  val x412 = x410 == x411
                  x412}) {  val x413 = x409.p_name
                    val x414 = x396.l_quantity
                    val x415 = Record449(x413, x414)
                    val x416 = List(x415)
                    x416} else {  Nil}})
                x417} else {  Nil}})
            x418} else {  Nil}})
        x419} else {  Nil}})
    val x421 = RecM_flat3(x388, x420)
    val x422 = List(x421)
    x422})
val M_flat3 = x423
val x424 = M_flat3
val x425 = (x314,x338,x348,x376,x386,x424)
x425 
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
