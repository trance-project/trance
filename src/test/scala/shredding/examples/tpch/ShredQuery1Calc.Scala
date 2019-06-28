
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record468(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record469(c_name: String, c_orders: Record468)
case class Record476(o__F: Orders, P__F: Int, L__F: Int)
case class Record477(o_orderdate: String, o_parts: Record476)
case class Record487(p_name: String, l_qty: Double)
case class RecM_flat2(_1: Record468, _2: List[Record477])
case class RecM_flat3(_1: Record476, _2: List[Record487])
case class RecM_flat1(_1: Q1Flat, _2: List[Record469])
case class RecM_ctx3(lbl: Record476)
case class RecM_ctx2(lbl: Record468)
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
    def f(){
      val x349 = Q1Flat(P__F, C__F, L__F, O__F)
val x350 = RecM_ctx1(x349)
val x351 = List(x350)
val M_ctx1 = x351
val x352 = M_ctx1
val x375 = M_ctx1.flatMap(x353 => { 
    val x354 = x353.lbl
    val x355 = C__D._1
    val x372 = x355.flatMap(x356 => { 
      if({val x357 = x353.lbl
      val x358 = x357.C__F
      val x359 = x356._1
      val x360 = x358 == x359
      x360}) {  val x361 = x356._2
        val x371 = x361.flatMap(x362 => { 
            val x363 = x362.c_name
            val x364 = x353.lbl
            val x365 = x364.P__F
            val x366 = x364.L__F
            val x367 = x364.O__F
            val x368 = Record468(x365, x366, x367, x362)
            val x369 = Record469(x363, x368)
            val x370 = List(x369)
            x370})
        x371} else {  Nil}})
    val x373 = RecM_flat1(x354, x372)
    val x374 = List(x373)
    x374})
val M_flat1 = x375
val x376 = M_flat1
val x384 = M_flat1.flatMap(x377 => { 
    val x378 = x377._2
    val x383 = x378.flatMap(x379 => { 
        val x380 = x379.c_orders
        val x381 = RecM_ctx2(x380)
        val x382 = List(x381)
        x382})
    x383})
val x385 = x384.distinct
val M_ctx2 = x385
val x386 = M_ctx2
val x413 = M_ctx2.flatMap(x387 => { 
    val x388 = x387.lbl
    val x389 = O__D._1
    val x410 = x389.flatMap(x390 => { 
      if({val x391 = x387.lbl
      val x392 = x391.O__F
      val x393 = x390._1
      val x394 = x392 == x393
      x394}) {  val x395 = x390._2
        val x409 = x395.flatMap(x396 => { 
          if({val x397 = x396.o_custkey
          val x398 = x387.lbl
          val x399 = x398.c__F
          val x400 = x399.c_custkey
          val x401 = x397 == x400
          x401}) {  val x402 = x396.o_orderdate
            val x403 = x387.lbl
            val x404 = x403.P__F
            val x405 = x403.L__F
            val x406 = Record476(x396, x404, x405)
            val x407 = Record477(x402, x406)
            val x408 = List(x407)
            x408} else {  Nil}})
        x409} else {  Nil}})
    val x411 = RecM_flat2(x388, x410)
    val x412 = List(x411)
    x412})
val M_flat2 = x413
val x414 = M_flat2
val x422 = M_flat2.flatMap(x415 => { 
    val x416 = x415._2
    val x421 = x416.flatMap(x417 => { 
        val x418 = x417.o_parts
        val x419 = RecM_ctx3(x418)
        val x420 = List(x419)
        x420})
    x421})
val x423 = x422.distinct
val M_ctx3 = x423
val x424 = M_ctx3
val x461 = M_ctx3.flatMap(x425 => { 
    val x426 = x425.lbl
    val x427 = L__D._1
    val x458 = x427.flatMap(x428 => { 
      if({val x429 = x425.lbl
      val x430 = x429.L__F
      val x431 = x428._1
      val x432 = x430 == x431
      x432}) {  val x433 = x428._2
        val x457 = x433.flatMap(x434 => { 
          if({val x435 = x434.l_orderkey
          val x436 = x425.lbl
          val x437 = x436.o__F
          val x438 = x437.o_orderkey
          val x439 = x435 == x438
          x439}) {  val x440 = P__D._1
            val x456 = x440.flatMap(x441 => { 
              if({val x442 = x425.lbl
              val x443 = x442.P__F
              val x444 = x441._1
              val x445 = x443 == x444
              x445}) {  val x446 = x441._2
                val x455 = x446.flatMap(x447 => { 
                  if({val x448 = x434.l_partkey
                  val x449 = x447.p_partkey
                  val x450 = x448 == x449
                  x450}) {  val x451 = x447.p_name
                    val x452 = x434.l_quantity
                    val x453 = Record487(x451, x452)
                    val x454 = List(x453)
                    x454} else {  Nil}})
                x455} else {  Nil}})
            x456} else {  Nil}})
        x457} else {  Nil}})
    val x459 = RecM_flat3(x426, x458)
    val x460 = List(x459)
    x460})
val M_flat3 = x461
val x462 = M_flat3
val x463 = (x352,x376,x386,x414,x424,x462)
x463 
    }
    var end0 = System.currentTimeMillis() - start0
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
