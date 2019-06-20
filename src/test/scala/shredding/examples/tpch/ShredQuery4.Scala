
package experiments
/** Generated code **/
object ShredQuery4 {
 
import shredding.examples.tpch._
val C__F = 1
val C__D = (List((C__F, TPCHLoader.loadCustomer.toList)), ())
val O__F = 2
val O__D = (List((O__F, TPCHLoader.loadOrders.toList)), ())
val L__F = 3
val L__D = (List((L__F, TPCHLoader.loadLineitem.toList)), ())
val P__F = 4
val P__D = (List((P__F, TPCHLoader.loadPart.toList)), ())
 case class Record415(lbl: Q1Flat)
case class Record420(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record421(c_name: String, c_orders: Record420)
case class Record422(_1: Q1Flat, _2: List[Record421])
case class Record425(lbl: Record420)
case class Record430(o__F: Orders, P__F: Int, L__F: Int)
case class Record431(o_orderdate: String, o_parts: Record430)
case class Record432(_1: Record420, _2: List[Record431])
case class Record435(lbl: Record430)
case class Record443(p_name: String, l_qty: Double)
case class Record444(_1: Record430, _2: List[Record443])
case class Record528(lbl: Q1Flat2)
case class Input_Q1__DFlat530(c_name: String, c_orders: Int)
case class Flat531(o_orderdate: String, o_parts: Int)
case class Dict531(o_parts: (List[(Int, List[Record443])], Unit))
case class Input_Q1__DDict530(c_orders: (List[(Int, List[Flat531])], Dict531))
case class Record544(l_qty: Double)
case class Record546(c_name: String, p_name: String, month: String, t_qty: Int)
case class Record547(_1: Q1Flat2, _2: List[Record546])
 var start0 = System.currentTimeMillis()
 val x299 = Q1Flat(P__F, C__F, L__F, O__F)
val x300 = Record415(x299)
val x301 = List(x300)
val x302 = x301
val x325 = x302.flatMap(x303 =>  
  {  val x304 = x303.lbl
    val x305 = C__D._1
    val x322 = x305.flatMap(x306 =>  
      if({val x307 = x303.lbl
      val x308 = x307.C__F
      val x309 = x306._1
      val x310 = x308 == x309
      x310}) {  val x311 = x306._2
        val x321 = x311.flatMap(x312 =>  
          {  val x313 = x312.c_name
            val x314 = x303.lbl
            val x315 = x314.P__F
            val x316 = x314.L__F
            val x317 = x314.O__F
            val x318 = Record420(x315, x316, x317, x312)
            val x319 = Record421(x313, x318)
            val x320 = List(x319)
            x320})
        x321} else {  Nil})
    val x323 = Record422(x304, x322)
    val x324 = List(x323)
    x324})
val x326 = x325
val x334 = x326.flatMap(x327 =>  
  {  val x328 = x327._2
    val x333 = x328.flatMap(x329 =>  
      {  val x330 = x329.c_orders
        val x331 = Record425(x330)
        val x332 = List(x331)
        x332})
    x333})
val x335 = x334.distinct
val x336 = x335
val x363 = x336.flatMap(x337 =>  
  {  val x338 = x337.lbl
    val x339 = O__D._1
    val x360 = x339.flatMap(x340 =>  
      if({val x341 = x337.lbl
      val x342 = x341.O__F
      val x343 = x340._1
      val x344 = x342 == x343
      x344}) {  val x345 = x340._2
        val x359 = x345.flatMap(x346 =>  
          if({val x347 = x346.o_custkey
          val x348 = x337.lbl
          val x349 = x348.c__F
          val x350 = x349.c_custkey
          val x351 = x347 == x350
          x351}) {  val x352 = x346.o_orderdate
            val x353 = x337.lbl
            val x354 = x353.P__F
            val x355 = x353.L__F
            val x356 = Record430(x346, x354, x355)
            val x357 = Record431(x352, x356)
            val x358 = List(x357)
            x358} else {  Nil})
        x359} else {  Nil})
    val x361 = Record432(x338, x360)
    val x362 = List(x361)
    x362})
val x364 = x363
val x372 = x364.flatMap(x365 =>  
  {  val x366 = x365._2
    val x371 = x366.flatMap(x367 =>  
      {  val x368 = x367.o_parts
        val x369 = Record435(x368)
        val x370 = List(x369)
        x370})
    x371})
val x373 = x372.distinct
val x374 = x373
val x412 = x374.flatMap(x375 =>  
  {  val x376 = x375.lbl
    val x377 = L__D._1
    val x409 = x377.flatMap(x378 =>  
      if({val x379 = x375.lbl
      val x380 = x379.L__F
      val x381 = x378._1
      val x382 = x380 == x381
      x382}) {  val x383 = x378._2
        val x408 = x383.flatMap(x384 =>  
          {  val x385 = P__D._1
            val x407 = x385.flatMap(x386 =>  
              if({val x387 = x375.lbl
              val x388 = x387.P__F
              val x389 = x386._1
              val x390 = x388 == x389
              x390}) {  val x391 = x386._2
                val x406 = x391.flatMap(x392 =>  
                  if({val x393 = x384.l_orderkey
                  val x394 = x375.lbl
                  val x395 = x394.o__F
                  val x396 = x395.o_orderkey
                  val x397 = x393 == x396
                  val x398 = x384.l_partkey
                  val x399 = x392.p_partkey
                  val x400 = x398 == x399
                  val x401 = x397 && x400
                  x401}) {  val x402 = x392.p_name
                    val x403 = x384.l_quantity
                    val x404 = Record443(x402, x403)
                    val x405 = List(x404)
                    x405} else {  Nil})
                x406} else {  Nil})
            x407})
        x408} else {  Nil})
    val x410 = Record444(x376, x409)
    val x411 = List(x410)
    x411})
val x413 = x412
val x414 = (x302,x326,x336,x364,x374,x413)
x414
 
case class Input_Q1_Dict2(o_parts: (List[Record444], Unit))
case class Input_Q1_Dict1(c_orders: (List[Record432], Input_Q1_Dict2))
val Q1__F = x414._1.head.lbl
val Q1__D = (x414._2, Input_Q1_Dict1((x414._4, Input_Q1_Dict2((x414._6, Unit)))))
 var end0 = System.currentTimeMillis() - start0
 println("setup time: "+end0)
 def main(args: Array[String]){ 
    var time = List[Long]()
    for (i <- 1 to 5) {  
     var start = System.currentTimeMillis()
      f
      var end = System.currentTimeMillis() - start
      time = time :+ end
    }
    println(time)
    val avg = (time.sum/5)
    println(avg)
 }
 def f(){
  val x456 = Q1Flat2(Q1__F)
val x457 = Record528(x456)
val x458 = List(x457)
val x459 = x458
val x525 = x459.flatMap(x460 =>  
  {  val x461 = x460.lbl
    val x462 = Q1__D._1
    val x522 = x462.flatMap(x463 =>  
      if({val x464 = x460.lbl
      val x465 = x464.Q1__F
      val x466 = x463._1
      val x467 = x465 == x466
      x467}) {  val x468 = x463._2
        val x521 = x468.flatMap(x469 =>  
          {  val x470 = Q1__D._2
            val x471 = x470.c_orders
            val x472 = x471._1
            val x520 = x472.flatMap(x473 =>  
              if({val x474 = x469.c_orders
              val x475 = x473._1
              val x476 = x474 == x475
              x476}) {  val x477 = x473._2
                val x519 = x477.flatMap(x478 =>  
                  {  val x479 = Q1__D._2
                    val x480 = x479.c_orders
                    val x481 = x480._2
                    val x482 = x481.o_parts
                    val x483 = x482._1
                    val x518 = x483.flatMap(x484 =>  
                      if({val x485 = x478.o_parts
                      val x486 = x484._1
                      val x487 = x485 == x486
                      x487}) {  val x488 = x484._2
                        val x517 = x488.flatMap(x489 =>  
                          {  val x490 = x469.c_name
                            val x491 = x489.p_name
                            val x492 = x478.o_orderdate
                            val x493 = Q1__D._2
                            val x494 = x493.c_orders
                            val x495 = x494._2
                            val x496 = x495.o_parts
                            val x497 = x496._1
                            val x514 = x497.foldLeft(0)((acc542, x498) => 
                              if({val x499 = x478.o_parts
                              val x500 = x498._1
                              val x501 = x499 == x500
                              x501}) {  acc542 + {val x502 = x498._2
                                val x513 = x502.foldLeft(0)((acc543, x503) => 
                                  if({val x504 = x503.p_name
                                  val x505 = x489.p_name
                                  val x506 = x504 == x505
                                  x506}) {  acc543 + {val x507 = x489.l_qty
                                    val x508 = Record544(x507)
                                    val x509 = x503.l_qty
                                    val x510 = (1 to x509.asInstanceOf[Int]).map(v => x508)
                                    val x512 = x510.foldLeft(0)((acc545, x511) => 
                                      {  acc545 + {1}})
                                    x512}} else {  acc543})
                                x513}} else {  acc542})
                            val x515 = Record546(x490, x491, x492, x514)
                            val x516 = List(x515)
                            x516})
                        x517} else {  Nil})
                    x518})
                x519} else {  Nil})
            x520})
        x521} else {  Nil})
    val x523 = Record547(x461, x522)
    val x524 = List(x523)
    x524})
val x526 = x525
val x527 = (x459,x526)
x527
 }
}
