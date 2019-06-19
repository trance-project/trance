
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
 case class Record413(lbl: Q1Flat)
case class Record418(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record419(c_name: String, c_orders: Record418)
case class Record420(_1: Q1Flat, _2: List[Record419])
case class Record423(lbl: Record418)
case class Record428(o__F: Orders, P__F: Int, L__F: Int)
case class Record429(o_orderdate: String, o_parts: Record428)
case class Record430(_1: Record418, _2: List[Record429])
case class Record433(lbl: Record428)
case class Record441(p_name: String, l_qty: Double)
case class Record442(_1: Record428, _2: List[Record441])
case class Record526(lbl: Q1Flat2)
case class Input_Q1__DFlat528(c_name: String, c_orders: Int)
case class Flat529(o_orderdate: String, o_parts: Int)
case class Dict529(o_parts: (List[(Int, List[Record441])], Unit))
case class Input_Q1__DDict528(c_orders: (List[(Int, List[Flat529])], Dict529))
case class Record542(l_qty: Double)
case class Record544(c_name: String, p_name: String, month: String, t_qty: Int)
case class Record545(_1: Q1Flat2, _2: List[Record544])
 val x297 = Q1Flat(P__F, C__F, L__F, O__F)
val x298 = Record413(x297)
val x299 = List(x298)
val x300 = x299
val x323 = x300.flatMap(x301 =>  
  {  val x302 = x301.lbl
    val x303 = C__D._1
    val x320 = x303.flatMap(x304 =>  
      if({val x305 = x301.lbl
      val x306 = x305.C__F
      val x307 = x304._1
      val x308 = x306 == x307
      x308}) {  val x309 = x304._2
        val x319 = x309.flatMap(x310 =>  
          {  val x311 = x310.c_name
            val x312 = x301.lbl
            val x313 = x312.P__F
            val x314 = x312.L__F
            val x315 = x312.O__F
            val x316 = Record418(x313, x314, x315, x310)
            val x317 = Record419(x311, x316)
            val x318 = List(x317)
            x318})
        x319} else {  Nil})
    val x321 = Record420(x302, x320)
    val x322 = List(x321)
    x322})
val x324 = x323
val x332 = x324.flatMap(x325 =>  
  {  val x326 = x325._2
    val x331 = x326.flatMap(x327 =>  
      {  val x328 = x327.c_orders
        val x329 = Record423(x328)
        val x330 = List(x329)
        x330})
    x331})
val x333 = x332.distinct
val x334 = x333
val x361 = x334.flatMap(x335 =>  
  {  val x336 = x335.lbl
    val x337 = O__D._1
    val x358 = x337.flatMap(x338 =>  
      if({val x339 = x335.lbl
      val x340 = x339.O__F
      val x341 = x338._1
      val x342 = x340 == x341
      x342}) {  val x343 = x338._2
        val x357 = x343.flatMap(x344 =>  
          if({val x345 = x344.o_custkey
          val x346 = x335.lbl
          val x347 = x346.c__F
          val x348 = x347.c_custkey
          val x349 = x345 == x348
          x349}) {  val x350 = x344.o_orderdate
            val x351 = x335.lbl
            val x352 = x351.P__F
            val x353 = x351.L__F
            val x354 = Record428(x344, x352, x353)
            val x355 = Record429(x350, x354)
            val x356 = List(x355)
            x356} else {  Nil})
        x357} else {  Nil})
    val x359 = Record430(x336, x358)
    val x360 = List(x359)
    x360})
val x362 = x361
val x370 = x362.flatMap(x363 =>  
  {  val x364 = x363._2
    val x369 = x364.flatMap(x365 =>  
      {  val x366 = x365.o_parts
        val x367 = Record433(x366)
        val x368 = List(x367)
        x368})
    x369})
val x371 = x370.distinct
val x372 = x371
val x410 = x372.flatMap(x373 =>  
  {  val x374 = x373.lbl
    val x375 = L__D._1
    val x407 = x375.flatMap(x376 =>  
      if({val x377 = x373.lbl
      val x378 = x377.L__F
      val x379 = x376._1
      val x380 = x378 == x379
      x380}) {  val x381 = x376._2
        val x406 = x381.flatMap(x382 =>  
          {  val x383 = P__D._1
            val x405 = x383.flatMap(x384 =>  
              if({val x385 = x373.lbl
              val x386 = x385.P__F
              val x387 = x384._1
              val x388 = x386 == x387
              x388}) {  val x389 = x384._2
                val x404 = x389.flatMap(x390 =>  
                  if({val x391 = x382.l_orderkey
                  val x392 = x373.lbl
                  val x393 = x392.o__F
                  val x394 = x393.o_orderkey
                  val x395 = x391 == x394
                  val x396 = x382.l_partkey
                  val x397 = x390.p_partkey
                  val x398 = x396 == x397
                  val x399 = x395 && x398
                  x399}) {  val x400 = x390.p_name
                    val x401 = x382.l_quantity
                    val x402 = Record441(x400, x401)
                    val x403 = List(x402)
                    x403} else {  Nil})
                x404} else {  Nil})
            x405})
        x406} else {  Nil})
    val x408 = Record442(x374, x407)
    val x409 = List(x408)
    x409})
val x411 = x410
val x412 = (x300,x324,x334,x362,x372,x411)
x412
 
case class Input_Q1_Dict2(o_parts: (List[Record442], Unit))
case class Input_Q1_Dict1(c_orders: (List[Record430], Input_Q1_Dict2))
val Q1__F = x412._1.head.lbl
val Q1__D = (x412._2, Input_Q1_Dict1((x412._4, Input_Q1_Dict2((x412._6, Unit)))))
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
  val x454 = Q1Flat2(Q1__F)
val x455 = Record526(x454)
val x456 = List(x455)
val x457 = x456
val x523 = x457.flatMap(x458 =>  
  {  val x459 = x458.lbl
    val x460 = Q1__D._1
    val x520 = x460.flatMap(x461 =>  
      if({val x462 = x458.lbl
      val x463 = x462.Q1__F
      val x464 = x461._1
      val x465 = x463 == x464
      x465}) {  val x466 = x461._2
        val x519 = x466.flatMap(x467 =>  
          {  val x468 = Q1__D._2
            val x469 = x468.c_orders
            val x470 = x469._1
            val x518 = x470.flatMap(x471 =>  
              if({val x472 = x467.c_orders
              val x473 = x471._1
              val x474 = x472 == x473
              x474}) {  val x475 = x471._2
                val x517 = x475.flatMap(x476 =>  
                  {  val x477 = Q1__D._2
                    val x478 = x477.c_orders
                    val x479 = x478._2
                    val x480 = x479.o_parts
                    val x481 = x480._1
                    val x516 = x481.flatMap(x482 =>  
                      if({val x483 = x476.o_parts
                      val x484 = x482._1
                      val x485 = x483 == x484
                      x485}) {  val x486 = x482._2
                        val x515 = x486.flatMap(x487 =>  
                          {  val x488 = x467.c_name
                            val x489 = x487.p_name
                            val x490 = x476.o_orderdate
                            val x491 = Q1__D._2
                            val x492 = x491.c_orders
                            val x493 = x492._2
                            val x494 = x493.o_parts
                            val x495 = x494._1
                            val x512 = x495.foldLeft(0)((acc540, x496) => 
                              if({val x497 = x476.o_parts
                              val x498 = x496._1
                              val x499 = x497 == x498
                              x499}) {  acc540 + {val x500 = x496._2
                                val x511 = x500.foldLeft(0)((acc541, x501) => 
                                  if({val x502 = x501.p_name
                                  val x503 = x487.p_name
                                  val x504 = x502 == x503
                                  x504}) {  acc541 + {val x505 = x487.l_qty
                                    val x506 = Record542(x505)
                                    val x507 = x501.l_qty
                                    val x508 = (1 to x507.asInstanceOf[Int]).map(v => x506)
                                    val x510 = x508.foldLeft(0)((acc543, x509) => 
                                      {  acc543 + {1}})
                                    x510}} else {  acc541})
                                x511}} else {  acc540})
                            val x513 = Record544(x488, x489, x490, x512)
                            val x514 = List(x513)
                            x514})
                        x515} else {  Nil})
                    x516})
                x517} else {  Nil})
            x518})
        x519} else {  Nil})
    val x521 = Record545(x459, x520)
    val x522 = List(x521)
    x522})
val x524 = x523
val x525 = (x457,x524)
x525
 }
}
