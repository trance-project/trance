
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
 case class Record411(lbl: Q1Flat)
case class Record416(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record417(c_name: String, c_orders: Record416)
case class Record418(_1: Q1Flat, _2: List[Record417])
case class Record421(lbl: Record416)
case class Record426(o__F: Orders, P__F: Int, L__F: Int)
case class Record427(o_orderdate: String, o_parts: Record426)
case class Record428(_1: Record416, _2: List[Record427])
case class Record431(lbl: Record426)
case class Record439(p_name: String, l_qty: Double)
case class Record440(_1: Record426, _2: List[Record439])
case class Record524(lbl: Q1Flat2)
case class Input_Q1__DFlat526(c_name: String, c_orders: Int)
case class Flat527(o_orderdate: String, o_parts: Int)
case class Dict527(o_parts: (List[(Int, List[Record439])], Unit))
case class Input_Q1__DDict526(c_orders: (List[(Int, List[Flat527])], Dict527))
case class Record540(l_qty: Double)
case class Record542(c_name: String, p_name: String, month: String, t_qty: Int)
case class Record543(_1: Q1Flat2, _2: List[Record542])
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    val x296 = Q1Flat(P__F, C__F, L__F, O__F)
val x297 = Record411(x296)
val x298 = List(x297)
val x299 = x298
val x322 = x299.flatMap(x300 =>  
  {  val x301 = x300.lbl
    val x302 = C__D._1
    val x319 = x302.flatMap(x303 =>  
      if({val x304 = x300.lbl
      val x305 = x304.C__F
      val x306 = x303._1
      val x307 = x305 == x306
      x307}) {  val x308 = x303._2
        val x318 = x308.flatMap(x309 =>  
          {  val x310 = x309.c_name
            val x311 = x300.lbl
            val x312 = x311.P__F
            val x313 = x311.L__F
            val x314 = x311.O__F
            val x315 = Record416(x312, x313, x314, x309)
            val x316 = Record417(x310, x315)
            val x317 = List(x316)
            x317})
        x318} else {  Nil})
    val x320 = Record418(x301, x319)
    val x321 = List(x320)
    x321})
val x323 = x322
val x331 = x323.flatMap(x324 =>  
  {  val x325 = x324._2
    val x330 = x325.flatMap(x326 =>  
      {  val x327 = x326.c_orders
        val x328 = Record421(x327)
        val x329 = List(x328)
        x329})
    x330})
val x332 = x331.distinct
val x333 = x332
val x360 = x333.flatMap(x334 =>  
  {  val x335 = x334.lbl
    val x336 = O__D._1
    val x357 = x336.flatMap(x337 =>  
      if({val x338 = x334.lbl
      val x339 = x338.O__F
      val x340 = x337._1
      val x341 = x339 == x340
      x341}) {  val x342 = x337._2
        val x356 = x342.flatMap(x343 =>  
          if({val x344 = x343.o_custkey
          val x345 = x334.lbl
          val x346 = x345.c__F
          val x347 = x346.c_custkey
          val x348 = x344 == x347
          x348}) {  val x349 = x343.o_orderdate
            val x350 = x334.lbl
            val x351 = x350.P__F
            val x352 = x350.L__F
            val x353 = Record426(x343, x351, x352)
            val x354 = Record427(x349, x353)
            val x355 = List(x354)
            x355} else {  Nil})
        x356} else {  Nil})
    val x358 = Record428(x335, x357)
    val x359 = List(x358)
    x359})
val x361 = x360
val x369 = x361.flatMap(x362 =>  
  {  val x363 = x362._2
    val x368 = x363.flatMap(x364 =>  
      {  val x365 = x364.o_parts
        val x366 = Record431(x365)
        val x367 = List(x366)
        x367})
    x368})
val x370 = x369.distinct
val x371 = x370
val x408 = x371.flatMap(x372 =>  
  {  val x373 = x372.lbl
    val x374 = L__D._1
    val x405 = x374.flatMap(x375 =>  
      if({val x376 = x372.lbl
      val x377 = x376.L__F
      val x378 = x375._1
      val x379 = x377 == x378
      x379}) {  val x380 = x375._2
        val x404 = x380.flatMap(x381 =>  
          if({val x382 = x381.l_orderkey
          val x383 = x372.lbl
          val x384 = x383.o__F
          val x385 = x384.o_orderkey
          val x386 = x382 == x385
          x386}) {  val x387 = P__D._1
            val x403 = x387.flatMap(x388 =>  
              if({val x389 = x372.lbl
              val x390 = x389.P__F
              val x391 = x388._1
              val x392 = x390 == x391
              x392}) {  val x393 = x388._2
                val x402 = x393.flatMap(x394 =>  
                  if({val x395 = x381.l_partkey
                  val x396 = x394.p_partkey
                  val x397 = x395 == x396
                  x397}) {  val x398 = x394.p_name
                    val x399 = x381.l_quantity
                    val x400 = Record439(x398, x399)
                    val x401 = List(x400)
                    x401} else {  Nil})
                x402} else {  Nil})
            x403} else {  Nil})
        x404} else {  Nil})
    val x406 = Record440(x373, x405)
    val x407 = List(x406)
    x407})
val x409 = x408
val x410 = (x299,x323,x333,x361,x371,x409)
x410
    
case class Input_Q1_Dict2(o_parts: (List[Record442], Unit))
case class Input_Q1_Dict1(c_orders: (List[Record430], Input_Q1_Dict2))
val Q1__F = x410._1.head.lbl
val Q1__D = (x410._2, Input_Q1_Dict1((x410._4, Input_Q1_Dict2((x410._6, Unit)))))
    var end0 = System.currentTimeMillis() - start0
    println("setup time: "+end0)
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
  val x452 = Q1Flat2(Q1__F)
val x453 = Record524(x452)
val x454 = List(x453)
val x455 = x454
val x521 = x455.flatMap(x456 =>  
  {  val x457 = x456.lbl
    val x458 = Q1__D._1
    val x518 = x458.flatMap(x459 =>  
      if({val x460 = x456.lbl
      val x461 = x460.Q1__F
      val x462 = x459._1
      val x463 = x461 == x462
      x463}) {  val x464 = x459._2
        val x517 = x464.flatMap(x465 =>  
          {  val x466 = Q1__D._2
            val x467 = x466.c_orders
            val x468 = x467._1
            val x516 = x468.flatMap(x469 =>  
              if({val x470 = x465.c_orders
              val x471 = x469._1
              val x472 = x470 == x471
              x472}) {  val x473 = x469._2
                val x515 = x473.flatMap(x474 =>  
                  {  val x475 = Q1__D._2
                    val x476 = x475.c_orders
                    val x477 = x476._2
                    val x478 = x477.o_parts
                    val x479 = x478._1
                    val x514 = x479.flatMap(x480 =>  
                      if({val x481 = x474.o_parts
                      val x482 = x480._1
                      val x483 = x481 == x482
                      x483}) {  val x484 = x480._2
                        val x513 = x484.flatMap(x485 =>  
                          {  val x486 = x465.c_name
                            val x487 = x485.p_name
                            val x488 = x474.o_orderdate
                            val x489 = Q1__D._2
                            val x490 = x489.c_orders
                            val x491 = x490._2
                            val x492 = x491.o_parts
                            val x493 = x492._1
                            val x510 = x493.foldLeft(0)((acc538, x494) => 
                              if({val x495 = x474.o_parts
                              val x496 = x494._1
                              val x497 = x495 == x496
                              x497}) {  acc538 + {val x498 = x494._2
                                val x509 = x498.foldLeft(0)((acc539, x499) => 
                                  if({val x500 = x499.p_name
                                  val x501 = x485.p_name
                                  val x502 = x500 == x501
                                  x502}) {  acc539 + {val x503 = x485.l_qty
                                    val x504 = Record540(x503)
                                    val x505 = x499.l_qty
                                    val x506 = (1 to x505.asInstanceOf[Int]).map(v => x504)
                                    val x508 = x506.foldLeft(0)((acc541, x507) => 
                                      {  acc541 + {1}})
                                    x508}} else {  acc539})
                                x509}} else {  acc538})
                            val x511 = Record542(x486, x487, x488, x510)
                            val x512 = List(x511)
                            x512})
                        x513} else {  Nil})
                    x514})
                x515} else {  Nil})
            x516})
        x517} else {  Nil})
    val x519 = Record543(x457, x518)
    val x520 = List(x519)
    x520})
val x522 = x521
val x523 = (x455,x522)
x523
 }
}
