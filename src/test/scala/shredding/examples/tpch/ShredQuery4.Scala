
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
 case class Record385(lbl: Q1Flat)
case class Record387(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record388(c_name: String, c_orders: Record387)
case class Record389(_1: Q1Flat, _2: List[Record388])
case class Record390(lbl: Record387)
case class Record392(o__F: Orders, P__F: Int, L__F: Int)
case class Record393(o_orderdate: String, o_parts: Record392)
case class Record394(_1: Record387, _2: List[Record393])
case class Record395(lbl: Record392)
case class Record398(p_name: String, l_qty: Double)
case class Record399(_1: Record392, _2: List[Record398])
case class Record483(lbl: Q1Flat2)
case class Input_Q1__DFlat484(c_name: String, c_orders: Int)
case class Flat485(o_orderdate: String, o_parts: Int)
case class Dict485(o_parts: (List[(Int, List[Record398])], Unit))
case class Input_Q1__DDict484(c_orders: (List[(Int, List[Flat485])], Dict485))
case class Record490(l_qty: Double)
case class Record491(c_name: String, p_name: String, month: String, t_qty: Int)
case class Record492(_1: Q1Flat2, _2: List[Record491])
 val x269 = Q1Flat(P__F, C__F, L__F, O__F)
val x270 = Record385(x269)
val x271 = List(x270)
val x272 = x271
val x295 = x272.flatMap({x273 => 
   val x274 = x273.lbl
  val x275 = C__D._1
  val x292 = x275.withFilter({x276 => val x277 = x273.lbl
  val x278 = x277.C__F
  val x279 = x276._1
  val x280 = x278 == x279
  x280}).flatMap({x276 => 
     val x281 = x276._2
    val x291 = x281.flatMap({x282 => 
       val x283 = x282.c_name
      val x284 = x273.lbl
      val x285 = x284.P__F
      val x286 = x284.L__F
      val x287 = x284.O__F
      val x288 = Record387(x285, x286, x287, x282)
      val x289 = Record388(x283, x288)
      val x290 = List(x289)
      x290})
    x291})
  val x293 = Record389(x274, x292)
  val x294 = List(x293)
  x294})
val x296 = x295
val x304 = x296.flatMap({x297 => 
   val x298 = x297._2
  val x303 = x298.flatMap({x299 => 
     val x300 = x299.c_orders
    val x301 = Record390(x300)
    val x302 = List(x301)
    x302})
  x303})
val x305 = x304.distinct
val x306 = x305
val x333 = x306.flatMap({x307 => 
   val x308 = x307.lbl
  val x309 = O__D._1
  val x330 = x309.withFilter({x310 => val x311 = x307.lbl
  val x312 = x311.O__F
  val x313 = x310._1
  val x314 = x312 == x313
  x314}).flatMap({x310 => 
     val x315 = x310._2
    val x329 = x315.withFilter({x316 => val x317 = x316.o_custkey
    val x318 = x307.lbl
    val x319 = x318.c__F
    val x320 = x319.c_custkey
    val x321 = x317 == x320
    x321}).flatMap({x316 => 
       val x322 = x316.o_orderdate
      val x323 = x307.lbl
      val x324 = x323.P__F
      val x325 = x323.L__F
      val x326 = Record392(x316, x324, x325)
      val x327 = Record393(x322, x326)
      val x328 = List(x327)
      x328})
    x329})
  val x331 = Record394(x308, x330)
  val x332 = List(x331)
  x332})
val x334 = x333
val x342 = x334.flatMap({x335 => 
   val x336 = x335._2
  val x341 = x336.flatMap({x337 => 
     val x338 = x337.o_parts
    val x339 = Record395(x338)
    val x340 = List(x339)
    x340})
  x341})
val x343 = x342.distinct
val x344 = x343
val x382 = x344.flatMap({x345 => 
   val x346 = x345.lbl
  val x347 = L__D._1
  val x379 = x347.withFilter({x348 => val x349 = x345.lbl
  val x350 = x349.L__F
  val x351 = x348._1
  val x352 = x350 == x351
  x352}).flatMap({x348 => 
     val x353 = x348._2
    val x378 = x353.flatMap({x354 => 
       val x355 = P__D._1
      val x377 = x355.withFilter({x356 => val x357 = x345.lbl
      val x358 = x357.P__F
      val x359 = x356._1
      val x360 = x358 == x359
      x360}).flatMap({x356 => 
         val x361 = x356._2
        val x376 = x361.withFilter({x362 => val x363 = x354.l_orderkey
        val x364 = x345.lbl
        val x365 = x364.o__F
        val x366 = x365.o_orderkey
        val x367 = x363 == x366
        val x368 = x354.l_partkey
        val x369 = x362.p_partkey
        val x370 = x368 == x369
        val x371 = x367 && x370
        x371}).flatMap({x362 => 
           val x372 = x362.p_name
          val x373 = x354.l_quantity
          val x374 = Record398(x372, x373)
          val x375 = List(x374)
          x375})
        x376})
      x377})
    x378})
  val x380 = Record399(x346, x379)
  val x381 = List(x380)
  x381})
val x383 = x382
val x384 = (x272,x296,x306,x334,x344,x383)
x384
 
case class Input_Q1_Dict2(o_parts: (List[Record399], Unit))
case class Input_Q1_Dict1(c_orders: (List[Record394], Input_Q1_Dict2))
val Q1__F = x384._1.head.lbl
val Q1__D = (x384._2, Input_Q1_Dict1((x384._4, Input_Q1_Dict2((x384._6, Unit)))))
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
  val x411 = Q1Flat2(Q1__F)
val x412 = Record483(x411)
val x413 = List(x412)
val x414 = x413
val x480 = x414.flatMap({x415 =>
   val x416 = x415.lbl
  val x417 = Q1__D._1
  val x477 = x417.withFilter({x418 => val x419 = x415.lbl
  val x420 = x419.Q1__F
  val x421 = x418._1
  val x422 = x420 == x421
  x422}).flatMap({x418 =>
     val x423 = x418._2
    val x476 = x423.flatMap({x424 =>
       val x425 = Q1__D._2
      val x426 = x425.c_orders
      val x427 = x426._1
      val x475 = x427.withFilter({x428 => val x429 = x424.c_orders
      val x430 = x428._1
      val x431 = x429 == x430
      x431}).flatMap({x428 =>
         val x432 = x428._2
        val x474 = x432.flatMap({x433 =>
           val x434 = Q1__D._2
          val x435 = x434.c_orders
          val x436 = x435._2
          val x437 = x436.o_parts
          val x438 = x437._1
          val x473 = x438.withFilter({x439 => val x440 = x433.o_parts
          val x441 = x439._1
          val x442 = x440 == x441
          x442}).flatMap({x439 =>
             val x443 = x439._2
            val x472 = x443.flatMap({x444 =>
               val x445 = x424.c_name
              val x446 = x444.p_name
              val x447 = x433.o_orderdate
              val x448 = Q1__D._2
              val x449 = x448.c_orders
              val x450 = x449._2
              val x451 = x450.o_parts
              val x452 = x451._1
              val x469 = x452.withFilter({x453 => val x454 = x433.o_parts
              val x455 = x453._1
              val x456 = x454 == x455
              x456}).flatMap({x453 =>
                 val x457 = x453._2
                val x468 = x457.withFilter({x458 => val x459 = x458.p_name
                val x460 = x444.p_name
                val x461 = x459 == x460
                x461}).flatMap({x458 =>
                   val x462 = x444.l_qty
                  val x463 = Record490(x462)
                  val x464 = x458.l_qty
                  val x465 = (1 to x464.asInstanceOf[Int]).map(v => x463)
                  val x467 = x465.map({x466 => 1})
                  x467})
                x468}).sum
              val x470 = Record491(x445, x446, x447, x469)
              val x471 = List(x470)
              x471})
            x472})
          x473})
        x474})
      x475})
    x476})
  val x478 = Record492(x416, x477)
  val x479 = List(x478)
  x479})
val x481 = x480
val x482 = (x414,x481)
x482
 }
}
