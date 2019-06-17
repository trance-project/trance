
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
 case class Record388(P__F: Int, C__F: Int, L__F: Int, O__F: Int)
case class Record389(lbl: Record388)
case class Record391(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record392(c_name: String, c_orders: Record391)
case class Record393(k: Record388, v: List[Record392])
case class Record394(lbl: Record391)
case class Record396(o__F: Orders, P__F: Int, L__F: Int)
case class Record397(o_orderdate: String, o_parts: Record396)
case class Record398(k: Record391, v: List[Record397])
case class Record399(lbl: Record396)
case class Record402(p_name: String, l_qty: Double)
case class Record403(k: Record396, v: List[Record402])
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
  val x272 = Record388(P__F, C__F, L__F, O__F)
val x273 = Record389(x272)
val x274 = List(x273)
val x275 = x274
val x298 = x275.flatMap({x276 => 
   val x277 = x276.lbl
  val x278 = C__D._1
  val x295 = x278.withFilter({x279 => val x280 = x276.lbl
  val x281 = x280.C__F
  val x282 = x279._1
  val x283 = x281 == x282
  x283}).flatMap({x279 => 
     val x284 = x279._2
    val x294 = x284.flatMap({x285 => 
       val x286 = x285.c_name
      val x287 = x276.lbl
      val x288 = x287.P__F
      val x289 = x287.L__F
      val x290 = x287.O__F
      val x291 = Record391(x288, x289, x290, x285)
      val x292 = Record392(x286, x291)
      val x293 = List(x292)
      x293})
    x294})
  val x296 = Record393(x277, x295)
  val x297 = List(x296)
  x297})
val x299 = x298
val x307 = x299.flatMap({x300 => 
   val x301 = x300.v
  val x306 = x301.flatMap({x302 => 
     val x303 = x302.c_orders
    val x304 = Record394(x303)
    val x305 = List(x304)
    x305})
  x306})
val x308 = x307.distinct
val x309 = x308
val x336 = x309.flatMap({x310 => 
   val x311 = x310.lbl
  val x312 = O__D._1
  val x333 = x312.withFilter({x313 => val x314 = x310.lbl
  val x315 = x314.O__F
  val x316 = x313._1
  val x317 = x315 == x316
  x317}).flatMap({x313 => 
     val x318 = x313._2
    val x332 = x318.withFilter({x319 => val x320 = x319.o_custkey
    val x321 = x310.lbl
    val x322 = x321.c__F
    val x323 = x322.c_custkey
    val x324 = x320 == x323
    x324}).flatMap({x319 => 
       val x325 = x319.o_orderdate
      val x326 = x310.lbl
      val x327 = x326.P__F
      val x328 = x326.L__F
      val x329 = Record396(x319, x327, x328)
      val x330 = Record397(x325, x329)
      val x331 = List(x330)
      x331})
    x332})
  val x334 = Record398(x311, x333)
  val x335 = List(x334)
  x335})
val x337 = x336
val x345 = x337.flatMap({x338 => 
   val x339 = x338.v
  val x344 = x339.flatMap({x340 => 
     val x341 = x340.o_parts
    val x342 = Record399(x341)
    val x343 = List(x342)
    x343})
  x344})
val x346 = x345.distinct
val x347 = x346
val x385 = x347.flatMap({x348 => 
   val x349 = x348.lbl
  val x350 = L__D._1
  val x382 = x350.withFilter({x351 => val x352 = x348.lbl
  val x353 = x352.L__F
  val x354 = x351._1
  val x355 = x353 == x354
  x355}).flatMap({x351 => 
     val x356 = x351._2
    val x381 = x356.flatMap({x357 => 
       val x358 = P__D._1
      val x380 = x358.withFilter({x359 => val x360 = x348.lbl
      val x361 = x360.P__F
      val x362 = x359._1
      val x363 = x361 == x362
      x363}).flatMap({x359 => 
         val x364 = x359._2
        val x379 = x364.withFilter({x365 => val x366 = x357.l_orderkey
        val x367 = x348.lbl
        val x368 = x367.o__F
        val x369 = x368.o_orderkey
        val x370 = x366 == x369
        val x371 = x357.l_partkey
        val x372 = x365.p_partkey
        val x373 = x371 == x372
        val x374 = x370 && x373
        x374}).flatMap({x365 => 
           val x375 = x365.p_name
          val x376 = x357.l_quantity
          val x377 = Record402(x375, x376)
          val x378 = List(x377)
          x378})
        x379})
      x380})
    x381})
  val x383 = Record403(x349, x382)
  val x384 = List(x383)
  x384})
val x386 = x385
val x387 = x275
x299
x309
x337
x347
x386
x387
 }
}
