
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record270(p_name: String, l_qty: Double)
case class Record271(o_orderdate: String, o_parts: List[Record270])
case class Q1Out(c_name: String, c_orders: List[Record271])
case class Record388(l_qty: Double)
case class Record391(c_name: String, p_name: String, month: String, t_qty: Int)
object Query4 {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    val Q1 = { val x275 = C
val x277 = O
val x282 = { val hm323 = x275.groupBy{ case x278 => {
  val x280 = x278.c_custkey
  x280}}
x277.flatMap(x279 => hm323.get({val x281 = x279.o_custkey
x281}) match {
 case Some(a) => a.map(v => (v, x279))
 case _ => Nil
}) }
val x284 = L
val x290 = { val hm324 = x282.groupBy{ case (x285, x286) => {
  val x288 = x286.o_orderkey
  x288}}
x284.flatMap(x287 => hm324.get({val x289 = x287.l_orderkey
x289}) match {
 case Some(a) => a.map(v => (v, x287))
 case _ => Nil
}) }
val x292 = P
val x299 = { val hm325 = x290.groupBy{ case ((x293, x294), x295) => {
  val x297 = x295.l_partkey
  x297}}
x292.flatMap(x296 => hm325.get({val x298 = x296.p_partkey
x298}) match {
 case Some(a) => a.map(v => (v, x296))
 case _ => Nil
}) }
val x309 = { val grps326 = x299.groupBy{ case (((x300, x301), x302), x303) => { val x304 = (x300,x301)
x304 }}
 grps326.map(x308 => (x308._1, x308._2.map{case (((x300, x301), x302), x303) => val x305 = x303.p_name
val x306 = x302.l_quantity
val x307 = Record270(x305, x306)
x307})).toList }
val x317 = { val grps328 = x309.groupBy{ case ((x310, x311), x312) => { val x313 = (x310)
x313 }}
 grps328.map(x316 => (x316._1, x316._2.map{case ((x310, x311), x312) => val x314 = x311.o_orderdate
val x315 = Record271(x314, x312)
x315})).toList }
val x322 = x317.map{ case (x318, x319) => { 
  val x320 = x318.c_name
  val x321 = Q1Out(x320, x319)
  x321 }}
x322 }
    var end0 = System.currentTimeMillis() - start0
    
    def f(){
      val x338 = Q1
val x342 = x338.flatMap{ case x339 => 
  val x340 = x339.c_orders
  x340.map(x341 => {
  val nv384 = (x339, x341) 
     nv384
})}
val x347 = x342.flatMap{ case (x343, x344) => 
  val x345 = x344.o_parts
  x345.map(x346 => {
  val nv385 = ((x343, x344), x346) 
     nv385
})}
val x356 = x347.flatMap{ case ((x348, x349), x350) =>
  val x351 = x349.o_parts
  x351.withFilter{ case x352 =>
  {val x353 = x352.p_name
  val x354 = x350.p_name
  val x355 = x353 == x354
  x355}}.map(x352 => (((x348, x349), x350), x352))
}
val x366 = x356.flatMap{ case (((x357, x358), x359), x360) => 
  val x361 = x359.l_qty
  val x362 = Record388(x361)
  val x363 = x360.l_qty
  val x364 = (1 to x363.asInstanceOf[Int]).map(v => x362)
  x364.map(x365 => {
  val nv387 = ((((x357, x358), x359), x360), x365) 
     nv387
})}
val x374 = { val grps389 = x366.groupBy{ case ((((x367, x368), x369), x370), x371) => { val x372 = (x367,x368,x369)
x372 }}
 grps389.map(x373 => (x373._1, x373._2.foldLeft(0)((acc390, x373) => acc390 + 1))).toList }
val x383 = x374.map{ case ((x375, x376, x377), x378) => { 
  val x379 = x375.c_name
  val x380 = x377.p_name
  val x381 = x376.o_orderdate
  val x382 = Record391(x379, x380, x381, x378)
  x382 }}
x383
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
