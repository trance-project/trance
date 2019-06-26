
package experiments
/** Generated code **/
import shredding.examples.tpch._
object Query4 {
 case class Record514(p_name: String, l_qty: Double)
case class Record515(o_orderdate: String, o_parts: List[Record514])
case class Q1Out(c_name: String, c_orders: List[Record515])
case class Record633(l_qty: Double)
case class Record637(c_name: String, p_name: String, month: String, t_qty: Int)
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    val Q1 = { val x519 = C
val x521 = O
val x526 = { val hm567 = x519.groupBy{ case x522 => {
  val x524 = x522.c_custkey
  x524}}
x521.flatMap(x523 => hm567.get({val x525 = x523.o_custkey
x525}) match {
 case Some(a) => a.map(v => (v, x523))
 case _ => Nil
}) }
val x528 = L
val x534 = { val hm568 = x526.groupBy{ case (x529, x530) => {
  val x532 = x530.o_orderkey
  x532}}
x528.flatMap(x531 => hm568.get({val x533 = x531.l_orderkey
x533}) match {
 case Some(a) => a.map(v => (v, x531))
 case _ => Nil
}) }
val x536 = P
val x543 = { val hm569 = x534.groupBy{ case ((x537, x538), x539) => {
  val x541 = x539.l_partkey
  x541}}
x536.flatMap(x540 => hm569.get({val x542 = x540.p_partkey
x542}) match {
 case Some(a) => a.map(v => (v, x540))
 case _ => Nil
}) }
val x553 = { val grps570 = x543.groupBy{ case (((x544, x545), x546), x547) => { val x548 = (x544,x545)
x548 }}
 grps570.map(x552 => (x552._1, x552._2.map{case (((x544, x545), x546), x547) => val x549 = x547.p_name
val x550 = x546.l_quantity
val x551 = Record514(x549, x550)
x551})).toList }
val x561 = { val grps572 = x553.groupBy{ case ((x554, x555), x556) => { val x557 = (x554)
x557 }}
 grps572.map(x560 => (x560._1, x560._2.map{case ((x554, x555), x556) => val x558 = x555.o_orderdate
val x559 = Record515(x558, x556)
x559})).toList }
val x566 = x561.map{ case (x562, x563) => { 
  val x564 = x562.c_name
  val x565 = Q1Out(x564, x563)
  x565 }}
x566 }
    var end0 = System.currentTimeMillis() - start0
    var time = List[Long]()
    for (i <- 1 to 5) {  
     var start = System.currentTimeMillis()
      f(Q1)
      var end = System.currentTimeMillis() - start
      time = time :+ end
    }
    val avg = (time.sum/5)
    println(end0+","+avg)
 }
 def f(Q1: List[Q1Out]){
  val x582 = Q1
val x586 = x582.flatMap{ case x583 => 
  val x584 = x583.c_orders
  x584.map(x585 => {
  val nv630 = (x583, x585) 
     nv630
})}
val x591 = x586.flatMap{ case (x587, x588) => 
  val x589 = x588.o_parts
  x589.map(x590 => {
  val nv631 = ((x587, x588), x590) 
     nv631
})}
val x600 = x591.flatMap{ case ((x592, x593), x594) => 
  val x595 = x593.o_parts
  x595.map(x596 => {
  val nv632 = (((x592, x593), x594), x596) 
   if({val x597 = x596.p_name
  val x598 = x594.p_name
  val x599 = x597 == x598
  x599}) {  nv632} else {  Nil}
})}
val x601 = x577.l_qty
val x602 = Record633(x601)
val x603 = x578.l_qty
val x604 = (1 to x603.asInstanceOf[Int]).map(v => x602)
val x606 = x604
val x612 = { val hm634 = x600.groupBy{ case (((x607, x608), x609), x610) => {
  true}}
x606.flatMap(x611 => hm634.get({true}) match {
 case Some(a) => a.map(v => (v, x611))
 case _ => Nil
}) }
val x620 = { val grps635 = x612.groupBy{ case ((((x613, x614), x615), x616), x617) => { val x618 = (x613,x614,x615)
x618 }}
 grps635.map(x619 => (x619._1, x619._2.foldLeft(0)(acc636, x619) => acc636 + 1))).toList
val x629 = x620.map{ case ((x621, x622, x623), x624) => { 
  val x625 = x621.c_name
  val x626 = x623.p_name
  val x627 = x622.o_orderdate
  val x628 = Record637(x625, x626, x627, x624)
  x628 }}
x629
 }
}
