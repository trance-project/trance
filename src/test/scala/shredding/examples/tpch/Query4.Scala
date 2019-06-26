
package experiments
/** Generated code **/
import shredding.examples.tpch._
object Query4 {
 case class Record514(p_name: String, l_qty: Double)
case class Record515(o_orderdate: String, o_parts: List[Record514])
case class Q1Out(c_name: String, c_orders: List[Record515])
case class Record574(o_orderdate: String, o_parts: Record514)
case class Record575(c_name: String, c_orders: Record574)
case class Record632(l_qty: Double)
case class Record636(c_name: String, p_name: String, month: String, t_qty: Int)
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
val x561 = { val grps572 = x553.groupBy{ case (x554, x555, x556) => { val x557 = (x554)
x557 }}
 grps572.map(x560 => (x560._1, x560._2.map{case (x554, x555, x556) => val x558 = x555.o_orderdate
val x559 = Record574(x558, x556)
x559})).toList }
val x566 = x561.map{ case (x562, x563) => { 
  val x564 = x562.c_name
  val x565 = Record575(x564, x563)
  x565 }}
x566 }
    var end0 = System.currentTimeMillis() - start0
    println("setup time: "+end0)
    var time = List[Long]()
    for (i <- 1 to 5) {  
     var start = System.currentTimeMillis()
      f(Q1)
      var end = System.currentTimeMillis() - start
      time = time :+ end
    }
    val avg = (time.sum/5)
    println(avg)
 }
 def f(Q1: List[Q1Out]){
  val x584 = Q1
val x588 = 
x584.flatMap{x585 => 
  val x586 = x585.c_orders
  x586.map(x587 => {
  val nv = Listx585 :+ x587 
   if (true) { nv } else { Nil }
)}
val x593 = 
x588.flatMap{(x589, x590) => 
  val x591 = x590.o_parts
  x591.map(x592 => {
  val nv = List(x589, x590) :+ x592 
   if (true) { nv } else { Nil }
)}
val x602 = 
x593.flatMap{((x594, x595), x596) => 
  val x597 = x595.o_parts
  x597.map(x598 => {
  val nv = List((x594, x595), x596) :+ x598 
   if (val x599 = x598.p_name
  val x600 = x596.p_name
  val x601 = x599 == x600
  x601) { nv } else { Nil }
)}
val x603 = x579.l_qty
val x604 = Record632(x603)
val x605 = x580.l_qty
val x606 = (1 to x605.asInstanceOf[Int]).map(v => x604)
val x608 = x606
val x614 = { val hm633 = x602.groupBy{ case (((x609, x610), x611), x612) => {
  true}}
x608.flatMap(x613 => hm633.get({true}) match {
 case Some(a) => a.map(v => (v, x613))
 case _ => Nil
}) }
val x622 = { val grps634 = x614.groupBy{ case ((((x615, x616), x617), x618), x619) => { val x620 = (x615,x616,x617)
x620 }}
 grps634.map(x621 => (x621._1, x621._2.foldLeft(0)(acc635, x621) => acc635 + 1))).toList
val x631 = x622.map{ case (x623, x624, x625, x626) => { 
  val x627 = x623.c_name
  val x628 = x625.p_name
  val x629 = x624.o_orderdate
  val x630 = Record636(x627, x628, x629, x626)
  x630 }}
x631
 }
}
