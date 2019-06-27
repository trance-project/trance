
package experiments
/** Generated code **/
import shredding.examples.tpch._
object Query4 {
 case class Record514(p_name: String, l_qty: Double)
case class Record515(o_orderdate: String, o_parts: List[Record514])
case class Q1Out(c_name: String, c_orders: List[Record515])
case class Record632(l_qty: Double)
case class Record635(c_name: String, p_name: String, month: String, t_qty: Int)
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
  val nv628 = (x583, x585) 
     nv628
})}
val x591 = x586.flatMap{ case (x587, x588) => 
  val x589 = x588.o_parts
  x589.map(x590 => {
  val nv629 = ((x587, x588), x590) 
     nv629
})}
val x600 = x591.flatMap{ case ((x592, x593), x594) => 
  val x595 = x593.o_parts
  x595.flatMap(x596 => {
  val nv630 = (((x592, x593), x594), x596) 
   if({val x597 = x596.p_name
  val x598 = x594.p_name
  val x599 = x597 == x598
  x599}) {  List(nv630)} else {  Nil}
})}
val x610 = x600.flatMap{ case (((x601, x602), x603), x604) => 
  val x605 = x603.l_qty
  val x606 = Record632(x605)
  val x607 = x604.l_qty
  val x608 = (1 to x607.asInstanceOf[Int]).map(v => x606)
  x608.map(x609 => {
  val nv631 = ((((x601, x602), x603), x604), x609) 
     nv631
})}
val x618 = { val grps633 = x610.groupBy{ case ((((x611, x612), x613), x614), x615) => { val x616 = (x611,x612,x613)
x616 }}
 grps633.map(x617 => (x617._1, x617._2.foldLeft(0)((acc634, x617) => acc634 + 1))).toList }
val x627 = x618.map{ case ((x619, x620, x621), x622) => { 
  val x623 = x619.c_name
  val x624 = x621.p_name
  val x625 = x620.o_orderdate
  val x626 = Record635(x623, x624, x625, x622)
  x626 }}
x627
 }
}
