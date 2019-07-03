
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record521(p_name: String, l_qty: Double)
case class Record522(o_orderdate: String, o_parts: List[Record521])
case class Q1Out(c_name: String, c_orders: List[Record522])
case class Record629(c_name: String, p_name: String, month: String, t_qty: Double)
object Query4 {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    val Q1 = { val x526 = C
val x528 = O
val x533 = { val hm574 = x526.groupBy{ case x529 => {val x531 = x529.c_custkey
x531} }
x528.flatMap(x530 => hm574.get({val x532 = x530.o_custkey
x532}) match {
 case Some(a) => a.map(v => (v, x530))
 case _ => Nil
}) }
val x535 = L
val x541 = { val hm575 = x533.groupBy{ case (x536, x537) => {val x539 = x537.o_orderkey
x539} }
x535.flatMap(x538 => hm575.get({val x540 = x538.l_orderkey
x540}) match {
 case Some(a) => a.map(v => (v, x538))
 case _ => Nil
}) }
val x543 = P
val x550 = { val hm576 = x541.groupBy{ case ((x544, x545), x546) => {val x548 = x546.l_partkey
x548} }
x543.flatMap(x547 => hm576.get({val x549 = x547.p_partkey
x549}) match {
 case Some(a) => a.map(v => (v, x547))
 case _ => Nil
}) }
val x560 = { val grps577 = x550.groupBy{ case (((x551, x552), x553), x554) => { val x555 = (x551,x552)
x555 }}
 grps577.map(x559 => (x559._1, x559._2.map{case (((x551, x552), x553), x554) => val x556 = x554.p_name
val x557 = x553.l_quantity
val x558 = Record521(x556, x557)
x558})).toList }
val x568 = { val grps579 = x560.groupBy{ case ((x561, x562), x563) => { val x564 = (x561)
x564 }}
 grps579.map(x567 => (x567._1, x567._2.map{case ((x561, x562), x563) => val x565 = x562.o_orderdate
val x566 = Record522(x565, x563)
x566})).toList }
val x573 = x568.map{ case (x569, x570) => { 
  val x571 = x569.c_name
  val x572 = Q1Out(x571, x570)
  x572 }}
x573 }
    var end0 = System.currentTimeMillis() - start0
    
    def f(){
      val x588 = Q1
val x592 = x588.flatMap{ case x589 => 
  val x590 = x589.c_orders
  x590.map(x591 => {
  val nv624 = (x589, x591) 
     nv624
})}
val x597 = x592.flatMap{ case (x593, x594) => 
  val x595 = x594.o_parts
  x595.map(x596 => {
  val nv625 = ((x593, x594), x596) 
     nv625
})}
val x606 = x597.flatMap{ case ((x598, x599), x600) =>
  val x601 = x599.o_parts
  x601.withFilter{ case x602 =>
  {val x603 = x602.p_name
  val x604 = x600.p_name
  val x605 = x603 == x604
  x605}}.map(x602 => (((x598, x599), x600), x602))
}
val x614 = { val grps627 = x606.groupBy{ case (((x607, x608), x609), x610) => { val x611 = (x607,x608,x609)
x611 }}
 grps627.map(x613 => (x613._1, x613._2.foldLeft(0.0){ case (acc628, (((x607, x608), x609), x610)) => acc628 + {val x612 = x610.l_qty
x612} })).toList }
val x623 = x614.map{ case ((x615, x616, x617), x618) => { 
  val x619 = x615.c_name
  val x620 = x617.p_name
  val x621 = x616.o_orderdate
  val x622 = Record629(x619, x620, x621, x618)
  x622 }}
x623
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
