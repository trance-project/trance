
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record516(p_name: String, l_qty: Double)
case class Record517(o_orderdate: String, o_parts: List[Record516])
case class Q1Out(c_name: String, c_orders: List[Record517])
case class Record634(l_qty: Double)
case class Record637(c_name: String, p_name: String, month: String, t_qty: Int)
object Query4 {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    val Q1 = { val x521 = C
val x523 = O
val x528 = { val hm569 = x521.groupBy{ case x524 => {
  val x526 = x524.c_custkey
  x526}}
x523.flatMap(x525 => hm569.get({val x527 = x525.o_custkey
x527}) match {
 case Some(a) => a.map(v => (v, x525))
 case _ => Nil
}) }
val x530 = L
val x536 = { val hm570 = x528.groupBy{ case (x531, x532) => {
  val x534 = x532.o_orderkey
  x534}}
x530.flatMap(x533 => hm570.get({val x535 = x533.l_orderkey
x535}) match {
 case Some(a) => a.map(v => (v, x533))
 case _ => Nil
}) }
val x538 = P
val x545 = { val hm571 = x536.groupBy{ case ((x539, x540), x541) => {
  val x543 = x541.l_partkey
  x543}}
x538.flatMap(x542 => hm571.get({val x544 = x542.p_partkey
x544}) match {
 case Some(a) => a.map(v => (v, x542))
 case _ => Nil
}) }
val x555 = { val grps572 = x545.groupBy{ case (((x546, x547), x548), x549) => { val x550 = (x546,x547)
x550 }}
 grps572.map(x554 => (x554._1, x554._2.map{case (((x546, x547), x548), x549) => val x551 = x549.p_name
val x552 = x548.l_quantity
val x553 = Record516(x551, x552)
x553})).toList }
val x563 = { val grps574 = x555.groupBy{ case ((x556, x557), x558) => { val x559 = (x556)
x559 }}
 grps574.map(x562 => (x562._1, x562._2.map{case ((x556, x557), x558) => val x560 = x557.o_orderdate
val x561 = Record517(x560, x558)
x561})).toList }
val x568 = x563.map{ case (x564, x565) => { 
  val x566 = x564.c_name
  val x567 = Q1Out(x566, x565)
  x567 }}
x568 }
    var end0 = System.currentTimeMillis() - start0
    
    def f(){
      val x584 = Q1
val x588 = x584.flatMap{ case x585 => 
  val x586 = x585.c_orders
  x586.map(x587 => {
  val nv630 = (x585, x587) 
     nv630
})}
val x593 = x588.flatMap{ case (x589, x590) => 
  val x591 = x590.o_parts
  x591.map(x592 => {
  val nv631 = ((x589, x590), x592) 
     nv631
})}
val x602 = x593.flatMap{ case ((x594, x595), x596) =>
  val x597 = x595.o_parts
  x597.withFilter{ case x598 =>
  {val x599 = x598.p_name
  val x600 = x596.p_name
  val x601 = x599 == x600
  x601}}.map(x598 => (((x594, x595), x596), x598))
}
val x612 = x602.flatMap{ case (((x603, x604), x605), x606) => 
  val x607 = x605.l_qty
  val x608 = Record634(x607)
  val x609 = x606.l_qty
  val x610 = (1 to x609.asInstanceOf[Int]).map(v => x608)
  x610.map(x611 => {
  val nv633 = ((((x603, x604), x605), x606), x611) 
     nv633
})}
val x620 = { val grps635 = x612.groupBy{ case ((((x613, x614), x615), x616), x617) => { val x618 = (x613,x614,x615)
x618 }}
 grps635.map(x619 => (x619._1, x619._2.foldLeft(0)((acc636, x619) => acc636 + 1))).toList }
val x629 = x620.map{ case ((x621, x622, x623), x624) => { 
  val x625 = x621.c_name
  val x626 = x623.p_name
  val x627 = x622.o_orderdate
  val x628 = Record637(x625, x626, x627, x624)
  x628 }}
x629
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
