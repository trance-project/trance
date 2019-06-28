
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record567(p_name: String, l_qty: Double)
case class Record568(o_orderdate: String, o_parts: List[Record567])
case class Q1Out(c_name: String, c_orders: List[Record568])
case class Record685(l_qty: Double)
case class Record688(c_name: String, p_name: String, month: String, t_qty: Int)
object Query4 {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    val Q1 = { val x572 = C
val x574 = O
val x579 = { val hm620 = x572.groupBy{ case x575 => {
  val x577 = x575.c_custkey
  x577}}
x574.flatMap(x576 => hm620.get({val x578 = x576.o_custkey
x578}) match {
 case Some(a) => a.map(v => (v, x576))
 case _ => Nil
}) }
val x581 = L
val x587 = { val hm621 = x579.groupBy{ case (x582, x583) => {
  val x585 = x583.o_orderkey
  x585}}
x581.flatMap(x584 => hm621.get({val x586 = x584.l_orderkey
x586}) match {
 case Some(a) => a.map(v => (v, x584))
 case _ => Nil
}) }
val x589 = P
val x596 = { val hm622 = x587.groupBy{ case ((x590, x591), x592) => {
  val x594 = x592.l_partkey
  x594}}
x589.flatMap(x593 => hm622.get({val x595 = x593.p_partkey
x595}) match {
 case Some(a) => a.map(v => (v, x593))
 case _ => Nil
}) }
val x606 = { val grps623 = x596.groupBy{ case (((x597, x598), x599), x600) => { val x601 = (x597,x598)
x601 }}
 grps623.map(x605 => (x605._1, x605._2.map{case (((x597, x598), x599), x600) => val x602 = x600.p_name
val x603 = x599.l_quantity
val x604 = Record567(x602, x603)
x604})).toList }
val x614 = { val grps625 = x606.groupBy{ case ((x607, x608), x609) => { val x610 = (x607)
x610 }}
 grps625.map(x613 => (x613._1, x613._2.map{case ((x607, x608), x609) => val x611 = x608.o_orderdate
val x612 = Record568(x611, x609)
x612})).toList }
val x619 = x614.map{ case (x615, x616) => { 
  val x617 = x615.c_name
  val x618 = Q1Out(x617, x616)
  x618 }}
x619 }
    var end0 = System.currentTimeMillis() - start0
    
    def f(){
      val x635 = Q1
val x639 = x635.flatMap{ case x636 => 
  val x637 = x636.c_orders
  x637.map(x638 => {
  val nv681 = (x636, x638) 
     nv681
})}
val x644 = x639.flatMap{ case (x640, x641) => 
  val x642 = x641.o_parts
  x642.map(x643 => {
  val nv682 = ((x640, x641), x643) 
     nv682
})}
val x653 = x644.flatMap{ case ((x645, x646), x647) =>
  val x648 = x646.o_parts
  x648.withFilter{ case x649 =>
  {val x650 = x649.p_name
  val x651 = x647.p_name
  val x652 = x650 == x651
  x652}}.map(x649 => (((x645, x646), x647), x649))
}
val x663 = x653.flatMap{ case (((x654, x655), x656), x657) => 
  val x658 = x656.l_qty
  val x659 = Record685(x658)
  val x660 = x657.l_qty
  val x661 = (1 to x660.asInstanceOf[Int]).map(v => x659)
  x661.map(x662 => {
  val nv684 = ((((x654, x655), x656), x657), x662) 
     nv684
})}
val x671 = { val grps686 = x663.groupBy{ case ((((x664, x665), x666), x667), x668) => { val x669 = (x664,x665,x666)
x669 }}
 grps686.map(x670 => (x670._1, x670._2.foldLeft(0)((acc687, x670) => acc687 + 1))).toList }
val x680 = x671.map{ case ((x672, x673, x674), x675) => { 
  val x676 = x672.c_name
  val x677 = x674.p_name
  val x678 = x673.o_orderdate
  val x679 = Record688(x676, x677, x678, x675)
  x679 }}
x680
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
