
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record500(p_name: String, l_qty: Double)
case class Record501(o_orderdate: String, o_parts: List[Record500])
case class Q1Out(c_name: String, c_orders: List[Record501])
case class Record608(c_name: String, p_name: String, month: String, t_qty: Double)
object Query4 {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    val Q1 = { val x505 = C
val x507 = O
val x512 = { val hm553 = x505.groupBy{ case x508 => {
  val x510 = x508.c_custkey
  x510}}
x507.flatMap(x509 => hm553.get({val x511 = x509.o_custkey
x511}) match {
 case Some(a) => a.map(v => (v, x509))
 case _ => Nil
}) }
val x514 = L
val x520 = { val hm554 = x512.groupBy{ case (x515, x516) => {
  val x518 = x516.o_orderkey
  x518}}
x514.flatMap(x517 => hm554.get({val x519 = x517.l_orderkey
x519}) match {
 case Some(a) => a.map(v => (v, x517))
 case _ => Nil
}) }
val x522 = P
val x529 = { val hm555 = x520.groupBy{ case ((x523, x524), x525) => {
  val x527 = x525.l_partkey
  x527}}
x522.flatMap(x526 => hm555.get({val x528 = x526.p_partkey
x528}) match {
 case Some(a) => a.map(v => (v, x526))
 case _ => Nil
}) }
val x539 = { val grps556 = x529.groupBy{ case (((x530, x531), x532), x533) => { val x534 = (x530,x531)
x534 }}
 grps556.map(x538 => (x538._1, x538._2.map{case (((x530, x531), x532), x533) => val x535 = x533.p_name
val x536 = x532.l_quantity
val x537 = Record500(x535, x536)
x537})).toList }
val x547 = { val grps558 = x539.groupBy{ case ((x540, x541), x542) => { val x543 = (x540)
x543 }}
 grps558.map(x546 => (x546._1, x546._2.map{case ((x540, x541), x542) => val x544 = x541.o_orderdate
val x545 = Record501(x544, x542)
x545})).toList }
val x552 = x547.map{ case (x548, x549) => { 
  val x550 = x548.c_name
  val x551 = Q1Out(x550, x549)
  x551 }}
x552 }
    var end0 = System.currentTimeMillis() - start0
    
    def f(){
      val x567 = Q1
val x571 = x567.flatMap{ case x568 => 
  val x569 = x568.c_orders
  x569.map(x570 => {
  val nv603 = (x568, x570) 
     nv603
})}
val x576 = x571.flatMap{ case (x572, x573) => 
  val x574 = x573.o_parts
  x574.map(x575 => {
  val nv604 = ((x572, x573), x575) 
     nv604
})}
val x585 = x576.flatMap{ case ((x577, x578), x579) =>
  val x580 = x578.o_parts
  x580.withFilter{ case x581 =>
  {val x582 = x581.p_name
  val x583 = x579.p_name
  val x584 = x582 == x583
  x584}}.map(x581 => (((x577, x578), x579), x581))
}
val x593 = { val grps606 = x585.groupBy{ case (((x586, x587), x588), x589) => { val x590 = (x586,x587,x588)
x590 }}
 grps606.map(x592 => (x592._1, x592._2.foldLeft(0.0){ case (acc607, (((x586, x587), x588), x589)) => acc607 + {val x591 = x589.l_qty
x591} })).toList }
val x602 = x593.map{ case ((x594, x595, x596), x597) => { 
  val x598 = x594.c_name
  val x599 = x596.p_name
  val x600 = x595.o_orderdate
  val x601 = Record608(x598, x599, x600, x597)
  x601 }}
x602
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
