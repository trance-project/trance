
package experiments
/** Generated code **/
object Query4 {
 
import shredding.examples.tpch._
val C = TPCHLoader.loadCustomer.toList
val O = TPCHLoader.loadOrders.toList
val L = TPCHLoader.loadLineitem.toList
val P = TPCHLoader.loadPart.toList
 case class Record32(p_name: String, l_qty: Int)
case class Record33(o_orderdate: String, o_parts: List[Record32])
case class Input_Q1(c_name: String, c_orders: List[Record33])
case class Record34(l_qty: Int)
case class Record35(c_name: String, p_name: String, month: String, t_qty: Int)
 def main(args: Array[String]){ 
    var time = List[Long]()
    for (i <- 1 to 5) {
      var start = System.currentTimeMillis()
      f
      var end = System.currentTimeMillis() - start
      time = time :+ end
    }
    val avg = (time.sum/5)
    println(avg)
 }
 def f(){
  val x31 = Q1.flatMap({x7 => 
   val x8 = x7.c_orders
  val x30 = x8.flatMap({x9 => 
     val x10 = x9.o_parts
    val x29 = x10.flatMap({x11 => 
       val x12 = x7.c_name
      val x13 = x11.p_name
      val x14 = x9.o_orderdate
      val x15 = x9.o_parts
      val x26 = x15.withFilter({x16 => val x17 = x16.p_name
      val x18 = x11.p_name
      val x19 = x17 == x18
      x19}).flatMap({x16 => 
         val x20 = x11.l_qty
        val x21 = Record34(x20)
        val x22 = x16.l_qty
        val x23 = (1 to x22).map(v => x21)
        val x25 = x23.map({x24 => 1}).sum
        x25})
      val x27 = Record35(x12, x13, x14, x26)
      val x28 = List(x27)
      x28})
    x29})
  x30})
x31
 }
}
