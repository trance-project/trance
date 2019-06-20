
package experiments
/** Generated code **/
object Query1 {
 
import shredding.examples.tpch._
val C = TPCHLoader.loadCustomer.toList
val O = TPCHLoader.loadOrders.toList
val L = TPCHLoader.loadLineitem.toList
val P = TPCHLoader.loadPart.toList
 case class Record38(p_name: String, l_qty: Double)
case class Record39(o_orderdate: String, o_parts: List[Record38])
case class Record40(c_name: String, c_orders: List[Record39])
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
  val x33 = C.flatMap(x7 =>  
  {  val x8 = x7.c_name
    val x30 = O.flatMap(x9 =>  
      if({val x10 = x9.o_custkey
      val x11 = x7.c_custkey
      val x12 = x10 == x11
      x12}) {  val x13 = x9.o_orderdate
        val x27 = L.flatMap(x14 =>  
          if({val x15 = x14.l_orderkey
          val x16 = x9.o_orderkey
          val x17 = x15 == x16
          x17}) {  val x26 = P.flatMap(x18 =>  
              if({val x19 = x14.l_partkey
              val x20 = x18.p_partkey
              val x21 = x19 == x20
              x21}) {  val x22 = x18.p_name
                val x23 = x14.l_quantity
                val x24 = Record38(x22, x23)
                val x25 = List(x24)
                x25} else {  Nil})
            x26} else {  Nil})
        val x28 = Record39(x13, x27)
        val x29 = List(x28)
        x29} else {  Nil})
    val x31 = Record40(x8, x30)
    val x32 = List(x31)
    x32})
x33
 }
}
