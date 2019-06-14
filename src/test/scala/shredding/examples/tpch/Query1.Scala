
package experiments
/** Generated code **/
object Query1 {
 
import shredding.examples.tpch._
val C = TPCHLoader.loadCustomer.toList
val O = TPCHLoader.loadOrders.toList
val L = TPCHLoader.loadLineitem.toList
val P = TPCHLoader.loadPart.toList
 case class Record33(p_name: String, l_qty: Double)
case class Record34(o_orderdate: String, o_parts: List[Record33])
case class Record35(c_name: String, c_orders: List[Record34])
 def main(args: Array[String]){ 
    var time = List[Long]()
    for (i <- 1 to 5) {
      var start = System.currentTimeMillis()
      println(f)
      var end = System.currentTimeMillis() - start
      time = time :+ end
    }
    val avg = (time.sum/5)
    println(avg)
 }
 
 def f(){
   val x32 = C.flatMap({x5 => 
   val x6 = x5.c_name
  val x29 = O.withFilter({x7 => val x8 = x7.o_custkey
  val x9 = x5.c_custkey
  val x10 = x8 == x9
  x10}).flatMap({x7 => 
     val x11 = x7.o_orderdate
    val x26 = L.flatMap({x12 => 
       val x25 = P.withFilter({x13 => val x14 = x12.l_orderkey
      val x15 = x7.o_orderkey
      val x16 = x14 == x15
      val x17 = x12.l_partkey
      val x18 = x13.p_partkey
      val x19 = x17 == x18
      val x20 = x16 && x19
      x20}).flatMap({x13 => 
         val x21 = x13.p_name
        val x22 = x12.l_quantity
        val x23 = Record33(x21, x22)
        val x24 = List(x23)
        x24})
      x25})
    val x27 = Record34(x11, x26)
    val x28 = List(x27)
    x28})
  val x30 = Record35(x6, x29)
  val x31 = List(x30)
  x31})
x32
 }
}
