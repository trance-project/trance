
package experiments
/** Generated code **/
object Query1 {
 
import shredding.examples.tpch._
val C = TPCHLoader.loadCustomer.toList
val O = TPCHLoader.loadOrders.toList
val L = TPCHLoader.loadLineitem.toList
val P = TPCHLoader.loadPart.toList
 case class Record37(p_name: String, l_qty: Double)
case class Record38(o_orderdate: String, o_parts: List[Record37])
case class Record39(c_name: String, c_orders: List[Record38])
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
  val x32 = C.flatMap(acc33 =>  
  {  val x6 = x5.c_name
    val x29 = O.flatMap(acc34 =>  
      if({val x8 = x7.o_custkey
      val x9 = x5.c_custkey
      val x10 = x8 == x9
      x10}) {  val x11 = x7.o_orderdate
        val x26 = L.flatMap(acc35 =>  
          {  val x25 = P.flatMap(acc36 =>  
              if({val x14 = x12.l_orderkey
              val x15 = x7.o_orderkey
              val x16 = x14 == x15
              val x17 = x12.l_partkey
              val x18 = x13.p_partkey
              val x19 = x17 == x18
              val x20 = x16 && x19
              x20}) {  val x21 = x13.p_name
                val x22 = x12.l_quantity
                val x23 = Record37(x21, x22)
                val x24 = List(x23)
                x24} else {  Nil})
            x25})
        val x27 = Record38(x11, x26)
        val x28 = List(x27)
        x28} else {  Nil})
    val x30 = Record39(x6, x29)
    val x31 = List(x30)
    x31})
x32
 }
}
