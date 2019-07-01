
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record36(p_name: String, l_qty: Double)
case class Record37(o_orderdate: String, o_parts: List[Record36])
case class Record38(c_name: String, c_orders: List[Record37])
object Query1Calc {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    var end0 = System.currentTimeMillis() - start0
    def f(){
      val x31 = C.flatMap(x5 => { 
    val x6 = x5.c_name
    val x28 = O.flatMap(x7 => { 
      if({val x8 = x7.o_custkey
      val x9 = x5.c_custkey
      val x10 = x8 == x9
      x10}) {  val x11 = x7.o_orderdate
        val x25 = L.flatMap(x12 => { 
          if({val x13 = x12.l_orderkey
          val x14 = x7.o_orderkey
          val x15 = x13 == x14
          x15}) {  val x24 = P.flatMap(x16 => { 
              if({val x17 = x12.l_partkey
              val x18 = x16.p_partkey
              val x19 = x17 == x18
              x19}) {  val x20 = x16.p_name
                val x21 = x12.l_quantity
                val x22 = Record36(x20, x21)
                val x23 = List(x22)
                x23} else {  Nil}})
            x24} else {  Nil}})
        val x26 = Record37(x11, x25)
        val x27 = List(x26)
        x27} else {  Nil}})
    val x29 = Record38(x6, x28)
    val x30 = List(x29)
    x30})
x31 
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
