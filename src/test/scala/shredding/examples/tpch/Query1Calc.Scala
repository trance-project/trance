
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record59(p_name: String, l_qty: Double)
case class Record60(o_orderdate: String, o_parts: List[Record59])
case class Record61(c_name: String, c_orders: List[Record60])
object Query1Calc {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    def f(){
      val x54 = C.flatMap(x28 => { 
    val x29 = x28.c_name
    val x51 = O.flatMap(x30 => { 
      if({val x31 = x30.o_custkey
      val x32 = x28.c_custkey
      val x33 = x31 == x32
      x33}) {  val x34 = x30.o_orderdate
        val x48 = L.flatMap(x35 => { 
          if({val x36 = x35.l_orderkey
          val x37 = x30.o_orderkey
          val x38 = x36 == x37
          x38}) {  val x47 = P.flatMap(x39 => { 
              if({val x40 = x35.l_partkey
              val x41 = x39.p_partkey
              val x42 = x40 == x41
              x42}) {  val x43 = x39.p_name
                val x44 = x35.l_quantity
                val x45 = Record59(x43, x44)
                val x46 = List(x45)
                x46} else {  Nil}})
            x47} else {  Nil}})
        val x49 = Record60(x34, x48)
        val x50 = List(x49)
        x50} else {  Nil}})
    val x52 = Record61(x29, x51)
    val x53 = List(x52)
    x53})
x54 
    }
    var end0 = System.currentTimeMillis() - start0
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
