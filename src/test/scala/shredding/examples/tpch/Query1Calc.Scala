
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record60(p_name: String, l_qty: Double)
case class Record61(o_orderdate: String, o_parts: List[Record60])
case class Record62(c_name: String, c_orders: List[Record61])
object Query1Calc {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    var end0 = System.currentTimeMillis() - start0
    def f(){
      val x55 = C.flatMap(x29 => { 
    val x30 = x29.c_name
    val x52 = O.flatMap(x31 => { 
      if({val x32 = x31.o_custkey
      val x33 = x29.c_custkey
      val x34 = x32 == x33
      x34}) {  val x35 = x31.o_orderdate
        val x49 = L.flatMap(x36 => { 
          if({val x37 = x36.l_orderkey
          val x38 = x31.o_orderkey
          val x39 = x37 == x38
          x39}) {  val x48 = P.flatMap(x40 => { 
              if({val x41 = x36.l_partkey
              val x42 = x40.p_partkey
              val x43 = x41 == x42
              x43}) {  val x44 = x40.p_name
                val x45 = x36.l_quantity
                val x46 = Record60(x44, x45)
                val x47 = List(x46)
                x47} else {  Nil}})
            x48} else {  Nil}})
        val x50 = Record61(x35, x49)
        val x51 = List(x50)
        x51} else {  Nil}})
    val x53 = Record62(x30, x52)
    val x54 = List(x53)
    x54})
x55 
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
