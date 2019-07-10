
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record994(p_name: String, l_qty: Double)
case class Record995(o_orderdate: String, o_parts: List[Record994])
case class Query1Out(c_name: String, c_orders: List[Record995])
object Query1Calc {
 def main(args: Array[String]){
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    var end0 = System.currentTimeMillis() - start0
    def f(){
      val x989 = C.flatMap(x963 => { 
    val x964 = x963.c_name 
    val x986 = O.flatMap(x965 => { 
      if({val x966 = x965.o_custkey 
      val x967 = x963.c_custkey 
      val x968 = x966 == x967 
      x968   }) {  val x969 = x965.o_orderdate 
        val x983 = L.flatMap(x970 => { 
          if({val x971 = x970.l_orderkey 
          val x972 = x965.o_orderkey 
          val x973 = x971 == x972 
          x973   }) {  val x982 = P.flatMap(x974 => { 
              if({val x975 = x970.l_partkey 
              val x976 = x974.p_partkey 
              val x977 = x975 == x976 
              x977   }) {  val x978 = x974.p_name 
                val x979 = x970.l_quantity 
                val x980 = Record994(x978, x979) 
                val x981 = List(x980) 
                x981} else {  Nil}}) 
            x982} else {  Nil}}) 
        val x984 = Record995(x969, x983) 
        val x985 = List(x984) 
        x985} else {  Nil}}) 
    val x987 = Query1Out(x964, x986) 
    val x988 = List(x987) 
    x988}) 
x989 
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
