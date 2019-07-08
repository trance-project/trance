
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record994(p_name: String, l_qty: Double)
case class Record995(o_orderdate: String, o_parts: List[Record994])
case class Record1024(c_name: String, p_name: String, month: String, t_qty: Double)
case class Query1Out(c_name: String, c_orders: List[Record995])
object Query4Calc {
 def main(args: Array[String]){
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    val Query4 = { val x989 = C.flatMap(x963 => { 
    val x964 = x963.c_name
    val x986 = O.flatMap(x965 => { 
      if({val x966 = x965.o_custkey
      val x967 = x963.c_custkey
      val x968 = x966 == x967
      x968}) {  val x969 = x965.o_orderdate
        val x983 = L.flatMap(x970 => { 
          if({val x971 = x970.l_orderkey
          val x972 = x965.o_orderkey
          val x973 = x971 == x972
          x973}) {  val x982 = P.flatMap(x974 => { 
              if({val x975 = x970.l_partkey
              val x976 = x974.p_partkey
              val x977 = x975 == x976
              x977}) {  val x978 = x974.p_name
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
x989 }
    var end0 = System.currentTimeMillis() - start0
    
    def f(){
      val x1019 = Query4.flatMap(x1000 => { 
    val x1001 = x1000.c_orders
    val x1018 = x1001.flatMap(x1002 => { 
        val x1003 = x1002.o_parts
        val x1017 = x1003.flatMap(x1004 => { 
            val x1005 = x1000.c_name
            val x1006 = x1004.p_name
            val x1007 = x1002.o_orderdate
            val x1008 = x1002.o_parts
            val x1014 = x1008.foldLeft(0.0)((acc1023, x1009) => 
              if({val x1010 = x1009.p_name
              val x1011 = x1004.p_name
              val x1012 = x1010 == x1011
              x1012}) {  acc1023 + {val x1013 = x1009.l_qty
                x1013}} else {  acc1023})
            val x1015 = Record1024(x1005, x1006, x1007, x1014)
            val x1016 = List(x1015)
            x1016})
        x1017})
    x1018})
x1019
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
