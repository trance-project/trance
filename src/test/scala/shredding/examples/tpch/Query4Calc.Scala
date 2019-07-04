
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record998(p_name: String, l_qty: Double)
case class Record999(o_orderdate: String, o_parts: List[Record998])
case class Record1028(c_name: String, p_name: String, month: String, t_qty: Double)
case class Query1Out(c_name: String, c_orders: List[Record999])
object Query4Calc {
 def main(args: Array[String]){
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    val Query4 = { val x993 = C.flatMap(x967 => { 
    val x968 = x967.c_name
    val x990 = O.flatMap(x969 => { 
      if({val x970 = x969.o_custkey
      val x971 = x967.c_custkey
      val x972 = x970 == x971
      x972}) {  val x973 = x969.o_orderdate
        val x987 = L.flatMap(x974 => { 
          if({val x975 = x974.l_orderkey
          val x976 = x969.o_orderkey
          val x977 = x975 == x976
          x977}) {  val x986 = P.flatMap(x978 => { 
              if({val x979 = x974.l_partkey
              val x980 = x978.p_partkey
              val x981 = x979 == x980
              x981}) {  val x982 = x978.p_name
                val x983 = x974.l_quantity
                val x984 = Record998(x982, x983)
                val x985 = List(x984)
                x985} else {  Nil}})
            x986} else {  Nil}})
        val x988 = Record999(x973, x987)
        val x989 = List(x988)
        x989} else {  Nil}})
    val x991 = Query1Out(x968, x990)
    val x992 = List(x991)
    x992})
x993 }
    var end0 = System.currentTimeMillis() - start0
    
    def f(){
      val x1023 = Query4.flatMap(x1004 => { 
    val x1005 = x1004.c_orders
    val x1022 = x1005.flatMap(x1006 => { 
        val x1007 = x1006.o_parts
        val x1021 = x1007.flatMap(x1008 => { 
            val x1009 = x1004.c_name
            val x1010 = x1008.p_name
            val x1011 = x1006.o_orderdate
            val x1012 = x1006.o_parts
            val x1018 = x1012.foldLeft(0.0)((acc1027, x1013) => 
              if({val x1014 = x1013.p_name
              val x1015 = x1008.p_name
              val x1016 = x1014 == x1015
              x1016}) {  acc1027 + {val x1017 = x1013.l_qty
                x1017}} else {  acc1027})
            val x1019 = Record1028(x1009, x1010, x1011, x1018)
            val x1020 = List(x1019)
            x1020})
        x1021})
    x1022})
x1023
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
