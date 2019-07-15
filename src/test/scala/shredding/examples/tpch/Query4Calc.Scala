
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
case class Record1004(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record1005(o_orderdate: String, o_parts: List[Record1004], uniqueId: Long) extends CaseClassRecord
case class Record1034(c_name: String, p_name: String, month: String, t_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Query1Out(c_name: String, c_orders: List[Record1005], uniqueId: Long) extends CaseClassRecord
object Query4Calc {
 def main(args: Array[String]){
    var start0 = System.currentTimeMillis()
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    val Query4 = { val x999 = C.flatMap(x973 => { 
    val x974 = x973.c_name 
    val x996 = O.flatMap(x975 => { 
      if({val x976 = x975.o_custkey 
      val x977 = x973.c_custkey 
      val x978 = x976 == x977 
      x978   }) {  val x979 = x975.o_orderdate 
        val x993 = L.flatMap(x980 => { 
          if({val x981 = x980.l_orderkey 
          val x982 = x975.o_orderkey 
          val x983 = x981 == x982 
          x983   }) {  val x992 = P.flatMap(x984 => { 
              if({val x985 = x980.l_partkey 
              val x986 = x984.p_partkey 
              val x987 = x985 == x986 
              x987   }) {  val x988 = x984.p_name 
                val x989 = x980.l_quantity 
                val x990 = Record1004(x988, x989, newId) 
                val x991 = List(x990) 
                x991} else {  Nil}}) 
            x992} else {  Nil}}) 
        val x994 = Record1005(x979, x993, newId) 
        val x995 = List(x994) 
        x995} else {  Nil}}) 
    val x997 = Query1Out(x974, x996, newId) 
    val x998 = List(x997) 
    x998}) 
x999  }
    var end0 = System.currentTimeMillis() - start0
    
    def f(){
      val x1029 = Query4.flatMap(x1010 => { 
    val x1011 = x1010.c_orders 
    val x1028 = x1011.flatMap(x1012 => { 
        val x1013 = x1012.o_parts 
        val x1027 = x1013.flatMap(x1014 => { 
            val x1015 = x1010.c_name 
            val x1016 = x1014.p_name 
            val x1017 = x1012.o_orderdate 
            val x1018 = x1012.o_parts 
            val x1024 = x1018.foldLeft(0.0)((acc1033, x1019) => 
              if({val x1020 = x1019.p_name 
              val x1021 = x1014.p_name 
              val x1022 = x1020 == x1021 
              x1022   }) {  acc1033 + {val x1023 = x1019.l_qty 
                x1023 }} else {  acc1033}) 
            val x1025 = Record1034(x1015, x1016, x1017, x1024, newId) 
            val x1026 = List(x1025) 
            x1026}) 
        x1027}) 
    x1028}) 
x1029 
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
