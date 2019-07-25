
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
    case class Record546(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record547(o_orderdate: String, o_parts: List[Record546], uniqueId: Long) extends CaseClassRecord
case class Query1Out(c_name: String, c_orders: List[Record547], uniqueId: Long) extends CaseClassRecord
object Query1Calc {
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
    var end0 = System.currentTimeMillis() - start0
    def f(){
      val x541 = C.flatMap(x515 => { 
    val x516 = x515.c_name 
    val x538 = O.flatMap(x517 => { 
      if({val x518 = x517.o_custkey 
      val x519 = x515.c_custkey 
      val x520 = x518 == x519 
      x520   }) {  val x521 = x517.o_orderdate 
        val x535 = L.flatMap(x522 => { 
          if({val x523 = x522.l_orderkey 
          val x524 = x517.o_orderkey 
          val x525 = x523 == x524 
          x525   }) {  val x534 = P.flatMap(x526 => { 
              if({val x527 = x522.l_partkey 
              val x528 = x526.p_partkey 
              val x529 = x527 == x528 
              x529   }) {  val x530 = x526.p_name 
                val x531 = x522.l_quantity 
                val x532 = Record546(x530, x531, newId) 
                val x533 = List(x532) 
                x533} else {  Nil}}) 
            x534} else {  Nil}}) 
        val x536 = Record547(x521, x535, newId) 
        val x537 = List(x536) 
        x537} else {  Nil}}) 
    val x539 = Query1Out(x516, x538, newId) 
    val x540 = List(x539) 
    x540}) 
x541 
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
