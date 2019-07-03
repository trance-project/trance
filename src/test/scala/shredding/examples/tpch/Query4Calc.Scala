
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record464(p_name: String, l_qty: Double)
case class Record465(o_orderdate: String, o_parts: List[Record464])
case class Record495(c_name: String, p_name: String, month: String, t_qty: Double)
case class Q1Out(c_name: String, c_orders: List[Record465])
object Query4Calc {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    val Q1 = { val x459 = C.flatMap(x433 => { 
    val x434 = x433.c_name
    val x456 = O.flatMap(x435 => { 
      if({val x436 = x435.o_custkey
      val x437 = x433.c_custkey
      val x438 = x436 == x437
      x438}) {  val x439 = x435.o_orderdate
        val x453 = L.flatMap(x440 => { 
          if({val x441 = x440.l_orderkey
          val x442 = x435.o_orderkey
          val x443 = x441 == x442
          x443}) {  val x452 = P.flatMap(x444 => { 
              if({val x445 = x440.l_partkey
              val x446 = x444.p_partkey
              val x447 = x445 == x446
              x447}) {  val x448 = x444.p_name
                val x449 = x440.l_quantity
                val x450 = Record464(x448, x449)
                val x451 = List(x450)
                x451} else {  Nil}})
            x452} else {  Nil}})
        val x454 = Record465(x439, x453)
        val x455 = List(x454)
        x455} else {  Nil}})
    val x457 = Q1Out(x434, x456)
    val x458 = List(x457)
    x458})
x459 }
    var end0 = System.currentTimeMillis() - start0
    
    def f(){
      val x490 = Q1.flatMap(x471 => { 
    val x472 = x471.c_orders
    val x489 = x472.flatMap(x473 => { 
        val x474 = x473.o_parts
        val x488 = x474.flatMap(x475 => { 
            val x476 = x471.c_name
            val x477 = x475.p_name
            val x478 = x473.o_orderdate
            val x479 = x473.o_parts
            val x485 = x479.foldLeft(0.0)((acc494, x480) => 
              if({val x481 = x480.p_name
              val x482 = x475.p_name
              val x483 = x481 == x482
              x483}) {  acc494 + {val x484 = x480.l_qty
                x484}} else {  acc494})
            val x486 = Record495(x476, x477, x478, x485)
            val x487 = List(x486)
            x487})
        x488})
    x489})
x490
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
