
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record523(p_name: String, l_qty: Double)
case class Record524(o_orderdate: String, o_parts: List[Record523])
case class Record560(l_qty: Double)
case class Record562(c_name: String, p_name: String, month: String, t_qty: Int)
case class Q1Out(c_name: String, c_orders: List[Record524])
object Query4Calc {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    val Q1 = { val x518 = C.flatMap(x492 => { 
    val x493 = x492.c_name
    val x515 = O.flatMap(x494 => { 
      if({val x495 = x494.o_custkey
      val x496 = x492.c_custkey
      val x497 = x495 == x496
      x497}) {  val x498 = x494.o_orderdate
        val x512 = L.flatMap(x499 => { 
          if({val x500 = x499.l_orderkey
          val x501 = x494.o_orderkey
          val x502 = x500 == x501
          x502}) {  val x511 = P.flatMap(x503 => { 
              if({val x504 = x499.l_partkey
              val x505 = x503.p_partkey
              val x506 = x504 == x505
              x506}) {  val x507 = x503.p_name
                val x508 = x499.l_quantity
                val x509 = Record523(x507, x508)
                val x510 = List(x509)
                x510} else {  Nil}})
            x511} else {  Nil}})
        val x513 = Record524(x498, x512)
        val x514 = List(x513)
        x514} else {  Nil}})
    val x516 = Q1Out(x493, x515)
    val x517 = List(x516)
    x517})
x518 }
    var end0 = System.currentTimeMillis() - start0
    
    def f(){
      val x555 = Q1.flatMap(x531 => { 
    val x532 = x531.c_orders
    val x554 = x532.flatMap(x533 => { 
        val x534 = x533.o_parts
        val x553 = x534.flatMap(x535 => { 
            val x536 = x531.c_name
            val x537 = x535.p_name
            val x538 = x533.o_orderdate
            val x539 = x533.o_parts
            val x550 = x539.foldLeft(0)((acc559, x540) => 
              if({val x541 = x540.p_name
              val x542 = x535.p_name
              val x543 = x541 == x542
              x543}) {  acc559 + {val x544 = x535.l_qty
                val x545 = Record560(x544)
                val x546 = x540.l_qty
                val x547 = (1 to x546.asInstanceOf[Int]).map(v => x545)
                val x549 = x547.foldLeft(0)((acc561, x548) => 
                    acc561 + {1})
                x549}} else {  acc559})
            val x551 = Record562(x536, x537, x538, x550)
            val x552 = List(x551)
            x552})
        x553})
    x554})
x555
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
