
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record470(p_name: String, l_qty: Double)
case class Record471(o_orderdate: String, o_parts: List[Record470])
case class Record507(l_qty: Double)
case class Record509(c_name: String, p_name: String, month: String, t_qty: Int)
case class Q1Out(c_name: String, c_orders: List[Record471])
object Query4Calc {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    val Q1 = { val x465 = C.flatMap(x439 => { 
    val x440 = x439.c_name
    val x462 = O.flatMap(x441 => { 
      if({val x442 = x441.o_custkey
      val x443 = x439.c_custkey
      val x444 = x442 == x443
      x444}) {  val x445 = x441.o_orderdate
        val x459 = L.flatMap(x446 => { 
          if({val x447 = x446.l_orderkey
          val x448 = x441.o_orderkey
          val x449 = x447 == x448
          x449}) {  val x458 = P.flatMap(x450 => { 
              if({val x451 = x446.l_partkey
              val x452 = x450.p_partkey
              val x453 = x451 == x452
              x453}) {  val x454 = x450.p_name
                val x455 = x446.l_quantity
                val x456 = Record470(x454, x455)
                val x457 = List(x456)
                x457} else {  Nil}})
            x458} else {  Nil}})
        val x460 = Record471(x445, x459)
        val x461 = List(x460)
        x461} else {  Nil}})
    val x463 = Q1Out(x440, x462)
    val x464 = List(x463)
    x464})
x465 }
    var end0 = System.currentTimeMillis() - start0
    
    def f(){
      val x502 = Q1.flatMap(x478 => { 
    val x479 = x478.c_orders
    val x501 = x479.flatMap(x480 => { 
        val x481 = x480.o_parts
        val x500 = x481.flatMap(x482 => { 
            val x483 = x478.c_name
            val x484 = x482.p_name
            val x485 = x480.o_orderdate
            val x486 = x480.o_parts
            val x497 = x486.foldLeft(0)((acc506, x487) => 
              if({val x488 = x487.p_name
              val x489 = x482.p_name
              val x490 = x488 == x489
              x490}) {  acc506 + {val x491 = x482.l_qty
                val x492 = Record507(x491)
                val x493 = x487.l_qty
                val x494 = (1 to x493.asInstanceOf[Int]).map(v => x492)
                val x496 = x494.foldLeft(0)((acc508, x495) => 
                    acc508 + {1})
                x496}} else {  acc506})
            val x498 = Record509(x483, x484, x485, x497)
            val x499 = List(x498)
            x499})
        x500})
    x501})
x502
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
