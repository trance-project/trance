
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record472(p_name: String, l_qty: Double)
case class Record473(o_orderdate: String, o_parts: List[Record472])
case class Record509(l_qty: Double)
case class Record511(c_name: String, p_name: String, month: String, t_qty: Int)
case class Q1Out(c_name: String, c_orders: List[Record473])
object Query4Calc {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    val Q1 = { val x467 = C.flatMap(x441 => { 
    val x442 = x441.c_name
    val x464 = O.flatMap(x443 => { 
      if({val x444 = x443.o_custkey
      val x445 = x441.c_custkey
      val x446 = x444 == x445
      x446}) {  val x447 = x443.o_orderdate
        val x461 = L.flatMap(x448 => { 
          if({val x449 = x448.l_orderkey
          val x450 = x443.o_orderkey
          val x451 = x449 == x450
          x451}) {  val x460 = P.flatMap(x452 => { 
              if({val x453 = x448.l_partkey
              val x454 = x452.p_partkey
              val x455 = x453 == x454
              x455}) {  val x456 = x452.p_name
                val x457 = x448.l_quantity
                val x458 = Record472(x456, x457)
                val x459 = List(x458)
                x459} else {  Nil}})
            x460} else {  Nil}})
        val x462 = Record473(x447, x461)
        val x463 = List(x462)
        x463} else {  Nil}})
    val x465 = Q1Out(x442, x464)
    val x466 = List(x465)
    x466})
x467 }
    var end0 = System.currentTimeMillis() - start0
    
    def f(){
      val x504 = Q1.flatMap(x480 => { 
    val x481 = x480.c_orders
    val x503 = x481.flatMap(x482 => { 
        val x483 = x482.o_parts
        val x502 = x483.flatMap(x484 => { 
            val x485 = x480.c_name
            val x486 = x484.p_name
            val x487 = x482.o_orderdate
            val x488 = x482.o_parts
            val x499 = x488.foldLeft(0)((acc508, x489) => 
              if({val x490 = x489.p_name
              val x491 = x484.p_name
              val x492 = x490 == x491
              x492}) {  acc508 + {val x493 = x484.l_qty
                val x494 = Record509(x493)
                val x495 = x489.l_qty
                val x496 = (1 to x495.asInstanceOf[Int]).map(v => x494)
                val x498 = x496.foldLeft(0)((acc510, x497) => 
                    acc510 + {1})
                x498}} else {  acc508})
            val x500 = Record511(x485, x486, x487, x499)
            val x501 = List(x500)
            x501})
        x502})
    x503})
x504
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
