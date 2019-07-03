
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record485(p_name: String, l_qty: Double)
case class Record486(o_orderdate: String, o_parts: List[Record485])
case class Record516(c_name: String, p_name: String, month: String, t_qty: Double)
case class Q1Out(c_name: String, c_orders: List[Record486])
object Query4Calc {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    val Q1 = { val x480 = C.flatMap(x454 => { 
    val x455 = x454.c_name
    val x477 = O.flatMap(x456 => { 
      if({val x457 = x456.o_custkey
      val x458 = x454.c_custkey
      val x459 = x457 == x458
      x459}) {  val x460 = x456.o_orderdate
        val x474 = L.flatMap(x461 => { 
          if({val x462 = x461.l_orderkey
          val x463 = x456.o_orderkey
          val x464 = x462 == x463
          x464}) {  val x473 = P.flatMap(x465 => { 
              if({val x466 = x461.l_partkey
              val x467 = x465.p_partkey
              val x468 = x466 == x467
              x468}) {  val x469 = x465.p_name
                val x470 = x461.l_quantity
                val x471 = Record485(x469, x470)
                val x472 = List(x471)
                x472} else {  Nil}})
            x473} else {  Nil}})
        val x475 = Record486(x460, x474)
        val x476 = List(x475)
        x476} else {  Nil}})
    val x478 = Q1Out(x455, x477)
    val x479 = List(x478)
    x479})
x480 }
    var end0 = System.currentTimeMillis() - start0
    
    def f(){
      val x511 = Q1.flatMap(x492 => { 
    val x493 = x492.c_orders
    val x510 = x493.flatMap(x494 => { 
        val x495 = x494.o_parts
        val x509 = x495.flatMap(x496 => { 
            val x497 = x492.c_name
            val x498 = x496.p_name
            val x499 = x494.o_orderdate
            val x500 = x494.o_parts
            val x506 = x500.foldLeft(0.0)((acc515, x501) => 
              if({val x502 = x501.p_name
              val x503 = x496.p_name
              val x504 = x502 == x503
              x504}) {  acc515 + {val x505 = x501.l_qty
                x505}} else {  acc515})
            val x507 = Record516(x497, x498, x499, x506)
            val x508 = List(x507)
            x508})
        x509})
    x510})
x511
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
