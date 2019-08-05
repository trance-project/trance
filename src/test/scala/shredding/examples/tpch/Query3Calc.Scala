
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
    case class Record2501(s_name: String, s_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record2505(c_name: String, c_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Query3Out(p_name: String, suppliers: List[Record2501], customers: List[Record2505], uniqueId: Long) extends CaseClassRecord
object Query3Calc {
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
val PS = TPCHLoader.loadPartSupp[PartSupp].toList
val S = TPCHLoader.loadSupplier[Supplier].toList
    var end0 = System.currentTimeMillis() - start0
    def f(){
      val x2497 = P.flatMap(x2460 => { 
    val x2461 = x2460.p_name 
    val x2475 = PS.flatMap(x2462 => { 
      if({val x2463 = x2462.ps_partkey 
      val x2464 = x2460.p_partkey 
      val x2465 = x2463 == x2464 
      x2465   }) {  val x2474 = S.flatMap(x2466 => { 
          if({val x2467 = x2466.s_suppkey 
          val x2468 = x2462.ps_suppkey 
          val x2469 = x2467 == x2468 
          x2469   }) {  val x2470 = x2466.s_name 
            val x2471 = x2466.s_nationkey 
            val x2472 = Record2501(x2470, x2471, newId) 
            val x2473 = List(x2472) 
            x2473} else {  Nil}}) 
        x2474} else {  Nil}}) 
    val x2494 = L.flatMap(x2476 => { 
      if({val x2477 = x2476.l_partkey 
      val x2478 = x2460.p_partkey 
      val x2479 = x2477 == x2478 
      x2479   }) {  val x2493 = O.flatMap(x2480 => { 
          if({val x2481 = x2480.o_orderkey 
          val x2482 = x2476.l_orderkey 
          val x2483 = x2481 == x2482 
          x2483   }) {  val x2492 = C.flatMap(x2484 => { 
              if({val x2485 = x2484.c_custkey 
              val x2486 = x2480.o_custkey 
              val x2487 = x2485 == x2486 
              x2487   }) {  val x2488 = x2484.c_name 
                val x2489 = x2484.c_nationkey 
                val x2490 = Record2505(x2488, x2489, newId) 
                val x2491 = List(x2490) 
                x2491} else {  Nil}}) 
            x2492} else {  Nil}}) 
        x2493} else {  Nil}}) 
    val x2495 = Query3Out(x2461, x2475, x2494, newId) 
    val x2496 = List(x2495) 
    x2496}) 
x2497 
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
