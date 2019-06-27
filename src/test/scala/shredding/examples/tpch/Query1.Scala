
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record118(p_name: String, l_qty: Double)
case class Record121(o_orderdate: String, o_parts: List[Record118])
case class Record122(c_name: String, c_orders: List[Record121])
object Query1 {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    def f(){
      val x65 = C
val x67 = O
val x72 = { val hm113 = x65.groupBy{ case x68 => {
  val x70 = x68.c_custkey
  x70}}
x67.flatMap(x69 => hm113.get({val x71 = x69.o_custkey
x71}) match {
 case Some(a) => a.map(v => (v, x69))
 case _ => Nil
}) }
val x74 = L
val x80 = { val hm114 = x72.groupBy{ case (x75, x76) => {
  val x78 = x76.o_orderkey
  x78}}
x74.flatMap(x77 => hm114.get({val x79 = x77.l_orderkey
x79}) match {
 case Some(a) => a.map(v => (v, x77))
 case _ => Nil
}) }
val x82 = P
val x89 = { val hm115 = x80.groupBy{ case ((x83, x84), x85) => {
  val x87 = x85.l_partkey
  x87}}
x82.flatMap(x86 => hm115.get({val x88 = x86.p_partkey
x88}) match {
 case Some(a) => a.map(v => (v, x86))
 case _ => Nil
}) }
val x99 = { val grps116 = x89.groupBy{ case (((x90, x91), x92), x93) => { val x94 = (x90,x91)
x94 }}
 grps116.map(x98 => (x98._1, x98._2.map{case (((x90, x91), x92), x93) => val x95 = x93.p_name
val x96 = x92.l_quantity
val x97 = Record118(x95, x96)
x97})).toList }
val x107 = { val grps119 = x99.groupBy{ case ((x100, x101), x102) => { val x103 = (x100)
x103 }}
 grps119.map(x106 => (x106._1, x106._2.map{case ((x100, x101), x102) => val x104 = x101.o_orderdate
val x105 = Record121(x104, x102)
x105})).toList }
val x112 = x107.map{ case (x108, x109) => { 
  val x110 = x108.c_name
  val x111 = Record122(x110, x109)
  x111 }}
x112 
    }
    var end0 = System.currentTimeMillis() - start0
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
