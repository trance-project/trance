
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record122(p_name: String, l_qty: Double)
case class Record125(o_orderdate: String, o_parts: List[Record122])
case class Record126(c_name: String, c_orders: List[Record125])
object Query1 {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    var end0 = System.currentTimeMillis() - start0
    def f(){
      val x66 = C
val x68 = O
val x74 = { val hm117 = x66.groupBy{ case x69 => {val x72 = x69.c_custkey
x72} }
x68.flatMap(x70 => hm117.get({val x73 = x70.o_custkey
x73}) match {
 case Some(a) => a.map(v => (v, x70))
 case _ => Nil
}) }
val x76 = L
val x83 = { val hm118 = x74.groupBy{ case (x77, x78) => {val x81 = x78.o_orderkey
x81} }
x76.flatMap(x79 => hm118.get({val x82 = x79.l_orderkey
x82}) match {
 case Some(a) => a.map(v => (v, x79))
 case _ => Nil
}) }
val x85 = P
val x93 = { val hm119 = x83.groupBy{ case ((x86, x87), x88) => {val x91 = x88.l_partkey
x91} }
x85.flatMap(x89 => hm119.get({val x92 = x89.p_partkey
x92}) match {
 case Some(a) => a.map(v => (v, x89))
 case _ => Nil
}) }
val x103 = { val grps120 = x93.groupBy{ case (((x94, x95), x96), x97) => { val x98 = (x94,x95)
x98 }}
 grps120.map(x102 => (x102._1, x102._2.map{case (((x94, x95), x96), x97) => val x99 = x97.p_name
val x100 = x96.l_quantity
val x101 = Record122(x99, x100)
x101})).toList }
val x111 = { val grps123 = x103.groupBy{ case ((x104, x105), x106) => { val x107 = (x104)
x107 }}
 grps123.map(x110 => (x110._1, x110._2.map{case ((x104, x105), x106) => val x108 = x105.o_orderdate
val x109 = Record125(x108, x106)
x109})).toList }
val x116 = x111.map{ case (x112, x113) => { 
  val x114 = x112.c_name
  val x115 = Record126(x114, x113)
  x115 }}
x116 
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
