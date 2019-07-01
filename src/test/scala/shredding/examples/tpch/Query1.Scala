
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record95(p_name: String, l_qty: Double)
case class Record98(o_orderdate: String, o_parts: List[Record95])
case class Record99(c_name: String, c_orders: List[Record98])
object Query1 {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    var end0 = System.currentTimeMillis() - start0
    def f(){
      val x42 = C
val x44 = O
val x49 = { val hm90 = x42.groupBy{ case x45 => {
  val x47 = x45.c_custkey
  x47}}
x44.flatMap(x46 => hm90.get({val x48 = x46.o_custkey
x48}) match {
 case Some(a) => a.map(v => (v, x46))
 case _ => Nil
}) }
val x51 = L
val x57 = { val hm91 = x49.groupBy{ case (x52, x53) => {
  val x55 = x53.o_orderkey
  x55}}
x51.flatMap(x54 => hm91.get({val x56 = x54.l_orderkey
x56}) match {
 case Some(a) => a.map(v => (v, x54))
 case _ => Nil
}) }
val x59 = P
val x66 = { val hm92 = x57.groupBy{ case ((x60, x61), x62) => {
  val x64 = x62.l_partkey
  x64}}
x59.flatMap(x63 => hm92.get({val x65 = x63.p_partkey
x65}) match {
 case Some(a) => a.map(v => (v, x63))
 case _ => Nil
}) }
val x76 = { val grps93 = x66.groupBy{ case (((x67, x68), x69), x70) => { val x71 = (x67,x68)
x71 }}
 grps93.map(x75 => (x75._1, x75._2.map{case (((x67, x68), x69), x70) => val x72 = x70.p_name
val x73 = x69.l_quantity
val x74 = Record95(x72, x73)
x74})).toList }
val x84 = { val grps96 = x76.groupBy{ case ((x77, x78), x79) => { val x80 = (x77)
x80 }}
 grps96.map(x83 => (x83._1, x83._2.map{case ((x77, x78), x79) => val x81 = x78.o_orderdate
val x82 = Record98(x81, x79)
x82})).toList }
val x89 = x84.map{ case (x85, x86) => { 
  val x87 = x85.c_name
  val x88 = Record99(x87, x86)
  x88 }}
x89 
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
