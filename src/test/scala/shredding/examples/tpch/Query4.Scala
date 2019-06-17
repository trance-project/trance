
package experiments
/** Generated code **/
object Query4 {
 
import shredding.examples.tpch._
val C = TPCHLoader.loadCustomer.toList
val O = TPCHLoader.loadOrders.toList
val L = TPCHLoader.loadLineitem.toList
val P = TPCHLoader.loadPart.toList
 case class Record250(p_name: String, l_qty: Double)
case class Record251(o_orderdate: String, o_parts: List[Record250])
case class Record252(c_name: String, c_orders: List[Record251])
case class Record253(l_qty: Double)
case class Record254(c_name: String, p_name: String, month: String, t_qty: Int)
 def main(args: Array[String]){ 
    var time = List[Long]()
    for (i <- 1 to 5) {
      var start = System.currentTimeMillis()
      f
      var end = System.currentTimeMillis() - start
      time = time :+ end
    }
    println(time)
    val avg = (time.sum/5)
    println(avg)
 }
 def f(){
  val x222 = C.flatMap({x195 => 
   val x196 = x195.c_name
  val x219 = O.withFilter({x197 => val x198 = x197.o_custkey
  val x199 = x195.c_custkey
  val x200 = x198 == x199
  x200}).flatMap({x197 => 
     val x201 = x197.o_orderdate
    val x216 = L.flatMap({x202 => 
       val x215 = P.withFilter({x203 => val x204 = x202.l_orderkey
      val x205 = x197.o_orderkey
      val x206 = x204 == x205
      val x207 = x202.l_partkey
      val x208 = x203.p_partkey
      val x209 = x207 == x208
      val x210 = x206 && x209
      x210}).flatMap({x203 => 
         val x211 = x203.p_name
        val x212 = x202.l_quantity
        val x213 = Record250(x211, x212)
        val x214 = List(x213)
        x214})
      x215})
    val x217 = Record251(x201, x216)
    val x218 = List(x217)
    x218})
  val x220 = Record252(x196, x219)
  val x221 = List(x220)
  x221})
val x223 = x222
val x248 = x223.flatMap({x224 => 
   val x225 = x224.c_orders
  val x247 = x225.flatMap({x226 => 
     val x227 = x226.o_parts
    val x246 = x227.flatMap({x228 => 
       val x229 = x224.c_name
      val x230 = x228.p_name
      val x231 = x226.o_orderdate
      val x232 = x226.o_parts
      val x243 = x232.withFilter({x233 => val x234 = x233.p_name
      val x235 = x228.p_name
      val x236 = x234 == x235
      x236}).flatMap({x233 =>
         val x237 = x228.l_qty
        val x238 = Record253(x237)
        val x239 = x233.l_qty
        val x240 = (1 to x239.asInstanceOf[Int]).map(v => x238)
        val x242 = x240.map({x241 => 1})
        x242}).sum
      val x244 = Record254(x229, x230, x231, x243)
      val x245 = List(x244)
      x245})
    x246})
  x247})
val x249 = x223
x248
x249
 }
}
