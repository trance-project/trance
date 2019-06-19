
package experiments
/** Generated code **/
object Query4 {
 
import shredding.examples.tpch._
val C = TPCHLoader.loadCustomer.toList
val O = TPCHLoader.loadOrders.toList
val L = TPCHLoader.loadLineitem.toList
val P = TPCHLoader.loadPart.toList
 case class Record216(p_name: String, l_qty: Double)
case class Record217(o_orderdate: String, o_parts: List[Record216])
case class Record218(c_name: String, c_orders: List[Record217])
case class Record250(l_qty: Double)
case class Record251(c_name: String, p_name: String, month: String, t_qty: Int)
 val x215 = C.flatMap({x188 => 
   val x189 = x188.c_name
  val x212 = O.withFilter({x190 => val x191 = x190.o_custkey
  val x192 = x188.c_custkey
  val x193 = x191 == x192
  x193}).flatMap({x190 => 
     val x194 = x190.o_orderdate
    val x209 = L.flatMap({x195 => 
       val x208 = P.withFilter({x196 => val x197 = x195.l_orderkey
      val x198 = x190.o_orderkey
      val x199 = x197 == x198
      val x200 = x195.l_partkey
      val x201 = x196.p_partkey
      val x202 = x200 == x201
      val x203 = x199 && x202
      x203}).flatMap({x196 => 
         val x204 = x196.p_name
        val x205 = x195.l_quantity
        val x206 = Record216(x204, x205)
        val x207 = List(x206)
        x207})
      x208})
    val x210 = Record217(x194, x209)
    val x211 = List(x210)
    x211})
  val x213 = Record218(x189, x212)
  val x214 = List(x213)
  x214})
x215
 val Q1 = x215
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
  val x249 = Q1.flatMap({x225 =>
   val x226 = x225.c_orders
  val x248 = x226.flatMap({x227 =>
     val x228 = x227.o_parts
    val x247 = x228.flatMap({x229 =>
       val x230 = x225.c_name
      val x231 = x229.p_name
      val x232 = x227.o_orderdate
      val x233 = x227.o_parts
      val x244 = x233.withFilter({x234 => val x235 = x234.p_name
      val x236 = x229.p_name
      val x237 = x235 == x236
      x237}).flatMap({x234 =>
         val x238 = x229.l_qty
        val x239 = Record250(x238)
        val x240 = x234.l_qty
        val x241 = (1 to x240.asInstanceOf[Int]).map(v => x239)
        val x243 = x241.map({x242 => 1})
        x243})
      val x245 = Record251(x230, x231, x232, x244)
      val x246 = List(x245)
      x246})
    x247})
  x248}).sum
x249
 }
}
