
package experiments
/** Generated code **/
object Query4 {
 
import shredding.examples.tpch._
val C = TPCHLoader.loadCustomer.toList
val O = TPCHLoader.loadOrders.toList
val L = TPCHLoader.loadLineitem.toList
val P = TPCHLoader.loadPart.toList
 case class Record239(p_name: String, l_qty: Double)
case class Record240(o_orderdate: String, o_parts: List[Record239])
case class Record241(c_name: String, c_orders: List[Record240])
case class Record277(l_qty: Double)
case class Record279(c_name: String, p_name: String, month: String, t_qty: Int)
 val x234 = C.flatMap(acc235 =>  
  {  val x208 = x207.c_name
    val x231 = O.flatMap(acc236 =>  
      if({val x210 = x209.o_custkey
      val x211 = x207.c_custkey
      val x212 = x210 == x211
      x212}) {  val x213 = x209.o_orderdate
        val x228 = L.flatMap(acc237 =>  
          {  val x227 = P.flatMap(acc238 =>  
              if({val x216 = x214.l_orderkey
              val x217 = x209.o_orderkey
              val x218 = x216 == x217
              val x219 = x214.l_partkey
              val x220 = x215.p_partkey
              val x221 = x219 == x220
              val x222 = x218 && x221
              x222}) {  val x223 = x215.p_name
                val x224 = x214.l_quantity
                val x225 = Record239(x223, x224)
                val x226 = List(x225)
                x226} else {  Nil})
            x227})
        val x229 = Record240(x213, x228)
        val x230 = List(x229)
        x230} else {  Nil})
    val x232 = Record241(x208, x231)
    val x233 = List(x232)
    x233})
x234
 val Q1 = x234
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
  val x272 = Q1.flatMap(acc273 =>  
  {  val x249 = x248.c_orders
    val x271 = x249.flatMap(acc274 =>  
      {  val x251 = x250.o_parts
        val x270 = x251.flatMap(acc275 =>  
          {  val x253 = x248.c_name
            val x254 = x252.p_name
            val x255 = x250.o_orderdate
            val x256 = x250.o_parts
            val x267 = x256.foldLeft(0)((acc276, x257) => 
              if({val x258 = x257.p_name
              val x259 = x252.p_name
              val x260 = x258 == x259
              x260}) {  acc276 + val x261 = x252.l_qty
                val x262 = Record277(x261)
                val x263 = x257.l_qty
                val x264 = (1 to x263.asInstanceOf[Int]).map(v => x262)
                val x266 = x264.foldLeft(0)((acc278, x265) => 
                  {  acc278 + 1})
                x266} else {  acc276})
            val x268 = Record279(x253, x254, x255, x267)
            val x269 = List(x268)
            x269})
        x270})
    x271})
x272
 }
}
