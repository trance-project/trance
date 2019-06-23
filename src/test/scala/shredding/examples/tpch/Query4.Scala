
package experiments
/** Generated code **/
object Query4 {
 
import shredding.examples.tpch._
val C = TPCHLoader.loadCustomer.toList
val O = TPCHLoader.loadOrders.toList
val L = TPCHLoader.loadLineitem.toList
val P = TPCHLoader.loadPart.toList
 case class Record238(p_name: String, l_qty: Double)
case class Record239(o_orderdate: String, o_parts: List[Record238])
case class Record240(c_name: String, c_orders: List[Record239])
case class Record276(l_qty: Double)
case class Record278(c_name: String, p_name: String, month: String, t_qty: Int)
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    val x233 = C.flatMap(x207 =>  
  {  val x208 = x207.c_name
    val x230 = O.flatMap(x209 =>  
      if({val x210 = x209.o_custkey
      val x211 = x207.c_custkey
      val x212 = x210 == x211
      x212}) {  val x213 = x209.o_orderdate
        val x227 = L.flatMap(x214 =>  
          if({val x215 = x214.l_orderkey
          val x216 = x209.o_orderkey
          val x217 = x215 == x216
          x217}) {  val x226 = P.flatMap(x218 =>  
              if({val x219 = x214.l_partkey
              val x220 = x218.p_partkey
              val x221 = x219 == x220
              x221}) {  val x222 = x218.p_name
                val x223 = x214.l_quantity
                val x224 = Record238(x222, x223)
                val x225 = List(x224)
                x225} else {  Nil})
            x226} else {  Nil})
        val x228 = Record239(x213, x227)
        val x229 = List(x228)
        x229} else {  Nil})
    val x231 = Record240(x208, x230)
    val x232 = List(x231)
    x232})
x233
    val Q1 = x233
    var end0 = System.currentTimeMillis() - start0
    println("setup time: "+end0)
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
  val x271 = Q1.flatMap(x247 =>  
  {  val x248 = x247.c_orders
    val x270 = x248.flatMap(x249 =>  
      {  val x250 = x249.o_parts
        val x269 = x250.flatMap(x251 =>  
          {  val x252 = x247.c_name
            val x253 = x251.p_name
            val x254 = x249.o_orderdate
            val x255 = x249.o_parts
            val x266 = x255.foldLeft(0)((acc275, x256) => 
              if({val x257 = x256.p_name
              val x258 = x251.p_name
              val x259 = x257 == x258
              x259}) {  acc275 + {val x260 = x251.l_qty
                val x261 = Record276(x260)
                val x262 = x256.l_qty
                val x263 = (1 to x262.asInstanceOf[Int]).map(v => x261)
                val x265 = x263.foldLeft(0)((acc277, x264) => 
                  {  acc277 + {1}})
                x265}} else {  acc275})
            val x267 = Record278(x252, x253, x254, x266)
            val x268 = List(x267)
            x268})
        x269})
    x270})
x271
 }
}
