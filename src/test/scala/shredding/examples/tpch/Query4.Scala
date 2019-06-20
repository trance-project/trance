
package experiments
/** Generated code **/
object Query4 {
 
import shredding.examples.tpch._
val C = TPCHLoader.loadCustomer.toList
val O = TPCHLoader.loadOrders.toList
val L = TPCHLoader.loadLineitem.toList
val P = TPCHLoader.loadPart.toList
 case class Record241(p_name: String, l_qty: Double)
case class Record242(o_orderdate: String, o_parts: List[Record241])
case class Record243(c_name: String, c_orders: List[Record242])
case class Record279(l_qty: Double)
case class Record281(c_name: String, p_name: String, month: String, t_qty: Int)
 var start0 = System.currentTimeMillis()
 val x236 = C.flatMap(x209 =>  
  {  val x210 = x209.c_name
    val x233 = O.flatMap(x211 =>  
      if({val x212 = x211.o_custkey
      val x213 = x209.c_custkey
      val x214 = x212 == x213
      x214}) {  val x215 = x211.o_orderdate
        val x230 = L.flatMap(x216 =>  
          {  val x229 = P.flatMap(x217 =>  
              if({val x218 = x216.l_orderkey
              val x219 = x211.o_orderkey
              val x220 = x218 == x219
              val x221 = x216.l_partkey
              val x222 = x217.p_partkey
              val x223 = x221 == x222
              val x224 = x220 && x223
              x224}) {  val x225 = x217.p_name
                val x226 = x216.l_quantity
                val x227 = Record241(x225, x226)
                val x228 = List(x227)
                x228} else {  Nil})
            x229})
        val x231 = Record242(x215, x230)
        val x232 = List(x231)
        x232} else {  Nil})
    val x234 = Record243(x210, x233)
    val x235 = List(x234)
    x235})
x236
 val Q1 = x236
 var end0 = System.currentTimeMillis() - start0
 println("setup time: "+end0)
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
  val x274 = Q1.flatMap(x250 =>  
  {  val x251 = x250.c_orders
    val x273 = x251.flatMap(x252 =>  
      {  val x253 = x252.o_parts
        val x272 = x253.flatMap(x254 =>  
          {  val x255 = x250.c_name
            val x256 = x254.p_name
            val x257 = x252.o_orderdate
            val x258 = x252.o_parts
            val x269 = x258.foldLeft(0)((acc278, x259) => 
              if({val x260 = x259.p_name
              val x261 = x254.p_name
              val x262 = x260 == x261
              x262}) {  acc278 + {val x263 = x254.l_qty
                val x264 = Record279(x263)
                val x265 = x259.l_qty
                val x266 = (1 to x265.asInstanceOf[Int]).map(v => x264)
                val x268 = x266.foldLeft(0)((acc280, x267) => 
                  {  acc280 + {1}})
                x268}} else {  acc278})
            val x270 = Record281(x255, x256, x257, x269)
            val x271 = List(x270)
            x271})
        x272})
    x273})
x274
 }
}
