
/** Generated code **/

import shredding.queries.tpch._
val C = TPCHLoader.loadCustomer.toList
val O = TPCHLoader.loadOrders.toList
val L = TPCHLoader.loadLineitem.toList
val P = TPCHLoader.loadPart.toList
case class Record195(p_name: String, l_qty: Double)
case class Record196(o_orderdate: String, o_parts: List[Record195])
case class Record197(c_name: String, c_orders: List[Record196])
var start = System.currentTimeMillis()
val x194 = C.flatMap({x167 => 
   val x168 = x167.c_name
  val x191 = O.withFilter({x169 => val x170 = x169.o_custkey
  val x171 = x167.c_custkey
  val x172 = x170 == x171
  x172}).flatMap({x169 => 
     val x173 = x169.o_orderdate
    val x188 = L.flatMap({x174 => 
       val x187 = P.withFilter({x175 => val x176 = x174.l_orderkey
      val x177 = x169.o_orderkey
      val x178 = x176 == x177
      val x179 = x174.l_partkey
      val x180 = x175.p_partkey
      val x181 = x179 == x180
      val x182 = x178 && x181
      x182}).flatMap({x175 => 
         val x183 = x175.p_name
        val x184 = x174.l_quantity
        val x185 = Record195(x183, x184)
        val x186 = List(x185)
        x186})
      x187})
    val x189 = Record196(x173, x188)
    val x190 = List(x189)
    x190})
  val x192 = Record197(x168, x191)
  val x193 = List(x192)
  x193})
x194
var end = System.currentTimeMillis() - start
