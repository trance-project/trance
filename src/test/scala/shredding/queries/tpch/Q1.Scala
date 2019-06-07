
/** Generated code **/

import shredding.queries.tpch._
val C = TPCHLoader.loadCustomer.toList
val O = TPCHLoader.loadOrders.toList
val L = TPCHLoader.loadLineitem.toList
val P = TPCHLoader.loadPart.toList
case class Record196(p_name: String, l_qty: Double)
case class Record197(o_orderdate: String, o_parts: List[Record196])
case class Record198(c_name: String, c_orders: List[Record197])
var start = System.currentTimeMillis()
val x195 = C.flatMap({x168 => 
   val x169 = x168.c_name
  val x192 = O.withFilter({x170 => val x171 = x170.o_custkey
  val x172 = x168.c_custkey
  val x173 = x171 == x172
  x173}).flatMap({x170 => 
     val x174 = x170.o_orderdate
    val x189 = L.flatMap({x175 => 
       val x188 = P.withFilter({x176 => val x177 = x175.l_orderkey
      val x178 = x170.o_orderkey
      val x179 = x177 == x178
      val x180 = x175.l_partkey
      val x181 = x176.p_partkey
      val x182 = x180 == x181
      val x183 = x179 && x182
      x183}).flatMap({x176 => 
         val x184 = x176.p_name
        val x185 = x175.l_quantity
        val x186 = Record196(x184, x185)
        val x187 = List(x186)
        x187})
      x188})
    val x190 = Record197(x174, x189)
    val x191 = List(x190)
    x191})
  val x193 = Record198(x169, x192)
  val x194 = List(x193)
  x194})
x195
var end = System.currentTimeMillis() - start
