
/** Generated code **/

import shredding.queries.tpch._
val C = TPCHLoader.loadCustomer.toList
val O = TPCHLoader.loadOrders.toList
val L = TPCHLoader.loadLineitem.toList
val P = TPCHLoader.loadPart.toList
case class Record190(p_name: String, l_qty: Double)
case class Record191(o_orderdate: String, o_parts: List[Record190])
case class Record192(c_name: String, c_orders: List[Record191])
var start = System.currentTimeMillis()
val x189 = C.flatMap({x162 => 
   val x163 = x162.c_name
  val x186 = O.withFilter({x164 => val x165 = x164.o_custkey
  val x166 = x162.c_custkey
  val x167 = x165 == x166
  x167}).flatMap({x164 => 
     val x168 = x164.o_orderdate
    val x183 = L.flatMap({x169 => 
       val x182 = P.withFilter({x170 => val x171 = x169.l_orderkey
      val x172 = x164.o_orderkey
      val x173 = x171 == x172
      val x174 = x169.l_partkey
      val x175 = x170.p_partkey
      val x176 = x174 == x175
      val x177 = x173 && x176
      x177}).flatMap({x170 => 
         val x178 = x170.p_name
        val x179 = x169.l_quantity
        val x180 = Record190(x178, x179)
        val x181 = List(x180)
        x181})
      x182})
    val x184 = Record191(x168, x183)
    val x185 = List(x184)
    x185})
  val x187 = Record192(x163, x186)
  val x188 = List(x187)
  x188})
x189
var end = System.currentTimeMillis() - start
