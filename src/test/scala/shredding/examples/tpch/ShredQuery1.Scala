
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record277(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record278(c_name: String, c_orders: Record277)
case class Record284(o__F: Orders, P__F: Int, L__F: Int)
case class Record285(o_orderdate: String, o_parts: Record284)
case class Record293(p_name: String, l_qty: Double)
case class RecM_flat2(_1: Record277, _2: List[Record285])
case class RecM_flat3(_1: Record284, _2: List[Record293])
case class RecM_flat1(_1: Q1Flat, _2: List[Record278])
case class RecM_ctx3(lbl: Record284)
case class RecM_ctx2(lbl: Record277)
case class RecM_ctx1(lbl: Q1Flat)
object ShredQuery1 {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C__F = 1
val C__D = (List((C__F, TPCHLoader.loadCustomer[Customer].toList)), ())
val O__F = 2
val O__D = (List((O__F, TPCHLoader.loadOrders[Orders].toList)), ())
val L__F = 3
val L__D = (List((L__F, TPCHLoader.loadLineitem[Lineitem].toList)), ())
val P__F = 4
val P__D = (List((P__F, TPCHLoader.loadPart[Part].toList)), ())
    var end0 = System.currentTimeMillis() - start0
    def f(){
      val x143 = Q1Flat(P__F, C__F, L__F, O__F)
val x144 = RecM_ctx1(x143)
val x145 = List(x144)
val M_ctx1 = x145
val x146 = M_ctx1
val x148 = M_ctx1
val x149 = C__D._1
val x151 = x149
val x156 = { val hm274 = x148.groupBy{case x152 => { val x154 = x152.lbl
val x155 = x154.C__F
x155 } }
 x151.flatMap{x153 => hm274.get(x153._1) match {
 case Some(a) => a.map(a1 => (a1, x153._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x168 = { val grps275 = x156.groupBy{ case (x157, x158) => { val x159 = (x157)
x159 }}
 grps275.map(x167 => (x167._1, x167._2.map{case (x157, x158) => val x160 = x158.c_name
val x161 = x157.lbl
val x162 = x161.P__F
val x163 = x161.L__F
val x164 = x161.O__F
val x165 = Record277(x162, x163, x164, x158)
val x166 = Record278(x160, x165)
x166})).toList }
val x173 = x168.map{ case (x169, x170) => { 
  val x171 = x169.lbl
  val x172 = RecM_flat1(x171, x170)
  x172 }}
val M_flat1 = x173
val x174 = M_flat1
val x176 = M_flat1
val x180 = x176.flatMap{ case x177 => 
  val x178 = x177._2
  x178.map(x179 => {
  val nv279 = (x177, x179) 
     nv279
})}
val x185 = x180.map{ case (x181, x182) => { 
  val x183 = x182.c_orders
  val x184 = RecM_ctx2(x183)
  x184 }}
val x186 = x185.distinct
val M_ctx2 = x186
val x187 = M_ctx2
val x189 = M_ctx2
val x190 = O__D._1
val x192 = x190
val x201 = { val hm281 = x189.groupBy{case x193 => { val x195 = x193.lbl
val x196 = x195.O__F
x196 } }
 val join1 = x192.flatMap{x194 => hm281.get(x194._1) match {
 case Some(a) => x194._2
 case _ => Nil
 }}
 val join2 = x189.groupBy{case x193 => { val x198 = x193.lbl
val x199 = x198.c__F
val x200 = x199.c_custkey
x200 } }
 join1.flatMap(x194 => join2.get({ val x197 = x194.o_custkey
x197 }) match {
   case Some(a) => a.map(a1 => (a1, x194))
   case _ => Nil
 })
}
val x212 = { val grps282 = x201.groupBy{ case (x202, x203) => { val x204 = (x202)
x204 }}
 grps282.map(x211 => (x211._1, x211._2.map{case (x202, x203) => val x205 = x203.o_orderdate
val x206 = x202.lbl
val x207 = x206.P__F
val x208 = x206.L__F
val x209 = Record284(x203, x207, x208)
val x210 = Record285(x205, x209)
x210})).toList }
val x217 = x212.map{ case (x213, x214) => { 
  val x215 = x213.lbl
  val x216 = RecM_flat2(x215, x214)
  x216 }}
val M_flat2 = x217
val x218 = M_flat2
val x220 = M_flat2
val x224 = x220.flatMap{ case x221 => 
  val x222 = x221._2
  x222.map(x223 => {
  val nv286 = (x221, x223) 
     nv286
})}
val x229 = x224.map{ case (x225, x226) => { 
  val x227 = x226.o_parts
  val x228 = RecM_ctx3(x227)
  x228 }}
val x230 = x229.distinct
val M_ctx3 = x230
val x231 = M_ctx3
val x233 = M_ctx3
val x234 = L__D._1
val x236 = x234
val x245 = { val hm288 = x233.groupBy{case x237 => { val x239 = x237.lbl
val x240 = x239.L__F
x240 } }
 val join1 = x236.flatMap{x238 => hm288.get(x238._1) match {
 case Some(a) => x238._2
 case _ => Nil
 }}
 val join2 = x233.groupBy{case x237 => { val x242 = x237.lbl
val x243 = x242.o__F
val x244 = x243.o_orderkey
x244 } }
 join1.flatMap(x238 => join2.get({ val x241 = x238.l_orderkey
x241 }) match {
   case Some(a) => a.map(a1 => (a1, x238))
   case _ => Nil
 })
}
val x246 = P__D._1
val x248 = x246
val x256 = { val hm290 = x245.groupBy{case (x249, x250) => { val x252 = x249.lbl
val x253 = x252.P__F
x253 } }
 val join1 = x248.flatMap{x251 => hm290.get(x251._1) match {
 case Some(a) => x251._2
 case _ => Nil
 }}
 val join2 = x245.groupBy{case (x249, x250) => { val x255 = x250.l_partkey
x255 } }
 join1.flatMap(x251 => join2.get({ val x254 = x251.p_partkey
x254 }) match {
   case Some(a) => a.map(a1 => (a1, x251))
   case _ => Nil
 })
}
val x265 = { val grps291 = x256.groupBy{ case ((x257, x258), x259) => { val x260 = (x257)
x260 }}
 grps291.map(x264 => (x264._1, x264._2.map{case ((x257, x258), x259) => val x261 = x259.p_name
val x262 = x258.l_quantity
val x263 = Record293(x261, x262)
x263})).toList }
val x270 = x265.map{ case (x266, x267) => { 
  val x268 = x266.lbl
  val x269 = RecM_flat3(x268, x267)
  x269 }}
val M_flat3 = x270
val x271 = M_flat3
val x272 = (x146,x174,x187,x218,x231,x271)
x272 
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
