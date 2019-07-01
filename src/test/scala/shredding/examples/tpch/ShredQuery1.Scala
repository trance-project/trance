
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record249(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record250(c_name: String, c_orders: Record249)
case class Record256(o__F: Orders, P__F: Int, L__F: Int)
case class Record257(o_orderdate: String, o_parts: Record256)
case class Record265(p_name: String, l_qty: Double)
case class RecM_flat2(_1: Record249, _2: List[Record257])
case class RecM_flat3(_1: Record256, _2: List[Record265])
case class RecM_flat1(_1: Q1Flat, _2: List[Record250])
case class RecM_ctx3(lbl: Record256)
case class RecM_ctx2(lbl: Record249)
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
      val x115 = Q1Flat(P__F, C__F, L__F, O__F)
val x116 = RecM_ctx1(x115)
val x117 = List(x116)
val M_ctx1 = x117
val x118 = M_ctx1
val x120 = M_ctx1
val x121 = C__D._1
val x123 = x121
val x128 = { val hm246 = x123.toMap
 x120.flatMap{case x124 => hm246.get({ val x126 = x124.lbl
val x127 = x126.C__F
x127 }) match {
 case Some(a) => a.map(x125 => (x124, x125))
 case _ => Nil
}}}
val x140 = { val grps247 = x128.groupBy{ case (x129, x130) => { val x131 = (x129)
x131 }}
 grps247.map(x139 => (x139._1, x139._2.map{case (x129, x130) => val x132 = x130.c_name
val x133 = x129.lbl
val x134 = x133.P__F
val x135 = x133.L__F
val x136 = x133.O__F
val x137 = Record249(x134, x135, x136, x130)
val x138 = Record250(x132, x137)
x138})).toList }
val x145 = x140.map{ case (x141, x142) => { 
  val x143 = x141.lbl
  val x144 = RecM_flat1(x143, x142)
  x144 }}
val M_flat1 = x145
val x146 = M_flat1
val x148 = M_flat1
val x152 = x148.flatMap{ case x149 => 
  val x150 = x149._2
  x150.map(x151 => {
  val nv251 = (x149, x151) 
     nv251
})}
val x157 = x152.map{ case (x153, x154) => { 
  val x155 = x154.c_orders
  val x156 = RecM_ctx2(x155)
  x156 }}
val x158 = x157.distinct
val M_ctx2 = x158
val x159 = M_ctx2
val x161 = M_ctx2
val x162 = O__D._1
val x164 = x162
val x173 = { val hm253 = x161.groupBy{case x165 => { val x167 = x165.lbl
val x168 = x167.O__F
x168 } }
 val join1 = x164.flatMap{x166 => hm253.get(x166._1) match {
 case Some(a) => x166._2
 case _ => Nil
 }}
 val join2 = x161.groupBy{case x165 => { val x170 = x165.lbl
val x171 = x170.c__F
val x172 = x171.c_custkey
x172 } }
 join1.flatMap(x166 => join2.get({ val x169 = x166.o_custkey
x169 }) match {
   case Some(a) => a.map(a1 => (a1, x166))
   case _ => Nil
 })
}
val x184 = { val grps254 = x173.groupBy{ case (x174, x175) => { val x176 = (x174)
x176 }}
 grps254.map(x183 => (x183._1, x183._2.map{case (x174, x175) => val x177 = x175.o_orderdate
val x178 = x174.lbl
val x179 = x178.P__F
val x180 = x178.L__F
val x181 = Record256(x175, x179, x180)
val x182 = Record257(x177, x181)
x182})).toList }
val x189 = x184.map{ case (x185, x186) => { 
  val x187 = x185.lbl
  val x188 = RecM_flat2(x187, x186)
  x188 }}
val M_flat2 = x189
val x190 = M_flat2
val x192 = M_flat2
val x196 = x192.flatMap{ case x193 => 
  val x194 = x193._2
  x194.map(x195 => {
  val nv258 = (x193, x195) 
     nv258
})}
val x201 = x196.map{ case (x197, x198) => { 
  val x199 = x198.o_parts
  val x200 = RecM_ctx3(x199)
  x200 }}
val x202 = x201.distinct
val M_ctx3 = x202
val x203 = M_ctx3
val x205 = M_ctx3
val x206 = L__D._1
val x208 = x206
val x217 = { val hm260 = x205.groupBy{case x209 => { val x211 = x209.lbl
val x212 = x211.L__F
x212 } }
 val join1 = x208.flatMap{x210 => hm260.get(x210._1) match {
 case Some(a) => x210._2
 case _ => Nil
 }}
 val join2 = x205.groupBy{case x209 => { val x214 = x209.lbl
val x215 = x214.o__F
val x216 = x215.o_orderkey
x216 } }
 join1.flatMap(x210 => join2.get({ val x213 = x210.l_orderkey
x213 }) match {
   case Some(a) => a.map(a1 => (a1, x210))
   case _ => Nil
 })
}
val x218 = P__D._1
val x220 = x218
val x228 = { val hm262 = x217.groupBy{case (x221, x222) => { val x224 = x221.lbl
val x225 = x224.P__F
x225 } }
 val join1 = x220.flatMap{x223 => hm262.get(x223._1) match {
 case Some(a) => x223._2
 case _ => Nil
 }}
 val join2 = x217.groupBy{case (x221, x222) => { val x227 = x222.l_partkey
x227 } }
 join1.flatMap(x223 => join2.get({ val x226 = x223.p_partkey
x226 }) match {
   case Some(a) => a.map(a1 => (a1, x223))
   case _ => Nil
 })
}
val x237 = { val grps263 = x228.groupBy{ case ((x229, x230), x231) => { val x232 = (x229)
x232 }}
 grps263.map(x236 => (x236._1, x236._2.map{case ((x229, x230), x231) => val x233 = x231.p_name
val x234 = x230.l_quantity
val x235 = Record265(x233, x234)
x235})).toList }
val x242 = x237.map{ case (x238, x239) => { 
  val x240 = x238.lbl
  val x241 = RecM_flat3(x240, x239)
  x241 }}
val M_flat3 = x242
val x243 = M_flat3
val x244 = (x118,x146,x159,x190,x203,x243)
x244 
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
