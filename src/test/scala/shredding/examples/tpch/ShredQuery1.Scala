
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record280(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record281(c_name: String, c_orders: Record280)
case class Record287(o__F: Orders, P__F: Int, L__F: Int)
case class Record288(o_orderdate: String, o_parts: Record287)
case class Record296(p_name: String, l_qty: Double)
case class RecM_flat2(_1: Record280, _2: List[Record288])
case class RecM_flat3(_1: Record287, _2: List[Record296])
case class RecM_flat1(_1: Q1Flat, _2: List[Record281])
case class RecM_ctx3(lbl: Record287)
case class RecM_ctx2(lbl: Record280)
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
    def f(){
      val x138 = Q1Flat(P__F, C__F, L__F, O__F)
val x139 = RecM_ctx1(x138)
val x140 = List(x139)
val M_ctx1 = x140
val x141 = M_ctx1
val x143 = M_ctx1
val x144 = C__D._1
val x146 = x144
val x152 = { val hm277 = x146.toMap
x143.flatMap{ case x147 => hm277.get({val x149 = x147.lbl
val x150 = x149.C__F
x150}) match {
 case Some(a) => a.map(v1 => (x147, v1))
 case _ => Nil
}}}
val x164 = { val grps278 = x152.groupBy{ case (x153, x154) => { val x155 = (x153)
x155 }}
 grps278.map(x163 => (x163._1, x163._2.map{case (x153, x154) => val x156 = x154.c_name
val x157 = x153.lbl
val x158 = x157.P__F
val x159 = x157.L__F
val x160 = x157.O__F
val x161 = Record280(x158, x159, x160, x154)
val x162 = Record281(x156, x161)
x162})).toList }
val x169 = x164.map{ case (x165, x166) => { 
  val x167 = x165.lbl
  val x168 = RecM_flat1(x167, x166)
  x168 }}
val M_flat1 = x169
val x170 = M_flat1
val x172 = M_flat1
val x176 = x172.flatMap{ case x173 => 
  val x174 = x173._2
  x174.map(x175 => {
  val nv282 = (x173, x175) 
     nv282
})}
val x181 = x176.map{ case (x177, x178) => { 
  val x179 = x178.c_orders
  val x180 = RecM_ctx2(x179)
  x180 }}
val x182 = x181.distinct
val M_ctx2 = x182
val x183 = M_ctx2
val x185 = M_ctx2
val x186 = O__D._1
val x188 = x186
val x199 = { val hm284 = x188.flatMap(v1 => v1._2.map(x190 => ({val x196 = v1._1
val x197 = x190.o_custkey
val x198 = (x196,x197)
x198}, v1._2))).toMap
x185.flatMap{ case x189 => hm284.get({val x191 = x189.lbl
val x192 = x191.O__F
val x193 = x191.c__F
val x194 = x193.c_custkey
val x195 = (x192,x194)
x195}) match {
 case Some(a) => a.map(v1 => (x189, v1))
 case _ => Nil
}}}
val x210 = { val grps285 = x199.groupBy{ case (x200, x201) => { val x202 = (x200)
x202 }}
 grps285.map(x209 => (x209._1, x209._2.map{case (x200, x201) => val x203 = x201.o_orderdate
val x204 = x200.lbl
val x205 = x204.P__F
val x206 = x204.L__F
val x207 = Record287(x201, x205, x206)
val x208 = Record288(x203, x207)
x208})).toList }
val x215 = x210.map{ case (x211, x212) => { 
  val x213 = x211.lbl
  val x214 = RecM_flat2(x213, x212)
  x214 }}
val M_flat2 = x215
val x216 = M_flat2
val x218 = M_flat2
val x222 = x218.flatMap{ case x219 => 
  val x220 = x219._2
  x220.map(x221 => {
  val nv289 = (x219, x221) 
     nv289
})}
val x227 = x222.map{ case (x223, x224) => { 
  val x225 = x224.o_parts
  val x226 = RecM_ctx3(x225)
  x226 }}
val x228 = x227.distinct
val M_ctx3 = x228
val x229 = M_ctx3
val x231 = M_ctx3
val x232 = L__D._1
val x234 = x232
val x245 = { val hm291 = x234.flatMap(v1 => v1._2.map(x236 => ({val x242 = v1._1
val x243 = x236.l_orderkey
val x244 = (x242,x243)
x244}, v1._2))).toMap
x231.flatMap{ case x235 => hm291.get({val x237 = x235.lbl
val x238 = x237.L__F
val x239 = x237.o__F
val x240 = x239.o_orderkey
val x241 = (x238,x240)
x241}) match {
 case Some(a) => a.map(v1 => (x235, v1))
 case _ => Nil
}}}
val x246 = P__D._1
val x248 = x246
val x259 = { val hm293 = x248.flatMap(v1 => v1._2.map(x251 => ({val x256 = v1._1
val x257 = x251.p_partkey
val x258 = (x256,x257)
x258}, v1._2))).toMap
x245.flatMap{ case (x249, x250) => hm293.get({val x252 = x249.lbl
val x253 = x252.P__F
val x254 = x250.l_partkey
val x255 = (x253,x254)
x255}) match {
 case Some(a) => a.map(v1 => ((x249, x250), v1))
 case _ => Nil
}}}
val x268 = { val grps294 = x259.groupBy{ case ((x260, x261), x262) => { val x263 = (x260)
x263 }}
 grps294.map(x267 => (x267._1, x267._2.map{case ((x260, x261), x262) => val x264 = x262.p_name
val x265 = x261.l_quantity
val x266 = Record296(x264, x265)
x266})).toList }
val x273 = x268.map{ case (x269, x270) => { 
  val x271 = x269.lbl
  val x272 = RecM_flat3(x271, x270)
  x272 }}
val M_flat3 = x273
val x274 = M_flat3
val x275 = (x141,x170,x183,x216,x229,x274)
x275 
    }
    var end0 = System.currentTimeMillis() - start0
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
