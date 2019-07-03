
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record312(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record313(c_name: String, c_orders: Record312)
case class Record320(o__F: Orders, P__F: Int, L__F: Int)
case class Record321(o_orderdate: String, o_parts: Record320)
case class Record331(p_name: String, l_qty: Double)
case class RecM_flat2(_1: Record312, _2: List[Record321])
case class RecM_flat3(_1: Record320, _2: List[Record331])
case class RecM_flat1(_1: Q1Flat, _2: List[Record313])
case class RecM_ctx3(lbl: Record320)
case class RecM_ctx2(lbl: Record312)
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
val x157 = { val hm308 = x148.groupBy{ case x152 => {
  val x154 = x152.lbl
  val x155 = x154.C__F
  x155}}
x151.flatMap(x153 => hm308.get({val x156 = x153._1
x156}) match {
 case Some(a) => a.map(v => (v, x153))
 case _ => Nil
}) }
val x162 = x157.flatMap{ case (x158, x159) => 
  val x160 = x159._2
  x160.map(x161 => {
  val nv309 = ((x158, x159), x161) 
     nv309
})}
val x175 = { val grps310 = x162.groupBy{ case ((x163, x164), x165) => { val x166 = (x163)
x166 }}
 grps310.map(x174 => (x174._1, x174._2.map{case ((x163, x164), x165) => val x167 = x165.c_name
val x168 = x163.lbl
val x169 = x168.P__F
val x170 = x168.L__F
val x171 = x168.O__F
val x172 = Record312(x169, x170, x171, x165)
val x173 = Record313(x167, x172)
x173})).toList }
val x180 = x175.map{ case (x176, x177) => { 
  val x178 = x176.lbl
  val x179 = RecM_flat1(x178, x177)
  x179 }}
val M_flat1 = x180
val x181 = M_flat1
val x183 = M_flat1
val x187 = x183.flatMap{ case x184 => 
  val x185 = x184._2
  x185.map(x186 => {
  val nv314 = (x184, x186) 
     nv314
})}
val x192 = x187.map{ case (x188, x189) => { 
  val x190 = x189.c_orders
  val x191 = RecM_ctx2(x190)
  x191 }}
val x193 = x192.distinct
val M_ctx2 = x193
val x194 = M_ctx2
val x196 = M_ctx2
val x197 = O__D._1
val x199 = x197
val x205 = { val hm316 = x196.groupBy{ case x200 => {
  val x202 = x200.lbl
  val x203 = x202.O__F
  x203}}
x199.flatMap(x201 => hm316.get({val x204 = x201._1
x204}) match {
 case Some(a) => a.map(v => (v, x201))
 case _ => Nil
}) }
val x215 = x205.flatMap{ case (x206, x207) =>
  val x208 = x207._2
  x208.withFilter{ case x209 =>
  {val x210 = x209.o_custkey
  val x211 = x206.lbl
  val x212 = x211.c__F
  val x213 = x212.c_custkey
  val x214 = x210 == x213
  x214}}.map(x209 => ((x206, x207), x209))
}
val x227 = { val grps318 = x215.groupBy{ case ((x216, x217), x218) => { val x219 = (x216)
x219 }}
 grps318.map(x226 => (x226._1, x226._2.map{case ((x216, x217), x218) => val x220 = x218.o_orderdate
val x221 = x216.lbl
val x222 = x221.P__F
val x223 = x221.L__F
val x224 = Record320(x218, x222, x223)
val x225 = Record321(x220, x224)
x225})).toList }
val x232 = x227.map{ case (x228, x229) => { 
  val x230 = x228.lbl
  val x231 = RecM_flat2(x230, x229)
  x231 }}
val M_flat2 = x232
val x233 = M_flat2
val x235 = M_flat2
val x239 = x235.flatMap{ case x236 => 
  val x237 = x236._2
  x237.map(x238 => {
  val nv322 = (x236, x238) 
     nv322
})}
val x244 = x239.map{ case (x240, x241) => { 
  val x242 = x241.o_parts
  val x243 = RecM_ctx3(x242)
  x243 }}
val x245 = x244.distinct
val M_ctx3 = x245
val x246 = M_ctx3
val x248 = M_ctx3
val x249 = L__D._1
val x251 = x249
val x257 = { val hm324 = x248.groupBy{ case x252 => {
  val x254 = x252.lbl
  val x255 = x254.L__F
  x255}}
x251.flatMap(x253 => hm324.get({val x256 = x253._1
x256}) match {
 case Some(a) => a.map(v => (v, x253))
 case _ => Nil
}) }
val x267 = x257.flatMap{ case (x258, x259) =>
  val x260 = x259._2
  x260.withFilter{ case x261 =>
  {val x262 = x261.l_orderkey
  val x263 = x258.lbl
  val x264 = x263.o__F
  val x265 = x264.o_orderkey
  val x266 = x262 == x265
  x266}}.map(x261 => ((x258, x259), x261))
}
val x268 = P__D._1
val x270 = x268
val x278 = { val hm327 = x267.groupBy{ case ((x271, x272), x273) => {
  val x275 = x271.lbl
  val x276 = x275.P__F
  x276}}
x270.flatMap(x274 => hm327.get({val x277 = x274._1
x277}) match {
 case Some(a) => a.map(v => (v, x274))
 case _ => Nil
}) }
val x288 = x278.flatMap{ case (((x279, x280), x281), x282) =>
  val x283 = x282._2
  x283.withFilter{ case x284 =>
  {val x285 = x281.l_partkey
  val x286 = x284.p_partkey
  val x287 = x285 == x286
  x287}}.map(x284 => ((((x279, x280), x281), x282), x284))
}
val x299 = { val grps329 = x288.groupBy{ case ((((x289, x290), x291), x292), x293) => { val x294 = (x289)
x294 }}
 grps329.map(x298 => (x298._1, x298._2.map{case ((((x289, x290), x291), x292), x293) => val x295 = x293.p_name
val x296 = x291.l_quantity
val x297 = Record331(x295, x296)
x297})).toList }
val x304 = x299.map{ case (x300, x301) => { 
  val x302 = x300.lbl
  val x303 = RecM_flat3(x302, x301)
  x303 }}
val M_flat3 = x304
val x305 = M_flat3
val x306 = (x146,x181,x194,x233,x246,x305)
x306 
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
