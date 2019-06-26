
package experiments
/** Generated code **/
import shredding.examples.tpch._
object ShredQuery1 {
 case class Record259(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record260(c_name: String, c_orders: Record259)
case class Record267(o__F: Orders, P__F: Int, L__F: Int)
case class Record268(o_orderdate: String, o_parts: Record267)
case class Record278(p_name: String, l_qty: Double)
case class RecM_flat2(_1: Record259, _2: List[Record268])
case class RecM_flat3(_1: Record267, _2: List[Record278])
case class RecM_flat1(_1: Q1Flat, _2: List[Record260])
case class RecM_ctx3(lbl: Record267)
case class RecM_ctx2(lbl: Record259)
case class RecM_ctx1(lbl: Q1Flat)
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
    var time = List[Long]()
    for (i <- 1 to 5) {
      var start = System.currentTimeMillis()
      f(C__F, C__D, O__F, O__D, L__F, L__D, P__F, P__D)
      var end = System.currentTimeMillis() - start
      time = time :+ end
    }
    val avg = (time.sum/5)
    println(end0+","+avg)
 }
 def f(C__F: Int, C__D: (List[(Int, List[Customer])], Unit),
                     O__F: Int, O__D: (List[(Int, List[Orders])], Unit),
                     L__F: Int, L__D: (List[(Int, List[Lineitem])], Unit),
                     P__F: Int, P__D: (List[(Int, List[Part])], Unit)){
  val x140 = Q1Flat(P__F, C__F, L__F, O__F)
val x141 = RecM_ctx1(x140)
val x142 = List(x141)
val M_ctx1 = x142
val x143 = M_ctx1
val x166 = M_ctx1.flatMap(x144 =>  
  {  val x145 = x144.lbl
    val x146 = C__D._1
    val x163 = x146.flatMap(x147 =>  
      if({val x148 = x144.lbl
      val x149 = x148.C__F
      val x150 = x147._1
      val x151 = x149 == x150
      x151}) {  val x152 = x147._2
        val x162 = x152.flatMap(x153 =>  
          {  val x154 = x153.c_name
            val x155 = x144.lbl
            val x156 = x155.P__F
            val x157 = x155.L__F
            val x158 = x155.O__F
            val x159 = Record259(x156, x157, x158, x153)
            val x160 = Record260(x154, x159)
            val x161 = List(x160)
            x161})
        x162} else {  Nil})
    val x164 = RecM_flat1(x145, x163)
    val x165 = List(x164)
    x165})
val M_flat1 = x166
val x167 = M_flat1
val x175 = M_flat1.flatMap(x168 =>  
  {  val x169 = x168._2
    val x174 = x169.flatMap(x170 =>  
      {  val x171 = x170.c_orders
        val x172 = RecM_ctx2(x171)
        val x173 = List(x172)
        x173})
    x174})
val x176 = x175.distinct
val M_ctx2 = x176
val x177 = M_ctx2
val x204 = M_ctx2.flatMap(x178 =>  
  {  val x179 = x178.lbl
    val x180 = O__D._1
    val x201 = x180.flatMap(x181 =>  
      if({val x182 = x178.lbl
      val x183 = x182.O__F
      val x184 = x181._1
      val x185 = x183 == x184
      x185}) {  val x186 = x181._2
        val x200 = x186.flatMap(x187 =>  
          if({val x188 = x187.o_custkey
          val x189 = x178.lbl
          val x190 = x189.c__F
          val x191 = x190.c_custkey
          val x192 = x188 == x191
          x192}) {  val x193 = x187.o_orderdate
            val x194 = x178.lbl
            val x195 = x194.P__F
            val x196 = x194.L__F
            val x197 = Record267(x187, x195, x196)
            val x198 = Record268(x193, x197)
            val x199 = List(x198)
            x199} else {  Nil})
        x200} else {  Nil})
    val x202 = RecM_flat2(x179, x201)
    val x203 = List(x202)
    x203})
val M_flat2 = x204
val x205 = M_flat2
val x213 = M_flat2.flatMap(x206 =>  
  {  val x207 = x206._2
    val x212 = x207.flatMap(x208 =>  
      {  val x209 = x208.o_parts
        val x210 = RecM_ctx3(x209)
        val x211 = List(x210)
        x211})
    x212})
val x214 = x213.distinct
val M_ctx3 = x214
val x215 = M_ctx3
val x252 = M_ctx3.flatMap(x216 =>  
  {  val x217 = x216.lbl
    val x218 = L__D._1
    val x249 = x218.flatMap(x219 =>  
      if({val x220 = x216.lbl
      val x221 = x220.L__F
      val x222 = x219._1
      val x223 = x221 == x222
      x223}) {  val x224 = x219._2
        val x248 = x224.flatMap(x225 =>  
          if({val x226 = x225.l_orderkey
          val x227 = x216.lbl
          val x228 = x227.o__F
          val x229 = x228.o_orderkey
          val x230 = x226 == x229
          x230}) {  val x231 = P__D._1
            val x247 = x231.flatMap(x232 =>  
              if({val x233 = x216.lbl
              val x234 = x233.P__F
              val x235 = x232._1
              val x236 = x234 == x235
              x236}) {  val x237 = x232._2
                val x246 = x237.flatMap(x238 =>  
                  if({val x239 = x225.l_partkey
                  val x240 = x238.p_partkey
                  val x241 = x239 == x240
                  x241}) {  val x242 = x238.p_name
                    val x243 = x225.l_quantity
                    val x244 = Record278(x242, x243)
                    val x245 = List(x244)
                    x245} else {  Nil})
                x246} else {  Nil})
            x247} else {  Nil})
        x248} else {  Nil})
    val x250 = RecM_flat3(x217, x249)
    val x251 = List(x250)
    x251})
val M_flat3 = x252
val x253 = M_flat3
val x254 = (x143,x167,x177,x205,x215,x253)
x254
 }
}
