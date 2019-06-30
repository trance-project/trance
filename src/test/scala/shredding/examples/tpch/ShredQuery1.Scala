
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record158(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record159(c_name: String, c_orders: Record158)
case class Record165(o__F: Orders, P__F: Int, L__F: Int)
case class Record166(o_orderdate: String, o_parts: Record165)
case class Record174(p_name: String, l_qty: Double)
case class RecM_flat2(_1: Record158, _2: List[Record166])
case class RecM_flat3(_1: Record165, _2: List[Record174])
case class RecM_flat1(_1: Q1Flat, _2: List[Record159])
case class RecM_ctx3(lbl: Record165)
case class RecM_ctx2(lbl: Record158)
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
      val x16 = Q1Flat(P__F, C__F, L__F, O__F)
val x17 = RecM_ctx1(x16)
val x18 = List(x17)
val M_ctx1 = x18
val x19 = M_ctx1
val x21 = M_ctx1
val x22 = C__D._1
val x24 = x22
val x31 = { val hm155 = x24.toMap
 x21.flatMap{case x25 => hm155.get({ val x27 = x25.lbl
val x28 = x27.C__F
x28 }) match {
 case Some(a) => a.withFilter(x26 => { val x30 = true == true
x30 } ).map(x26 => (x25, x26))
 case _ => Nil
}}}
val x43 = { val grps156 = x31.groupBy{ case (x32, x33) => { val x34 = (x32)
x34 }}
 grps156.map(x42 => (x42._1, x42._2.map{case (x32, x33) => val x35 = x33.c_name
val x36 = x32.lbl
val x37 = x36.P__F
val x38 = x36.L__F
val x39 = x36.O__F
val x40 = Record158(x37, x38, x39, x33)
val x41 = Record159(x35, x40)
x41})).toList }
val x48 = x43.map{ case (x44, x45) => { 
  val x46 = x44.lbl
  val x47 = RecM_flat1(x46, x45)
  x47 }}
val M_flat1 = x48
val x49 = M_flat1
val x51 = M_flat1
val x55 = x51.flatMap{ case x52 => 
  val x53 = x52._2
  x53.map(x54 => {
  val nv160 = (x52, x54) 
     nv160
})}
val x60 = x55.map{ case (x56, x57) => { 
  val x58 = x57.c_orders
  val x59 = RecM_ctx2(x58)
  x59 }}
val x61 = x60.distinct
val M_ctx2 = x61
val x62 = M_ctx2
val x64 = M_ctx2
val x65 = O__D._1
val x67 = x65
val x78 = { val hm162 = x67.toMap
 x64.flatMap{case x68 => hm162.get({ val x70 = x68.lbl
val x71 = x70.O__F
x71 }) match {
 case Some(a) => a.withFilter(x69 => { val x73 = x68.lbl
val x74 = x73.c__F
val x75 = x74.c_custkey
val x76 = x69.o_custkey
val x77 = x75 == x76
x77 } ).map(x69 => (x68, x69))
 case _ => Nil
}}}
val x89 = { val grps163 = x78.groupBy{ case (x79, x80) => { val x81 = (x79)
x81 }}
 grps163.map(x88 => (x88._1, x88._2.map{case (x79, x80) => val x82 = x80.o_orderdate
val x83 = x79.lbl
val x84 = x83.P__F
val x85 = x83.L__F
val x86 = Record165(x80, x84, x85)
val x87 = Record166(x82, x86)
x87})).toList }
val x94 = x89.map{ case (x90, x91) => { 
  val x92 = x90.lbl
  val x93 = RecM_flat2(x92, x91)
  x93 }}
val M_flat2 = x94
val x95 = M_flat2
val x97 = M_flat2
val x101 = x97.flatMap{ case x98 => 
  val x99 = x98._2
  x99.map(x100 => {
  val nv167 = (x98, x100) 
     nv167
})}
val x106 = x101.map{ case (x102, x103) => { 
  val x104 = x103.o_parts
  val x105 = RecM_ctx3(x104)
  x105 }}
val x107 = x106.distinct
val M_ctx3 = x107
val x108 = M_ctx3
val x110 = M_ctx3
val x111 = L__D._1
val x113 = x111
val x124 = { val hm169 = x113.toMap
 x110.flatMap{case x114 => hm169.get({ val x116 = x114.lbl
val x117 = x116.L__F
x117 }) match {
 case Some(a) => a.withFilter(x115 => { val x119 = x114.lbl
val x120 = x119.o__F
val x121 = x120.o_orderkey
val x122 = x115.l_orderkey
val x123 = x121 == x122
x123 } ).map(x115 => (x114, x115))
 case _ => Nil
}}}
val x125 = P__D._1
val x127 = x125
val x137 = { val hm171 = x127.toMap
 x124.flatMap{case (x128, x129) => hm171.get({ val x131 = x128.lbl
val x132 = x131.P__F
x132 }) match {
 case Some(a) => a.withFilter(x130 => { val x134 = x129.l_partkey
val x135 = x130.p_partkey
val x136 = x134 == x135
x136 } ).map(x130 => ((x128, x129), x130))
 case _ => Nil
}}}
val x146 = { val grps172 = x137.groupBy{ case ((x138, x139), x140) => { val x141 = (x138)
x141 }}
 grps172.map(x145 => (x145._1, x145._2.map{case ((x138, x139), x140) => val x142 = x140.p_name
val x143 = x139.l_quantity
val x144 = Record174(x142, x143)
x144})).toList }
val x151 = x146.map{ case (x147, x148) => { 
  val x149 = x147.lbl
  val x150 = RecM_flat3(x149, x148)
  x150 }}
val M_flat3 = x151
val x152 = M_flat3
val x153 = (x19,x49,x62,x95,x108,x152)
x153 
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
