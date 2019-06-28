
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record151(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record152(c_name: String, c_orders: Record151)
case class Record158(o__F: Orders, P__F: Int, L__F: Int)
case class Record159(o_orderdate: String, o_parts: Record158)
case class Record167(p_name: String, l_qty: Double)
case class RecM_flat2(_1: Record151, _2: List[Record159])
case class RecM_flat3(_1: Record158, _2: List[Record167])
case class RecM_flat1(_1: Q1Flat, _2: List[Record152])
case class RecM_ctx3(lbl: Record158)
case class RecM_ctx2(lbl: Record151)
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
      val x16 = Q1Flat(P__F, C__F, L__F, O__F)
val x17 = RecM_ctx1(x16)
val x18 = List(x17)
val M_ctx1 = x18
val x19 = M_ctx1
val x21 = M_ctx1
val x22 = C__D.flat
val x24 = x22
val x29 = { val hm148 = x21.groupBy{ case x25 => {
  val x27 = x25.lbl
  val x28 = x27.C__F
  x28}}
x24.flatMap(x26 => hm148.get({true}) match {
 case Some(a) => a.map(v => (v, x26))
 case _ => Nil
}) }
val x41 = { val grps149 = x29.groupBy{ case (x30, x31) => { val x32 = (x30)
x32 }}
 grps149.map(x40 => (x40._1, x40._2.map{case (x30, x31) => val x33 = x31.c_name
val x34 = x30.lbl
val x35 = x34.P__F
val x36 = x34.L__F
val x37 = x34.O__F
val x38 = Record151(x35, x36, x37, x31)
val x39 = Record152(x33, x38)
x39})).toList }
val x46 = x41.map{ case (x42, x43) => { 
  val x44 = x42.lbl
  val x45 = RecM_flat1(x44, x43)
  x45 }}
val M_flat1 = x46
val x47 = M_flat1
val x49 = M_flat1
val x53 = x49.flatMap{ case x50 => 
  val x51 = x50._2
  x51.map(x52 => {
  val nv153 = (x50, x52) 
     nv153
})}
val x58 = x53.map{ case (x54, x55) => { 
  val x56 = x55.c_orders
  val x57 = RecM_ctx2(x56)
  x57 }}
val x59 = x58.distinct
val M_ctx2 = x59
val x60 = M_ctx2
val x62 = M_ctx2
val x63 = O__D.flat
val x65 = x63
val x74 = { val hm155 = x62.groupBy{ case x66 => {
  val x68 = x66.lbl
  val x69 = x68.O__F
  val x70 = x68.c__F
  val x71 = x70.c_custkey
  val x72 = (x69,x71)
  x72}}
x65.flatMap(x67 => hm155.get({val x73 = x67.o_custkey
x73}) match {
 case Some(a) => a.map(v => (v, x67))
 case _ => Nil
}) }
val x85 = { val grps156 = x74.groupBy{ case (x75, x76) => { val x77 = (x75)
x77 }}
 grps156.map(x84 => (x84._1, x84._2.map{case (x75, x76) => val x78 = x76.o_orderdate
val x79 = x75.lbl
val x80 = x79.P__F
val x81 = x79.L__F
val x82 = Record158(x76, x80, x81)
val x83 = Record159(x78, x82)
x83})).toList }
val x90 = x85.map{ case (x86, x87) => { 
  val x88 = x86.lbl
  val x89 = RecM_flat2(x88, x87)
  x89 }}
val M_flat2 = x90
val x91 = M_flat2
val x93 = M_flat2
val x97 = x93.flatMap{ case x94 => 
  val x95 = x94._2
  x95.map(x96 => {
  val nv160 = (x94, x96) 
     nv160
})}
val x102 = x97.map{ case (x98, x99) => { 
  val x100 = x99.o_parts
  val x101 = RecM_ctx3(x100)
  x101 }}
val x103 = x102.distinct
val M_ctx3 = x103
val x104 = M_ctx3
val x106 = M_ctx3
val x107 = L__D.flat
val x109 = x107
val x118 = { val hm162 = x106.groupBy{ case x110 => {
  val x112 = x110.lbl
  val x113 = x112.L__F
  val x114 = x112.o__F
  val x115 = x114.o_orderkey
  val x116 = (x113,x115)
  x116}}
x109.flatMap(x111 => hm162.get({val x117 = x111.l_orderkey
x117}) match {
 case Some(a) => a.map(v => (v, x111))
 case _ => Nil
}) }
val x119 = P__D.flat
val x121 = x119
val x130 = { val hm164 = x118.groupBy{ case (x122, x123) => {
  val x125 = x122.lbl
  val x126 = x125.P__F
  val x127 = x123.l_partkey
  val x128 = (x126,x127)
  x128}}
x121.flatMap(x124 => hm164.get({val x129 = x124.p_partkey
x129}) match {
 case Some(a) => a.map(v => (v, x124))
 case _ => Nil
}) }
val x139 = { val grps165 = x130.groupBy{ case ((x131, x132), x133) => { val x134 = (x131)
x134 }}
 grps165.map(x138 => (x138._1, x138._2.map{case ((x131, x132), x133) => val x135 = x133.p_name
val x136 = x132.l_quantity
val x137 = Record167(x135, x136)
x137})).toList }
val x144 = x139.map{ case (x140, x141) => { 
  val x142 = x140.lbl
  val x143 = RecM_flat3(x142, x141)
  x143 }}
val M_flat3 = x144
val x145 = M_flat3
val x146 = (x19,x47,x60,x91,x104,x145)
x146 
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
