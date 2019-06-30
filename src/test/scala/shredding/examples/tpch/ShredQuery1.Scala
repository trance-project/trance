
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record159(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record160(c_name: String, c_orders: Record159)
case class Record167(o__F: Orders, P__F: Int, L__F: Int)
case class Record168(o_orderdate: String, o_parts: Record167)
case class Record178(p_name: String, l_qty: Double)
case class RecM_flat2(_1: Record159, _2: List[Record168])
case class RecM_flat3(_1: Record167, _2: List[Record178])
case class RecM_flat1(_1: Q1Flat, _2: List[Record160])
case class RecM_ctx3(lbl: Record167)
case class RecM_ctx2(lbl: Record159)
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
val x22 = C__D._1
val x24 = x22
val x30 = { val hm155 = x21.groupBy{ case x25 => {
  val x27 = x25.lbl
  val x28 = x27.C__F
  x28}}
x24.flatMap(x26 => hm155.get({val x29 = C__D._1
x29}) match {
 case Some(a) => a.flatMap(a1 => x26._2.map(v => (a1, v))) // distinct labels
 case _ => Nil
}) }
val x42 = { val grps157 = x30.groupBy{ case (x31, x32) => { val x33 = (x31)
x33 }}
 grps157.map(x41 => (x41._1, x41._2.map{case (x31, x32) => val x34 = x32.c_name
val x35 = x31.lbl
val x36 = x35.P__F
val x37 = x35.L__F
val x38 = x35.O__F
val x39 = Record159(x36, x37, x38, x32)
val x40 = Record160(x34, x39)
x40})).toList }
val x47 = x42.map{ case (x43, x44) => { 
  val x45 = x43.lbl
  val x46 = RecM_flat1(x45, x44)
  x46 }}
val M_flat1 = x47
val x48 = M_flat1
val x50 = M_flat1
val x54 = x50.flatMap{ case x51 => 
  val x52 = x51._2
  x52.map(x53 => {
  val nv161 = (x51, x53) 
     nv161
})}
val x59 = x54.map{ case (x55, x56) => { 
  val x57 = x56.c_orders
  val x58 = RecM_ctx2(x57)
  x58 }}
val x60 = x59.distinct
val M_ctx2 = x60
val x61 = M_ctx2
val x63 = M_ctx2
val x64 = O__D._1
val x66 = x64
val x77 = { val hm163 = x63.groupBy{ case x67 => {
  val x69 = x67.lbl
  val x70 = x69.O__F
  val x71 = x69.c__F
  val x72 = x71.c_custkey
  val x73 = (x70,x72)
  x73}}
x66.flatMap(x68 => hm163.get({val x74 = O__D._1
val x75 = x68.o_custkey
val x76 = (x74,x75)
x76}) match {
 case Some(a) => a.flatMap(a1 => x68._2.map(v => (a1, v))) // distinct labels
 case _ => Nil
}) }
val x88 = { val grps165 = x77.groupBy{ case (x78, x79) => { val x80 = (x78)
x80 }}
 grps165.map(x87 => (x87._1, x87._2.map{case (x78, x79) => val x81 = x79.o_orderdate
val x82 = x78.lbl
val x83 = x82.P__F
val x84 = x82.L__F
val x85 = Record167(x79, x83, x84)
val x86 = Record168(x81, x85)
x86})).toList }
val x93 = x88.map{ case (x89, x90) => { 
  val x91 = x89.lbl
  val x92 = RecM_flat2(x91, x90)
  x92 }}
val M_flat2 = x93
val x94 = M_flat2
val x96 = M_flat2
val x100 = x96.flatMap{ case x97 => 
  val x98 = x97._2
  x98.map(x99 => {
  val nv169 = (x97, x99) 
     nv169
})}
val x105 = x100.map{ case (x101, x102) => { 
  val x103 = x102.o_parts
  val x104 = RecM_ctx3(x103)
  x104 }}
val x106 = x105.distinct
val M_ctx3 = x106
val x107 = M_ctx3
val x109 = M_ctx3
val x110 = L__D._1
val x112 = x110
val x123 = { val hm171 = x109.groupBy{ case x113 => {
  val x115 = x113.lbl
  val x116 = x115.L__F
  val x117 = x115.o__F
  val x118 = x117.o_orderkey
  val x119 = (x116,x118)
  x119}}
x112.flatMap(x114 => hm171.get({val x120 = L__D._1
val x121 = x114.l_orderkey
val x122 = (x120,x121)
x122}) match {
 case Some(a) => a.flatMap(a1 => x114._2.map(v => (a1, v))) // distinct labels
 case _ => Nil
}) }
val x124 = P__D._1
val x126 = x124
val x137 = { val hm174 = x123.groupBy{ case (x127, x128) => {
  val x130 = x127.lbl
  val x131 = x130.P__F
  val x132 = x128.l_partkey
  val x133 = (x131,x132)
  x133}}
x126.flatMap(x129 => hm174.get({val x134 = P__D._1
val x135 = x129.p_partkey
val x136 = (x134,x135)
x136}) match {
 case Some(a) => a.flatMap(a1 => x129._2.map(v => (a1, v))) // distinct labels
 case _ => Nil
}) }
val x146 = { val grps176 = x137.groupBy{ case ((x138, x139), x140) => { val x141 = (x138)
x141 }}
 grps176.map(x145 => (x145._1, x145._2.map{case ((x138, x139), x140) => val x142 = x140.p_name
val x143 = x139.l_quantity
val x144 = Record178(x142, x143)
x144})).toList }
val x151 = x146.map{ case (x147, x148) => { 
  val x149 = x147.lbl
  val x150 = RecM_flat3(x149, x148)
  x150 }}
val M_flat3 = x151
val x152 = M_flat3
val x153 = (x19,x48,x61,x94,x107,x152)
x153 
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
