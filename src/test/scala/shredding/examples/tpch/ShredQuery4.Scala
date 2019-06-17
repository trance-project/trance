
package experiments
/** Generated code **/
object ShredQuery4 {
 
import shredding.examples.tpch._
val C = TPCHLoader.loadCustomer.toList
val O = TPCHLoader.loadOrders.toList
val L = TPCHLoader.loadLineitem.toList
val P = TPCHLoader.loadPart.toList
 case class Record169(P__F: Int, C__F: Int, L__F: Int, O__F: Int)
case class Record170(lbl: Record169)
case class Record172(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record173(c_name: String, c_orders: Record172)
case class Record174(k: Record169, v: List[Record173])
case class Record175(lbl: Record172)
case class Record177(o__F: Orders, P__F: Int, L__F: Int)
case class Record178(o_orderdate: String, o_parts: Record177)
case class Record179(k: Record172, v: List[Record178])
case class Record180(lbl: Record177)
case class Record183(p_name: String, l_qty: Double)
case class Record184(k: Record177, v: List[Record183])
 def main(args: Array[String]){ 
    var time = List[Long]()
    for (i <- 1 to 5) {
      var start = System.currentTimeMillis()
      println(f)
      var end = System.currentTimeMillis() - start
      time = time :+ end
    }
    val avg = (time.sum/5)
    println(avg)
 }
 
 def f(){
   val x53 = Record169(P__F, C__F, L__F, O__F)
val x54 = Record170(x53)
val x55 = List(x54)
val x56 = x55
val x79 = x56.flatMap({x57 => 
   val x58 = x57.lbl
  val x59 = C__D._1
  val x76 = x59.withFilter({x60 => val x61 = x57.lbl
  val x62 = x61.C__F
  val x63 = x60._1
  val x64 = x62 == x63
  x64}).flatMap({x60 => 
     val x65 = x60._2
    val x75 = x65.flatMap({x66 => 
       val x67 = x66.c_name
      val x68 = x57.lbl
      val x69 = x68.P__F
      val x70 = x68.L__F
      val x71 = x68.O__F
      val x72 = Record172(x69, x70, x71, x66)
      val x73 = Record173(x67, x72)
      val x74 = List(x73)
      x74})
    x75})
  val x77 = Record174(x58, x76)
  val x78 = List(x77)
  x78})
val x80 = x79
val x88 = x80.flatMap({x81 => 
   val x82 = x81.v
  val x87 = x82.flatMap({x83 => 
     val x84 = x83.c_orders
    val x85 = Record175(x84)
    val x86 = List(x85)
    x86})
  x87})
val x89 = x88.distinct
val x90 = x89
val x117 = x90.flatMap({x91 => 
   val x92 = x91.lbl
  val x93 = O__D._1
  val x114 = x93.withFilter({x94 => val x95 = x91.lbl
  val x96 = x95.O__F
  val x97 = x94._1
  val x98 = x96 == x97
  x98}).flatMap({x94 => 
     val x99 = x94._2
    val x113 = x99.withFilter({x100 => val x101 = x100.o_custkey
    val x102 = x91.lbl
    val x103 = x102.c__F
    val x104 = x103.c_custkey
    val x105 = x101 == x104
    x105}).flatMap({x100 => 
       val x106 = x100.o_orderdate
      val x107 = x91.lbl
      val x108 = x107.P__F
      val x109 = x107.L__F
      val x110 = Record177(x100, x108, x109)
      val x111 = Record178(x106, x110)
      val x112 = List(x111)
      x112})
    x113})
  val x115 = Record179(x92, x114)
  val x116 = List(x115)
  x116})
val x118 = x117
val x126 = x118.flatMap({x119 => 
   val x120 = x119.v
  val x125 = x120.flatMap({x121 => 
     val x122 = x121.o_parts
    val x123 = Record180(x122)
    val x124 = List(x123)
    x124})
  x125})
val x127 = x126.distinct
val x128 = x127
val x166 = x128.flatMap({x129 => 
   val x130 = x129.lbl
  val x131 = L__D._1
  val x163 = x131.withFilter({x132 => val x133 = x129.lbl
  val x134 = x133.L__F
  val x135 = x132._1
  val x136 = x134 == x135
  x136}).flatMap({x132 => 
     val x137 = x132._2
    val x162 = x137.flatMap({x138 => 
       val x139 = P__D._1
      val x161 = x139.withFilter({x140 => val x141 = x129.lbl
      val x142 = x141.P__F
      val x143 = x140._1
      val x144 = x142 == x143
      x144}).flatMap({x140 => 
         val x145 = x140._2
        val x160 = x145.withFilter({x146 => val x147 = x138.l_orderkey
        val x148 = x129.lbl
        val x149 = x148.o__F
        val x150 = x149.o_orderkey
        val x151 = x147 == x150
        val x152 = x138.l_partkey
        val x153 = x146.p_partkey
        val x154 = x152 == x153
        val x155 = x151 && x154
        x155}).flatMap({x146 => 
           val x156 = x146.p_name
          val x157 = x138.l_quantity
          val x158 = Record183(x156, x157)
          val x159 = List(x158)
          x159})
        x160})
      x161})
    x162})
  val x164 = Record184(x130, x163)
  val x165 = List(x164)
  x165})
val x167 = x166
val x168 = x56
x80
x90
x118
x128
x167
x168
 }
}
