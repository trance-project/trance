
package experiments
/** Generated code **/
object ShredQuery1 {
 
val C__F = 1
val C__D = (List((C__F, TPCHLoader.loadCustomer[Customer].toList)), ())
val O__F = 2
val O__D = (List((O__F, TPCHLoader.loadOrders[Orders].toList)), ())
val L__F = 3
val L__D = (List((L__F, TPCHLoader.loadLineitem[Lineitem].toList)), ())
val P__F = 4
val P__D = (List((P__F, TPCHLoader.loadPart[Part].toList)), ())
 case class Record167(lbl: Q1Flat)
case class Record169(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record170(c_name: String, c_orders: Record169)
case class Record171(k: Q1Flat, v: List[Record170])
case class Record172(lbl: Record169)
case class Record174(o__F: Orders, P__F: Int, L__F: Int)
case class Record175(o_orderdate: String, o_parts: Record174)
case class Record176(k: Record169, v: List[Record175])
case class Record177(lbl: Record174)
case class Record180(p_name: String, l_qty: Double)
case class Record181(k: Record174, v: List[Record180])
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
   val x51 = Q1Flat(P__F, C__F, L__F, O__F)
val x52 = Record167(x51)
val x53 = List(x52)
val x54 = x53
val x77 = x54.flatMap({x55 => 
   val x56 = x55.lbl
  val x57 = C__D._1
  val x74 = x57.withFilter({x58 => val x59 = x55.lbl
  val x60 = x59.C__F
  val x61 = x58._1
  val x62 = x60 == x61
  x62}).flatMap({x58 => 
     val x63 = x58._2
    val x73 = x63.flatMap({x64 => 
       val x65 = x64.c_name
      val x66 = x55.lbl
      val x67 = x66.P__F
      val x68 = x66.L__F
      val x69 = x66.O__F
      val x70 = Record169(x67, x68, x69, x64)
      val x71 = Record170(x65, x70)
      val x72 = List(x71)
      x72})
    x73})
  val x75 = Record171(x56, x74)
  val x76 = List(x75)
  x76})
val x78 = x77
val x86 = x78.flatMap({x79 => 
   val x80 = x79.v
  val x85 = x80.flatMap({x81 => 
     val x82 = x81.c_orders
    val x83 = Record172(x82)
    val x84 = List(x83)
    x84})
  x85})
val x87 = x86.distinct
val x88 = x87
val x115 = x88.flatMap({x89 => 
   val x90 = x89.lbl
  val x91 = O__D._1
  val x112 = x91.withFilter({x92 => val x93 = x89.lbl
  val x94 = x93.O__F
  val x95 = x92._1
  val x96 = x94 == x95
  x96}).flatMap({x92 => 
     val x97 = x92._2
    val x111 = x97.withFilter({x98 => val x99 = x98.o_custkey
    val x100 = x89.lbl
    val x101 = x100.c__F
    val x102 = x101.c_custkey
    val x103 = x99 == x102
    x103}).flatMap({x98 => 
       val x104 = x98.o_orderdate
      val x105 = x89.lbl
      val x106 = x105.P__F
      val x107 = x105.L__F
      val x108 = Record174(x98, x106, x107)
      val x109 = Record175(x104, x108)
      val x110 = List(x109)
      x110})
    x111})
  val x113 = Record176(x90, x112)
  val x114 = List(x113)
  x114})
val x116 = x115
val x124 = x116.flatMap({x117 => 
   val x118 = x117.v
  val x123 = x118.flatMap({x119 => 
     val x120 = x119.o_parts
    val x121 = Record177(x120)
    val x122 = List(x121)
    x122})
  x123})
val x125 = x124.distinct
val x126 = x125
val x164 = x126.flatMap({x127 => 
   val x128 = x127.lbl
  val x129 = L__D._1
  val x161 = x129.withFilter({x130 => val x131 = x127.lbl
  val x132 = x131.L__F
  val x133 = x130._1
  val x134 = x132 == x133
  x134}).flatMap({x130 => 
     val x135 = x130._2
    val x160 = x135.flatMap({x136 => 
       val x137 = P__D._1
      val x159 = x137.withFilter({x138 => val x139 = x127.lbl
      val x140 = x139.P__F
      val x141 = x138._1
      val x142 = x140 == x141
      x142}).flatMap({x138 => 
         val x143 = x138._2
        val x158 = x143.withFilter({x144 => val x145 = x136.l_orderkey
        val x146 = x127.lbl
        val x147 = x146.o__F
        val x148 = x147.o_orderkey
        val x149 = x145 == x148
        val x150 = x136.l_partkey
        val x151 = x144.p_partkey
        val x152 = x150 == x151
        val x153 = x149 && x152
        x153}).flatMap({x144 => 
           val x154 = x144.p_name
          val x155 = x136.l_quantity
          val x156 = Record180(x154, x155)
          val x157 = List(x156)
          x157})
        x158})
      x159})
    x160})
  val x162 = Record181(x128, x161)
  val x163 = List(x162)
  x163})
val x165 = x164
val x166 = x54
x78
x88
x116
x126
x165
x166
 }
}
