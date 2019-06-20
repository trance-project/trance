
package experiments
/** Generated code **/
object ShredQuery1 {
 
import shredding.examples.tpch._
val C__F = 1
val C__D = (List((C__F, TPCHLoader.loadCustomer.toList)), ())
val O__F = 2
val O__D = (List((O__F, TPCHLoader.loadOrders.toList)), ())
val L__F = 3
val L__D = (List((L__F, TPCHLoader.loadLineitem.toList)), ())
val P__F = 4
val P__D = (List((P__F, TPCHLoader.loadPart.toList)), ())
 case class Record175(lbl: Q1Flat)
case class Record180(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record181(c_name: String, c_orders: Record180)
case class Record182(_1: Q1Flat, _2: List[Record181])
case class Record185(lbl: Record180)
case class Record190(o__F: Orders, P__F: Int, L__F: Int)
case class Record191(o_orderdate: String, o_parts: Record190)
case class Record192(_1: Record180, _2: List[Record191])
case class Record195(lbl: Record190)
case class Record203(p_name: String, l_qty: Double)
case class Record204(_1: Record190, _2: List[Record203])
 def main(args: Array[String]){ 
    var time = List[Long]()
    for (i <- 1 to 5) {
      var start = System.currentTimeMillis()
      f
      var end = System.currentTimeMillis() - start
      time = time :+ end
    }
    println(time)
    val avg = (time.sum/5)
    println(avg)
 }
 def f(){
  val x59 = Q1Flat(P__F, C__F, L__F, O__F)
val x60 = Record175(x59)
val x61 = List(x60)
val x62 = x61
val x85 = x62.flatMap(x63 =>  
  {  val x64 = x63.lbl
    val x65 = C__D._1
    val x82 = x65.flatMap(x66 =>  
      if({val x67 = x63.lbl
      val x68 = x67.C__F
      val x69 = x66._1
      val x70 = x68 == x69
      x70}) {  val x71 = x66._2
        val x81 = x71.flatMap(x72 =>  
          {  val x73 = x72.c_name
            val x74 = x63.lbl
            val x75 = x74.P__F
            val x76 = x74.L__F
            val x77 = x74.O__F
            val x78 = Record180(x75, x76, x77, x72)
            val x79 = Record181(x73, x78)
            val x80 = List(x79)
            x80})
        x81} else {  Nil})
    val x83 = Record182(x64, x82)
    val x84 = List(x83)
    x84})
val x86 = x85
val x94 = x86.flatMap(x87 =>  
  {  val x88 = x87._2
    val x93 = x88.flatMap(x89 =>  
      {  val x90 = x89.c_orders
        val x91 = Record185(x90)
        val x92 = List(x91)
        x92})
    x93})
val x95 = x94.distinct
val x96 = x95
val x123 = x96.flatMap(x97 =>  
  {  val x98 = x97.lbl
    val x99 = O__D._1
    val x120 = x99.flatMap(x100 =>  
      if({val x101 = x97.lbl
      val x102 = x101.O__F
      val x103 = x100._1
      val x104 = x102 == x103
      x104}) {  val x105 = x100._2
        val x119 = x105.flatMap(x106 =>  
          if({val x107 = x106.o_custkey
          val x108 = x97.lbl
          val x109 = x108.c__F
          val x110 = x109.c_custkey
          val x111 = x107 == x110
          x111}) {  val x112 = x106.o_orderdate
            val x113 = x97.lbl
            val x114 = x113.P__F
            val x115 = x113.L__F
            val x116 = Record190(x106, x114, x115)
            val x117 = Record191(x112, x116)
            val x118 = List(x117)
            x118} else {  Nil})
        x119} else {  Nil})
    val x121 = Record192(x98, x120)
    val x122 = List(x121)
    x122})
val x124 = x123
val x132 = x124.flatMap(x125 =>  
  {  val x126 = x125._2
    val x131 = x126.flatMap(x127 =>  
      {  val x128 = x127.o_parts
        val x129 = Record195(x128)
        val x130 = List(x129)
        x130})
    x131})
val x133 = x132.distinct
val x134 = x133
val x172 = x134.flatMap(x135 =>  
  {  val x136 = x135.lbl
    val x137 = L__D._1
    val x169 = x137.flatMap(x138 =>  
      if({val x139 = x135.lbl
      val x140 = x139.L__F
      val x141 = x138._1
      val x142 = x140 == x141
      x142}) {  val x143 = x138._2
        val x168 = x143.flatMap(x144 =>  
          {  val x145 = P__D._1
            val x167 = x145.flatMap(x146 =>  
              if({val x147 = x135.lbl
              val x148 = x147.P__F
              val x149 = x146._1
              val x150 = x148 == x149
              x150}) {  val x151 = x146._2
                val x166 = x151.flatMap(x152 =>  
                  if({val x153 = x144.l_orderkey
                  val x154 = x135.lbl
                  val x155 = x154.o__F
                  val x156 = x155.o_orderkey
                  val x157 = x153 == x156
                  val x158 = x144.l_partkey
                  val x159 = x152.p_partkey
                  val x160 = x158 == x159
                  val x161 = x157 && x160
                  x161}) {  val x162 = x152.p_name
                    val x163 = x144.l_quantity
                    val x164 = Record203(x162, x163)
                    val x165 = List(x164)
                    x165} else {  Nil})
                x166} else {  Nil})
            x167})
        x168} else {  Nil})
    val x170 = Record204(x136, x169)
    val x171 = List(x170)
    x171})
val x173 = x172
val x174 = (x62,x86,x96,x124,x134,x173)
x174
 }
}
