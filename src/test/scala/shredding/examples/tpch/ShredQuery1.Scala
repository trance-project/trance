
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
 case class Record173(lbl: Q1Flat)
case class Record178(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record179(c_name: String, c_orders: Record178)
case class Record180(_1: Q1Flat, _2: List[Record179])
case class Record183(lbl: Record178)
case class Record188(o__F: Orders, P__F: Int, L__F: Int)
case class Record189(o_orderdate: String, o_parts: Record188)
case class Record190(_1: Record178, _2: List[Record189])
case class Record193(lbl: Record188)
case class Record201(p_name: String, l_qty: Double)
case class Record202(_1: Record188, _2: List[Record201])
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
  val x57 = Q1Flat(P__F, C__F, L__F, O__F)
val x58 = Record173(x57)
val x59 = List(x58)
val x60 = x59
val x83 = x60.flatMap(acc174 =>  
  {  val x62 = x61.lbl
    val x63 = C__D._1
    val x80 = x63.flatMap(acc176 =>  
      if({val x65 = x61.lbl
      val x66 = x65.C__F
      val x67 = x64._1
      val x68 = x66 == x67
      x68}) {  val x69 = x64._2
        val x79 = x69.flatMap(acc177 =>  
          {  val x71 = x70.c_name
            val x72 = x61.lbl
            val x73 = x72.P__F
            val x74 = x72.L__F
            val x75 = x72.O__F
            val x76 = Record178(x73, x74, x75, x70)
            val x77 = Record179(x71, x76)
            val x78 = List(x77)
            x78})
        x79} else {  Nil})
    val x81 = Record180(x62, x80)
    val x82 = List(x81)
    x82})
val x84 = x83
val x92 = x84.flatMap(acc181 =>  
  {  val x86 = x85._2
    val x91 = x86.flatMap(acc182 =>  
      {  val x88 = x87.c_orders
        val x89 = Record183(x88)
        val x90 = List(x89)
        x90})
    x91})
val x93 = x92.distinct
val x94 = x93
val x121 = x94.flatMap(acc184 =>  
  {  val x96 = x95.lbl
    val x97 = O__D._1
    val x118 = x97.flatMap(acc186 =>  
      if({val x99 = x95.lbl
      val x100 = x99.O__F
      val x101 = x98._1
      val x102 = x100 == x101
      x102}) {  val x103 = x98._2
        val x117 = x103.flatMap(acc187 =>  
          if({val x105 = x104.o_custkey
          val x106 = x95.lbl
          val x107 = x106.c__F
          val x108 = x107.c_custkey
          val x109 = x105 == x108
          x109}) {  val x110 = x104.o_orderdate
            val x111 = x95.lbl
            val x112 = x111.P__F
            val x113 = x111.L__F
            val x114 = Record188(x104, x112, x113)
            val x115 = Record189(x110, x114)
            val x116 = List(x115)
            x116} else {  Nil})
        x117} else {  Nil})
    val x119 = Record190(x96, x118)
    val x120 = List(x119)
    x120})
val x122 = x121
val x130 = x122.flatMap(acc191 =>  
  {  val x124 = x123._2
    val x129 = x124.flatMap(acc192 =>  
      {  val x126 = x125.o_parts
        val x127 = Record193(x126)
        val x128 = List(x127)
        x128})
    x129})
val x131 = x130.distinct
val x132 = x131
val x170 = x132.flatMap(acc194 =>  
  {  val x134 = x133.lbl
    val x135 = L__D._1
    val x167 = x135.flatMap(acc196 =>  
      if({val x137 = x133.lbl
      val x138 = x137.L__F
      val x139 = x136._1
      val x140 = x138 == x139
      x140}) {  val x141 = x136._2
        val x166 = x141.flatMap(acc197 =>  
          {  val x143 = P__D._1
            val x165 = x143.flatMap(acc199 =>  
              if({val x145 = x133.lbl
              val x146 = x145.P__F
              val x147 = x144._1
              val x148 = x146 == x147
              x148}) {  val x149 = x144._2
                val x164 = x149.flatMap(acc200 =>  
                  if({val x151 = x142.l_orderkey
                  val x152 = x133.lbl
                  val x153 = x152.o__F
                  val x154 = x153.o_orderkey
                  val x155 = x151 == x154
                  val x156 = x142.l_partkey
                  val x157 = x150.p_partkey
                  val x158 = x156 == x157
                  val x159 = x155 && x158
                  x159}) {  val x160 = x150.p_name
                    val x161 = x142.l_quantity
                    val x162 = Record201(x160, x161)
                    val x163 = List(x162)
                    x163} else {  Nil})
                x164} else {  Nil})
            x165})
        x166} else {  Nil})
    val x168 = Record202(x134, x167)
    val x169 = List(x168)
    x169})
val x171 = x170
val x172 = (x60,x84,x94,x122,x132,x171)
x172
 }
}
