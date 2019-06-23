
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
  val x58 = Q1Flat(P__F, C__F, L__F, O__F)
val x59 = Record173(x58)
val x60 = List(x59)
val x61 = x60
val x84 = x61.flatMap(x62 =>  
  {  val x63 = x62.lbl
    val x64 = C__D._1
    val x81 = x64.flatMap(x65 =>  
      if({val x66 = x62.lbl
      val x67 = x66.C__F
      val x68 = x65._1
      val x69 = x67 == x68
      x69}) {  val x70 = x65._2
        val x80 = x70.flatMap(x71 =>  
          {  val x72 = x71.c_name
            val x73 = x62.lbl
            val x74 = x73.P__F
            val x75 = x73.L__F
            val x76 = x73.O__F
            val x77 = Record178(x74, x75, x76, x71)
            val x78 = Record179(x72, x77)
            val x79 = List(x78)
            x79})
        x80} else {  Nil})
    val x82 = Record180(x63, x81)
    val x83 = List(x82)
    x83})
val x85 = x84
val x93 = x85.flatMap(x86 =>  
  {  val x87 = x86._2
    val x92 = x87.flatMap(x88 =>  
      {  val x89 = x88.c_orders
        val x90 = Record183(x89)
        val x91 = List(x90)
        x91})
    x92})
val x94 = x93.distinct
val x95 = x94
val x122 = x95.flatMap(x96 =>  
  {  val x97 = x96.lbl
    val x98 = O__D._1
    val x119 = x98.flatMap(x99 =>  
      if({val x100 = x96.lbl
      val x101 = x100.O__F
      val x102 = x99._1
      val x103 = x101 == x102
      x103}) {  val x104 = x99._2
        val x118 = x104.flatMap(x105 =>  
          if({val x106 = x105.o_custkey
          val x107 = x96.lbl
          val x108 = x107.c__F
          val x109 = x108.c_custkey
          val x110 = x106 == x109
          x110}) {  val x111 = x105.o_orderdate
            val x112 = x96.lbl
            val x113 = x112.P__F
            val x114 = x112.L__F
            val x115 = Record188(x105, x113, x114)
            val x116 = Record189(x111, x115)
            val x117 = List(x116)
            x117} else {  Nil})
        x118} else {  Nil})
    val x120 = Record190(x97, x119)
    val x121 = List(x120)
    x121})
val x123 = x122
val x131 = x123.flatMap(x124 =>  
  {  val x125 = x124._2
    val x130 = x125.flatMap(x126 =>  
      {  val x127 = x126.o_parts
        val x128 = Record193(x127)
        val x129 = List(x128)
        x129})
    x130})
val x132 = x131.distinct
val x133 = x132
val x170 = x133.flatMap(x134 =>  
  {  val x135 = x134.lbl
    val x136 = L__D._1
    val x167 = x136.flatMap(x137 =>  
      if({val x138 = x134.lbl
      val x139 = x138.L__F
      val x140 = x137._1
      val x141 = x139 == x140
      x141}) {  val x142 = x137._2
        val x166 = x142.flatMap(x143 =>  
          if({val x144 = x143.l_orderkey
          val x145 = x134.lbl
          val x146 = x145.o__F
          val x147 = x146.o_orderkey
          val x148 = x144 == x147
          x148}) {  val x149 = P__D._1
            val x165 = x149.flatMap(x150 =>  
              if({val x151 = x134.lbl
              val x152 = x151.P__F
              val x153 = x150._1
              val x154 = x152 == x153
              x154}) {  val x155 = x150._2
                val x164 = x155.flatMap(x156 =>  
                  if({val x157 = x143.l_partkey
                  val x158 = x156.p_partkey
                  val x159 = x157 == x158
                  x159}) {  val x160 = x156.p_name
                    val x161 = x143.l_quantity
                    val x162 = Record201(x160, x161)
                    val x163 = List(x162)
                    x163} else {  Nil})
                x164} else {  Nil})
            x165} else {  Nil})
        x166} else {  Nil})
    val x168 = Record202(x135, x167)
    val x169 = List(x168)
    x169})
val x171 = x170
val x172 = (x61,x85,x95,x123,x133,x171)
x172
 }
}
