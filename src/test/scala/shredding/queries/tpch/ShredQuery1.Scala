
package experiments
/** Generated code **/
object ShredQuery1 {
 
import shredding.queries.tpch._
val C__F = 1
val C__D = (List((C__F, TPCHLoader.loadCustomer.toList)), ())
val O__F = 2
val O__D = (List((O__F, TPCHLoader.loadOrders.toList)), ())
val L__F = 3
val L__D = (List((L__F, TPCHLoader.loadLineitem.toList)), ())
val P__F = 4
val P__D = (List((P__F, TPCHLoader.loadPart.toList)), ())
 case class Record135(P__F: Int, C__F: Int, L__F: Int, O__F: Int)
case class Record136(lbl: Record135)
case class Record138(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record139(c_name: String, c_orders: Record138)
case class Record140(k: Record135, v: List[Record139])
case class Record141(lbl: Record138)
case class Record143(o__F: Orders, P__F: Int, L__F: Int)
case class Record144(o_orderdate: String, o_parts: Record143)
case class Record145(k: Record138, v: List[Record144])
case class Record146(lbl: Record143)
case class Record149(p_name: String, l_qty: Double)
case class Record150(k: Record143, v: List[Record149])
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
   val x19 = Record135(P__F, C__F, L__F, O__F)
val x20 = Record136(x19)
val x21 = List(x20)
val x22 = x21
val x45 = x22.flatMap({x23 => 
   val x24 = x23.lbl
  val x25 = C__D._1
  val x42 = x25.withFilter({x26 => val x27 = x23.lbl
  val x28 = x27.C__F
  val x29 = x26._1
  val x30 = x28 == x29
  x30}).flatMap({x26 => 
     val x31 = x26._2
    val x41 = x31.flatMap({x32 => 
       val x33 = x32.c_name
      val x34 = x23.lbl
      val x35 = x34.P__F
      val x36 = x34.L__F
      val x37 = x34.O__F
      val x38 = Record138(x35, x36, x37, x32)
      val x39 = Record139(x33, x38)
      val x40 = List(x39)
      x40})
    x41})
  val x43 = Record140(x24, x42)
  val x44 = List(x43)
  x44})
val x46 = x45
val x54 = x46.flatMap({x47 => 
   val x48 = x47.v
  val x53 = x48.flatMap({x49 => 
     val x50 = x49.c_orders
    val x51 = Record141(x50)
    val x52 = List(x51)
    x52})
  x53})
val x55 = x54.distinct
val x56 = x55
val x83 = x56.flatMap({x57 => 
   val x58 = x57.lbl
  val x59 = O__D._1
  val x80 = x59.withFilter({x60 => val x61 = x57.lbl
  val x62 = x61.O__F
  val x63 = x60._1
  val x64 = x62 == x63
  x64}).flatMap({x60 => 
     val x65 = x60._2
    val x79 = x65.withFilter({x66 => val x67 = x66.o_custkey
    val x68 = x57.lbl
    val x69 = x68.c__F
    val x70 = x69.c_custkey
    val x71 = x67 == x70
    x71}).flatMap({x66 => 
       val x72 = x66.o_orderdate
      val x73 = x57.lbl
      val x74 = x73.P__F
      val x75 = x73.L__F
      val x76 = Record143(x66, x74, x75)
      val x77 = Record144(x72, x76)
      val x78 = List(x77)
      x78})
    x79})
  val x81 = Record145(x58, x80)
  val x82 = List(x81)
  x82})
val x84 = x83
val x92 = x84.flatMap({x85 => 
   val x86 = x85.v
  val x91 = x86.flatMap({x87 => 
     val x88 = x87.o_parts
    val x89 = Record146(x88)
    val x90 = List(x89)
    x90})
  x91})
val x93 = x92.distinct
val x94 = x93
val x132 = x94.flatMap({x95 => 
   val x96 = x95.lbl
  val x97 = L__D._1
  val x129 = x97.withFilter({x98 => val x99 = x95.lbl
  val x100 = x99.L__F
  val x101 = x98._1
  val x102 = x100 == x101
  x102}).flatMap({x98 => 
     val x103 = x98._2
    val x128 = x103.flatMap({x104 => 
       val x105 = P__D._1
      val x127 = x105.withFilter({x106 => val x107 = x95.lbl
      val x108 = x107.P__F
      val x109 = x106._1
      val x110 = x108 == x109
      x110}).flatMap({x106 => 
         val x111 = x106._2
        val x126 = x111.withFilter({x112 => val x113 = x104.l_orderkey
        val x114 = x95.lbl
        val x115 = x114.o__F
        val x116 = x115.o_orderkey
        val x117 = x113 == x116
        val x118 = x104.l_partkey
        val x119 = x112.p_partkey
        val x120 = x118 == x119
        val x121 = x117 && x120
        x121}).flatMap({x112 => 
           val x122 = x112.p_name
          val x123 = x104.l_quantity
          val x124 = Record149(x122, x123)
          val x125 = List(x124)
          x125})
        x126})
      x127})
    x128})
  val x130 = Record150(x96, x129)
  val x131 = List(x130)
  x131})
val x133 = x132
val x134 = x22
x46
x56
x84
x94
x133
x134
 }
}
