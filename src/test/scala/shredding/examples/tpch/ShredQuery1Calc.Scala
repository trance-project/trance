
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
    case class Record174(P__F: Int, L__F: Int, O__F: Int, c__F: Customer, uniqueId: Long) extends CaseClassRecord
case class Record175(c_name: String, c_orders: Record174, uniqueId: Long) extends CaseClassRecord
case class Record182(o__F: Orders, P__F: Int, L__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record183(o_orderdate: String, o_parts: Record182, uniqueId: Long) extends CaseClassRecord
case class Record193(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class RecM_flat2(_1: Record174, _2: List[Record183], uniqueId: Long) extends CaseClassRecord
case class RecM_flat3(_1: Record182, _2: List[Record193], uniqueId: Long) extends CaseClassRecord
case class RecM_flat1(_1: Q1Flat, _2: List[Record175], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx3(lbl: Record182, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx2(lbl: Record174, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx1(lbl: Q1Flat, uniqueId: Long) extends CaseClassRecord
object ShredQuery1Calc {
 def main(args: Array[String]){
    var start0 = System.currentTimeMillis()
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
    
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
      val x55 = Q1Flat(P__F, C__F, L__F, O__F, newId) 
val x56 = RecM_ctx1(x55, newId) 
val x57 = List(x56) 
val M_ctx1 = x57
val x58 = M_ctx1
val x81 = M_ctx1.flatMap(x59 => { 
    val x60 = x59.lbl 
    val x61 = C__D._1 
    val x78 = x61.flatMap(x62 => { 
      if({val x63 = x59.lbl 
      val x64 = x63.C__F 
      val x65 = x62._1 
      val x66 = x64 == x65 
      x66    }) {  val x67 = x62._2 
        val x77 = x67.flatMap(x68 => { 
            val x69 = x68.c_name 
            val x70 = x59.lbl 
            val x71 = x70.P__F 
            val x72 = x70.L__F 
            val x73 = x70.O__F 
            val x74 = Record174(x71, x72, x73, x68, newId) 
            val x75 = Record175(x69, x74, newId) 
            val x76 = List(x75) 
            x76}) 
        x77} else {  Nil}}) 
    val x79 = RecM_flat1(x60, x78, newId) 
    val x80 = List(x79) 
    x80}) 
val M_flat1 = x81
val x82 = M_flat1
val x90 = M_flat1.flatMap(x83 => { 
    val x84 = x83._2 
    val x89 = x84.flatMap(x85 => { 
        val x86 = x85.c_orders 
        val x87 = RecM_ctx2(x86, newId) 
        val x88 = List(x87) 
        x88}) 
    x89}) 
val x91 = x90.distinct 
val M_ctx2 = x91
val x92 = M_ctx2
val x119 = M_ctx2.flatMap(x93 => { 
    val x94 = x93.lbl 
    val x95 = O__D._1 
    val x116 = x95.flatMap(x96 => { 
      if({val x97 = x93.lbl 
      val x98 = x97.O__F 
      val x99 = x96._1 
      val x100 = x98 == x99 
      x100    }) {  val x101 = x96._2 
        val x115 = x101.flatMap(x102 => { 
          if({val x103 = x102.o_custkey 
          val x104 = x93.lbl 
          val x105 = x104.c__F 
          val x106 = x105.c_custkey 
          val x107 = x103 == x106 
          x107     }) {  val x108 = x102.o_orderdate 
            val x109 = x93.lbl 
            val x110 = x109.P__F 
            val x111 = x109.L__F 
            val x112 = Record182(x102, x110, x111, newId) 
            val x113 = Record183(x108, x112, newId) 
            val x114 = List(x113) 
            x114} else {  Nil}}) 
        x115} else {  Nil}}) 
    val x117 = RecM_flat2(x94, x116, newId) 
    val x118 = List(x117) 
    x118}) 
val M_flat2 = x119
val x120 = M_flat2
val x128 = M_flat2.flatMap(x121 => { 
    val x122 = x121._2 
    val x127 = x122.flatMap(x123 => { 
        val x124 = x123.o_parts 
        val x125 = RecM_ctx3(x124, newId) 
        val x126 = List(x125) 
        x126}) 
    x127}) 
val x129 = x128.distinct 
val M_ctx3 = x129
val x130 = M_ctx3
val x167 = M_ctx3.flatMap(x131 => { 
    val x132 = x131.lbl 
    val x133 = L__D._1 
    val x164 = x133.flatMap(x134 => { 
      if({val x135 = x131.lbl 
      val x136 = x135.L__F 
      val x137 = x134._1 
      val x138 = x136 == x137 
      x138    }) {  val x139 = x134._2 
        val x163 = x139.flatMap(x140 => { 
          if({val x141 = x140.l_orderkey 
          val x142 = x131.lbl 
          val x143 = x142.o__F 
          val x144 = x143.o_orderkey 
          val x145 = x141 == x144 
          x145     }) {  val x146 = P__D._1 
            val x162 = x146.flatMap(x147 => { 
              if({val x148 = x131.lbl 
              val x149 = x148.P__F 
              val x150 = x147._1 
              val x151 = x149 == x150 
              x151    }) {  val x152 = x147._2 
                val x161 = x152.flatMap(x153 => { 
                  if({val x154 = x140.l_partkey 
                  val x155 = x153.p_partkey 
                  val x156 = x154 == x155 
                  x156   }) {  val x157 = x153.p_name 
                    val x158 = x140.l_quantity 
                    val x159 = Record193(x157, x158, newId) 
                    val x160 = List(x159) 
                    x160} else {  Nil}}) 
                x161} else {  Nil}}) 
            x162} else {  Nil}}) 
        x163} else {  Nil}}) 
    val x165 = RecM_flat3(x132, x164, newId) 
    val x166 = List(x165) 
    x166}) 
val M_flat3 = x167
val x168 = M_flat3
val x169 = (x58,x82,x92,x120,x130,x168) 
x169           
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
