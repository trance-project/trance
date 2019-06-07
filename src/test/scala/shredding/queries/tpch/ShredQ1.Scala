
/** Generated code **/

import shredding.queries.tpch._
val CF = 1
val CD = (List((CF, TPCHLoader.loadCustomer.toList)), ())
val OF = 2
val OD = (List((OF, TPCHLoader.loadOrders.toList)), ())
val LF = 3
val LD = (List((LF, TPCHLoader.loadLineitem.toList)), ())
val PF = 4
val PD = (List((PF, TPCHLoader.loadPart.toList)), ())
val PD = ((PF, TPCHLoader.loadPart.toList), ())
case class Label122(OF: Int, CF: Int, LF: Int, PF: Int)
case class Record124(lbl: Label122)
case class Label126(cF: Customer, OF: Int, LF: Int, PF: Int)
case class Record128(c_name: String, c_orders: Label126)
case class Record130(k: Label122, v: List[Record128])
case class Record132(lbl: Label126)
case class Label134(oF: Orders, LF: Int, PF: Int)
case class Record136(o_orderdate: String, o_parts: Label134)
case class Record138(k: Label126, v: List[Record136])
case class Record140(lbl: Label134)
case class Record143(p_name: String, l_qty: Double)
case class Record145(k: Label134, v: List[Record143])
var start = System.currentTimeMillis()
val x19 = Label122(OF, CF, LF, PF)
val x20 = Record124(x19)
val x21 = List(x20)
val x22 = x21
val x41 = x22.flatMap({x23 => 
   val x24 = x23.lbl
  val x25 = x23.lbl
  val x26 = CD._1
  val x37 = x26.withFilter({x27 => val x28 = x27._1
  val x29 = CF == x28
  x29}).flatMap({x27 => 
     val x30 = x27._2
    val x36 = x30.flatMap({x31 => 
       val x32 = x31.c_name
      val x33 = Label126(x31, OF, LF, PF)
      val x34 = Record128(x32, x33)
      val x35 = List(x34)
      x35})
    x36})
  val x38 = Record130(x25, x37)
  val x39 = List(x38)
  val x40 = x39
  x40})
val x42 = x41
val x50 = x42.flatMap({x43 => 
   val x44 = x43.v
  val x49 = x44.flatMap({x45 => 
     val x46 = x45.c_orders
    val x47 = Record132(x46)
    val x48 = List(x47)
    x48})
  x49})
val x51 = x50.distinct
val x52 = x51
val x74 = x52.flatMap({x53 => 
   val x54 = x53.lbl
  val x55 = x53.lbl
  val x56 = OD._1
  val x70 = x56.withFilter({x57 => val x58 = x57._1
  val x59 = OF == x58
  x59}).flatMap({x57 => 
     val x60 = x57._2
    val x69 = x60.withFilter({x61 => val x62 = x61.o_custkey
    val x63 = cF.c_custkey
    val x64 = x62 == x63
    x64}).flatMap({x61 => 
       val x65 = x61.o_orderdate
      val x66 = Label134(x61, LF, PF)
      val x67 = Record136(x65, x66)
      val x68 = List(x67)
      x68})
    x69})
  val x71 = Record138(x55, x70)
  val x72 = List(x71)
  val x73 = x72
  x73})
val x75 = x74
val x83 = x75.flatMap({x76 => 
   val x77 = x76.v
  val x82 = x77.flatMap({x78 => 
     val x79 = x78.o_parts
    val x80 = Record140(x79)
    val x81 = List(x80)
    x81})
  x82})
val x84 = x83.distinct
val x85 = x84
val x119 = x85.flatMap({x86 => 
   val x87 = x86.lbl
  val x88 = x86.lbl
  val x89 = LD._1
  val x115 = x89.withFilter({x90 => val x91 = x90._1
  val x92 = LF == x91
  x92}).flatMap({x90 => 
     val x93 = x90._2
    val x114 = x93.flatMap({x94 => 
       val x95 = PD._1
      val x113 = x95.withFilter({x96 => val x97 = x96._1
      val x98 = PF == x97
      x98}).flatMap({x96 => 
         val x99 = x96._2
        val x112 = x99.withFilter({x100 => val x101 = x94.l_orderkey
        val x102 = oF.o_orderkey
        val x103 = x101 == x102
        val x104 = x94.l_partkey
        val x105 = x100.p_partkey
        val x106 = x104 == x105
        val x107 = x103 && x106
        x107}).flatMap({x100 => 
           val x108 = x100.p_name
          val x109 = x94.l_quantity
          val x110 = Record143(x108, x109)
          val x111 = List(x110)
          x111})
        x112})
      x113})
    x114})
  val x116 = Record145(x88, x115)
  val x117 = List(x116)
  val x118 = x117
  x118})
val x120 = x119
val x121 = x22
x42
x52
x75
x85
x120
x121
var end = System.currentTimeMillis() - start
