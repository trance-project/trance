
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record230(lbl: Unit)
case class Record231(c_name: String, c_custkey: Int)
case class Record233(c__Fc_custkey: Int)
case class Record234(c_name: String, c_orders: Record233)
case class Record235(_1: Unit, _2: Iterable[Record234])
case class Record236(lbl: Record233)
case class Record237(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record239(o__Fo_orderkey: Int)
case class Record240(o_orderdate: String, o_parts: Record239)
case class Record241(_1: Record233, _2: Iterable[Record240])
case class Record242(lbl: Record239)
case class Record243(l_quantity: Double, l_partkey: Int, l_orderkey: Int)
case class Record244(p_name: String, p_partkey: Int)
case class Record246(p_name: String, l_qty: Double)
case class Record247(_1: Record239, _2: Iterable[Record246])
object ShredQuery1Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1Spark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   
val tpch = TPCHLoader(spark)
val C__F = 1
val C__D_1 = tpch.loadCustomers()
C__D_1.cache
C__D_1.count
val O__F = 2
val O__D_1 = tpch.loadOrders()
O__D_1.cache
O__D_1.count
val L__F = 3
val L__D_1 = tpch.loadLineitem()
L__D_1.cache
L__D_1.count
val P__F = 4
val P__D_1 = tpch.loadPart()
P__D_1.cache
P__D_1.count
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
   var start0 = System.currentTimeMillis()
   val x98 = () 
val x99 = Record230(x98) 
val x100 = List(x99) 
val M_ctx1 = x100
val x103 = M_ctx1 
val x104 = C__D_1 
val x109 = x104.map(x105 => { val x106 = x105.c_name 
val x107 = x105.c_custkey 
val x108 = Record231(x106, x107) 
x108 }) 
val x112 = x109.map{ case c => (x103.head, c) } 
val x122 = x112.map{ case (x113, x114) => val x121 = (x114) 
x121 match {
   case (null) => ({val x115 = (x113) 
x115}, null) 
   case x120 => ({val x115 = (x113) 
x115}, {val x116 = x114.c_name 
val x117 = x114.c_custkey 
val x118 = Record233(x117) 
val x119 = Record234(x116, x118) 
x119})
 }
}.groupByLabel() 
val x127 = x122.map{ case (x123, x124) => 
   val x125 = x123.lbl 
val x126 = Record235(x125, x124) 
x126 
} 
val M_flat1 = x127
//println("M_flat1")
val x128 = M_flat1
//M_flat1.collect.foreach(println(_))
val x130 = M_flat1 
val x134 = x130.flatMap{ case x131 => x131 match {
   case null => List((x131, null))
   case _ =>
   val x132 = x131._2 
x132 match {
     case x133 => x133.map{ case v2 => (x131, v2) }
  }
 }} 
val x139 = x134.map{ case (x135, x136) => 
   val x137 = x136.c_orders 
val x138 = Record236(x137) 
x138 
} 
val x140 = x139.distinct 
val M_ctx2 = x140
//println("M_ctx2")
val x141 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x143 = M_ctx2 
val x144 = O__D_1 
val x150 = x144.map(x145 => { val x146 = x145.o_orderdate 
val x147 = x145.o_orderkey 
val x148 = x145.o_custkey 
val x149 = Record237(x146, x147, x148) 
x149 }) 
val x156 = { val out1 = x143.map{ case x151 => ({val x153 = x151.lbl 
val x154 = x153.c__Fc_custkey 
x154}, x151) }
  val out2 = x150.map{ case x152 => ({val x155 = x152.o_custkey 
x155}, x152) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x166 = x156.map{ case (x157, x158) => val x165 = (x158) 
x165 match {
   case (null) => ({val x159 = (x157) 
x159}, null) 
   case x164 => ({val x159 = (x157) 
x159}, {val x160 = x158.o_orderdate 
val x161 = x158.o_orderkey 
val x162 = Record239(x161) 
val x163 = Record240(x160, x162) 
x163})
 }
}.groupByLabel() 
val x171 = x166.map{ case (x167, x168) => 
   val x169 = x167.lbl 
val x170 = Record241(x169, x168) 
x170 
} 
val M_flat2 = x171
//println("M_flat2")
val x172 = M_flat2
//M_flat2.collect.foreach(println(_))
val x174 = M_flat2 
val x178 = x174.flatMap{ case x175 => x175 match {
   case null => List((x175, null))
   case _ =>
   val x176 = x175._2 
x176 match {
     case x177 => x177.map{ case v2 => (x175, v2) }
  }
 }} 
val x183 = x178.map{ case (x179, x180) => 
   val x181 = x180.o_parts 
val x182 = Record242(x181) 
x182 
} 
val x184 = x183.distinct 
val M_ctx3 = x184
//println("M_ctx3")
val x185 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x187 = M_ctx3 
val x188 = L__D_1 
val x194 = x188.map(x189 => { val x190 = x189.l_quantity 
val x191 = x189.l_partkey 
val x192 = x189.l_orderkey 
val x193 = Record243(x190, x191, x192) 
x193 }) 
val x200 = { val out1 = x187.map{ case x195 => ({val x197 = x195.lbl 
val x198 = x197.o__Fo_orderkey 
x198}, x195) }
  val out2 = x194.map{ case x196 => ({val x199 = x196.l_orderkey 
x199}, x196) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x201 = P__D_1 
val x206 = x201.map(x202 => { val x203 = x202.p_name 
val x204 = x202.p_partkey 
val x205 = Record244(x203, x204) 
x205 }) 
val x212 = { val out1 = x200.map{ case (x207, x208) => ({val x210 = x208.l_partkey 
x210}, (x207, x208)) }
  val out2 = x206.map{ case x209 => ({val x211 = x209.p_partkey 
x211}, x209) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x222 = x212.map{ case ((x213, x214), x215) => val x221 = (x214,x215) 
x221 match {
   case (_,null) => ({val x216 = (x213) 
x216}, null) 
   case x220 => ({val x216 = (x213) 
x216}, {val x217 = x215.p_name 
val x218 = x214.l_quantity 
val x219 = Record246(x217, x218) 
x219})
 }
}.groupByLabel() 
val x227 = x222.map{ case (x223, x224) => 
   val x225 = x223.lbl 
val x226 = Record247(x225, x224) 
x226 
} 
val M_flat3 = x227
//println("M_flat3")
val x228 = M_flat3
//M_flat3.collect.foreach(println(_))
val res = x228.count
   var end0 = System.currentTimeMillis() - start0
   println("ShredQuery1Spark"+sf+","+Config.datapath+","+end0)
 }
}
