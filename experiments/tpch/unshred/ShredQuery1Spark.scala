
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record159(lbl: Unit)
case class Record160(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record161(p_name: String, p_partkey: Int)
case class Record162(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record163(c__Fc_custkey: Int)
case class Record164(c_name: String, c_orders: Record163)
case class Record165(lbl: Record163)
case class Record166(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record168(o__Fo_orderkey: Int)
case class Record169(o_orderdate: String, o_parts: Record168)
case class Record170(lbl: Record168)
case class Record172(p_name: String, l_qty: Double)
case class Record231(o_orderdate: String, o_parts: Iterable[Record172])
case class Record232(c_name: String, c_orders: Iterable[Record231])
object ShredQuery1Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1Spark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L__F = 3
val L__D_1 = tpch.loadLineitem
L__D_1.cache
L__D_1.count
val P__F = 4
val P__D_1 = tpch.loadPart
P__D_1.cache
P__D_1.count
val C__F = 1
val C__D_1 = tpch.loadCustomers
C__D_1.cache
C__D_1.count
val O__F = 2
val O__D_1 = tpch.loadOrders
O__D_1.cache
O__D_1.count

    def f = {
 
var start0 = System.currentTimeMillis()
val x46 = () 
val x47 = Record159(x46) 
val x48 = List(x47) 
val ljp_ctx1 = x48
val x49 = ljp_ctx1
//ljp_ctx1.collect.foreach(println(_))
val x55 = L__D_1.map(x50 => { val x51 = x50.l_orderkey 
val x52 = x50.l_quantity 
val x53 = x50.l_partkey 
val x54 = Record160(x51, x52, x53) 
x54 }) 
val x60 = P__D_1.map(x56 => { val x57 = x56.p_name 
val x58 = x56.p_partkey 
val x59 = Record161(x57, x58) 
x59 }) 
val x65 = { val out1 = x55.map{ case x61 => ({val x63 = x61.l_partkey 
x63}, x61) }
  val out2 = x60.map{ case x62 => ({val x64 = x62.p_partkey 
x64}, x62) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x72 = x65.map{ case (x66, x67) => 
   val x68 = x66.l_orderkey 
val x69 = x67.p_name 
val x70 = x66.l_quantity 
val x71 = Record162(x68, x69, x70) 
x71 
} 
val ljp__D_1 = x72
val x73 = ljp__D_1
//ljp__D_1.collect.foreach(println(_))
val M_ctx1 = x48
val x74 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x80 = C__D_1.map{ case x75 => 
   val x76 = x75.c_name 
val x77 = x75.c_custkey 
val x78 = Record163(x77) 
val x79 = Record164(x76, x78) 
x79 
} 
val M__D_1 = x80
val x81 = M__D_1
//M__D_1.collect.foreach(println(_))
val x83 = M__D_1 
val x87 = x83.map{ case x84 => 
   val x85 = x84.c_orders 
val x86 = Record165(x85) 
x86 
} 
val x88 = x87.distinct 
val c_orders_ctx1 = x88
val x89 = c_orders_ctx1
//c_orders_ctx1.collect.foreach(println(_))
val x91 = c_orders_ctx1 
val x97 = O__D_1.map(x92 => { val x93 = x92.o_orderdate 
val x94 = x92.o_orderkey 
val x95 = x92.o_custkey 
val x96 = Record166(x93, x94, x95) 
x96 }) 
val x103 = { val out1 = x91.map{ case x98 => ({val x100 = x98.lbl 
val x101 = x100.c__Fc_custkey 
x101}, x98) }
  val out2 = x97.map{ case x99 => ({val x102 = x99.o_custkey 
x102}, x99) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x113 = x103.flatMap{ case (x104, x105) => val x112 = (x105) 
x112 match {
   case (null) => Nil 
   case x111 => List(({val x106 = (x104) 
x106}, {val x107 = x105.o_orderdate 
val x108 = x105.o_orderkey 
val x109 = Record168(x108) 
val x110 = Record169(x107, x109) 
x110}))
 }
}.groupByLabel() 
val x118 = x113.map{ case (x114, x115) => 
   val x116 = x114.lbl 
val x117 = (x116, x115) 
x117 
} 
val c_orders__D_1 = x118
val x119 = c_orders__D_1
//c_orders__D_1.collect.foreach(println(_))
val x121 = c_orders__D_1 
val x125 = x121.flatMap{ case x122 => x122 match {
   case null => List((x122, null))
   case _ =>
   val x123 = x122._2 
x123 match {
     case x124 => x124.map{ case v2 => (x122, v2) }
  }
 }} 
val x130 = x125.map{ case (x126, x127) => 
   val x128 = x127.o_parts 
val x129 = Record170(x128) 
x129 
} 
val x131 = x130.distinct 
val o_parts_ctx1 = x131
val x132 = o_parts_ctx1
//o_parts_ctx1.collect.foreach(println(_))
val x134 = o_parts_ctx1 
val x136 = ljp__D_1 
val x142 = { val out1 = x134.map{ case x137 => ({val x139 = x137.lbl 
val x140 = x139.o__Fo_orderkey 
x140}, x137) }
  val out2 = x136.map{ case x138 => ({val x141 = x138.l_orderkey 
x141}, x138) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x151 = x142.flatMap{ case (x143, x144) => val x150 = (x144) 
x150 match {
   case (null) => Nil 
   case x149 => List(({val x145 = (x143) 
x145}, {val x146 = x144.p_name 
val x147 = x144.l_qty 
val x148 = Record172(x146, x147) 
x148}))
 }
}.groupByLabel() 
val x156 = x151.map{ case (x152, x153) => 
   val x154 = x152.lbl 
val x155 = (x154, x153) 
x155 
} 
val o_parts__D_1 = x156
val x157 = o_parts__D_1
//o_parts__D_1.collect.foreach(println(_))
x157.count
var end0 = System.currentTimeMillis() - start0
println("ShredQuery1Spark,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    

var start1 = System.currentTimeMillis()
val x196 = M__D_1_1 
val x200 = { val out1 = x196.map{ case x197 => ({val x199 = x197.c_orders 
x199}, x197) }
  val out2 = c_orders__D_1_1.flatMapValues(identity)
  out1.lookup(out2)
} 
val x203 = o_parts__D_1_1.map(x201 => { val x202 = () 
x202 }) 
val x207 = x200.cartesian(x203) 
val x214 = x207.flatMap{ case ((x208, x209), x210) => val x213 = (x210) 
x213 match {
   case (null) => Nil 
   case x212 => List(({val x211 = (x208,x209) 
x211}, {x210}))
 }
}.groupByKey() 
val x223 = x214.flatMap{ case ((x215, x216), x217) => val x222 = (x216,x217) 
x222 match {
   case (_,null) => Nil 
   case x221 => List(({val x218 = (x215) 
x218}, {val x219 = x216.o_orderdate 
val x220 = Record231(x219, x217) 
x220}))
 }
}.groupByKey() 
val x228 = x223.map{ case (x224, x225) => 
   val x226 = x224.c_name 
val x227 = Record232(x226, x225) 
x227 
} 
x228.count
var end1 = System.currentTimeMillis() - start1
println("ShredQuery1Spark,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("ShredQuery1Spark"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
