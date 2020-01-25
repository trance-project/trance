
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record165(lbl: Unit)
case class Record166(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record167(p_name: String, p_partkey: Int)
case class Record168(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record169(c_custkey: Int, c_name: String)
case class Record170(c__Fc_custkey: Int)
case class Record171(c_custkey: Int, c_name: String, c_orders: Record170)
case class Record172(lbl: Record170)
case class Record173(o_orderkey: Int, o_orderdate: String, o_custkey: Int)
case class Record175(o__Fo_orderkey: Int)
case class Record176(o_orderkey: Int, o_orderdate: String, o_parts: Record175)
case class Record177(lbl: Record175)
case class Record179(p_name: String, l_qty: Double)
case class Record328(c_name: String, c_orders: Record170)
case class Record329(c2__Fc_orders: Record170)
case class Record330(c_name: String, c_orders: Record329)
case class Record331(lbl: Record329)
case class Record333(o2__Fo_parts: Record175)
case class Record334(o_orderdate: String, o_parts: Record333)
case class Record335(lbl: Record333)
case class Record336(p_retailprice: Double, p_name: String)
case class Record338(p_name: String)
case class Record388(p_name: String, _2: Double)
case class Record389(o_orderdate: String, o_parts: Iterable[Record388])
case class Record390(c_name: String, c_orders: Iterable[Record389])
object ShredQuery4Filter2Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4Filter2Spark"+sf)
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

   val x47 = () 
val x48 = Record165(x47) 
val x49 = List(x48) 
val ljp_ctx1 = x49
val x50 = ljp_ctx1
//ljp_ctx1.collect.foreach(println(_))
val x56 = L__D_1.map(x51 => { val x52 = x51.l_orderkey 
val x53 = x51.l_quantity 
val x54 = x51.l_partkey 
val x55 = Record166(x52, x53, x54) 
x55 }) 
val x61 = P__D_1.map(x57 => { val x58 = x57.p_name 
val x59 = x57.p_partkey 
val x60 = Record167(x58, x59) 
x60 }) 
val x66 = { val out1 = x56.map{ case x62 => ({val x64 = x62.l_partkey 
x64}, x62) }
  val out2 = x61.map{ case x63 => ({val x65 = x63.p_partkey 
x65}, x63) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x73 = x66.map{ case (x67, x68) => 
   val x69 = x67.l_orderkey 
val x70 = x68.p_name 
val x71 = x67.l_quantity 
val x72 = Record168(x69, x70, x71) 
x72 
} 
val ljp__D_1 = x73
val x74 = ljp__D_1
//ljp__D_1.collect.foreach(println(_))
val M_ctx1 = x49
val x75 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x80 = C__D_1.map(x76 => { val x77 = x76.c_custkey 
val x78 = x76.c_name 
val x79 = Record169(x77, x78) 
x79 }) 
val x86 = x80.map{ case x81 => 
   val x82 = x81.c_custkey 
val x83 = x81.c_name 
val x84 = Record170(x82) 
val x85 = Record171(x82, x83, x84) 
x85 
} 
val M__D_1 = x86
val x87 = M__D_1
//M__D_1.collect.foreach(println(_))
val x89 = M__D_1 
val x93 = x89.map{ case x90 => 
   val x91 = x90.c_orders 
val x92 = Record172(x91) 
x92 
} 
val x94 = x93.distinct 
val M_ctx2 = x94
val x95 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x97 = M_ctx2 
val x103 = O__D_1.map(x98 => { val x99 = x98.o_orderkey 
val x100 = x98.o_orderdate 
val x101 = x98.o_custkey 
val x102 = Record173(x99, x100, x101) 
x102 }) 
val x109 = { val out1 = x97.map{ case x104 => ({val x106 = x104.lbl 
val x107 = x106.c__Fc_custkey 
x107}, x104) }
  val out2 = x103.map{ case x105 => ({val x108 = x105.o_custkey 
x108}, x105) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x119 = x109.flatMap{ case (x110, x111) => val x118 = (x111) 
x118 match {
   case (null) => Nil 
   case x117 => List(({val x112 = (x110) 
x112}, {val x113 = x111.o_orderkey 
val x114 = x111.o_orderdate 
val x115 = Record175(x113) 
val x116 = Record176(x113, x114, x115) 
x116}))
 }
}.groupByLabel() 
val x124 = x119.map{ case (x120, x121) => 
   val x122 = x120.lbl 
val x123 = (x122, x121) 
x123 
} 
val M__D_2 = x124
val x125 = M__D_2
//M__D_2.collect.foreach(println(_))
val x127 = M__D_2 
val x131 = x127.flatMap{ 
 case x128 => {val x129 = x128._2 
x129}.map{ case v2 => (x128._1, v2) }
}
         
val x136 = x131.map{ case (x132, x133) => 
   val x134 = x133.o_parts 
val x135 = Record177(x134) 
x135 
} 
val x137 = x136.distinct 
val M_ctx3 = x137
val x138 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x140 = M_ctx3 
val x142 = ljp__D_1 
val x148 = { val out1 = x140.map{ case x143 => ({val x145 = x143.lbl 
val x146 = x145.o__Fo_orderkey 
x146}, x143) }
  val out2 = x142.map{ case x144 => ({val x147 = x144.l_orderkey 
x147}, x144) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x157 = x148.flatMap{ case (x149, x150) => val x156 = (x150) 
x156 match {
   case (null) => Nil 
   case x155 => List(({val x151 = (x149) 
x151}, {val x152 = x150.p_name 
val x153 = x150.l_qty 
val x154 = Record179(x152, x153) 
x154}))
 }
}.groupByLabel() 
val x162 = x157.map{ case (x158, x159) => 
   val x160 = x158.lbl 
val x161 = (x160, x159) 
x161 
} 
val M__D_3 = x162
val x163 = M__D_3
//M__D_3.collect.foreach(println(_))
val Query1Extended__D_1 = M__D_1
Query1Extended__D_1.cache
Query1Extended__D_1.count
val Query1Extended__D_2c_orders_1 = M__D_2
Query1Extended__D_2c_orders_1.cache
Query1Extended__D_2c_orders_1.count
val Query1Extended__D_2c_orders_2o_parts_1 = M__D_3
Query1Extended__D_2c_orders_2o_parts_1.cache
Query1Extended__D_2c_orders_2o_parts_1.count
 def f = {
 
var start0 = System.currentTimeMillis()
val x225 = () 
val x226 = Record165(x225) 
val x227 = List(x226) 
val M_ctx1 = x227
val x228 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x234 = Query1Extended__D_1.filter(x229 => { val x230 = x229.c_custkey <= 1500000 
x230 }).map(x229 => { val x231 = x229.c_name 
val x232 = x229.c_orders 
val x233 = Record328(x231, x232) 
x233 }) 
val x240 = x234.map{ case x235 => 
   val x236 = x235.c_name 
val x237 = x235.c_orders 
val x238 = Record329(x237)
val x239 = Record330(x236, x238) 
x239 
} 
val M__D_1 = x240
val x241 = M__D_1
//M__D_1.collect.foreach(println(_))
val x243 = M__D_1 
val x247 = x243.map{ case x244 => 
   val x245 = x244.c_orders 
val x246 = Record331(x245) 
x246 
} 
val x248 = x247.distinct 
val M_ctx2 = x248
val x249 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x251 = M_ctx2 
val x253 = Query1Extended__D_2c_orders_1
val x258 = { val out1 = x251.map{ case x254 => ({val x256 = x254.lbl 
val x257 = x256.c2__Fc_orders 
x257}, x254) }
val out2 = x253.flatMap{
  case (lbl, bag) => bag.flatMap{
    case o => if (o.o_orderkey <= 150000000) List((lbl, o)) else Nil
  }
}
out2.lookupSkewLeft(out1)
}
         
val x268 = x258.flatMap{ case (x260, x259) => val x267 = (x260) 
x267 match {
   case (null) => Nil 
   case x266 => List(({val x261 = (x259) 
x261}, {val x262 = x260.o_orderdate 
val x263 = x260.o_parts 
val x264 = Record333(x263) 
val x265 = Record334(x262, x264) 
x265}))
 }
}.groupByLabel() 
val x273 = x268.map{ case (x269, x270) => 
   val x271 = x269.lbl 
val x272 = (x271, x270) 
x272 
} 
val M__D_2 = x273
val x274 = M__D_2
//M__D_2.collect.foreach(println(_))
val x276 = M__D_2 
val x280 = x276.flatMap{ 
 case x277 => {val x278 = x277._2 
x278}.map{ case v2 => (x277._1, v2) }
}
         
val x285 = x280.map{ case (x281, x282) => 
   val x283 = x282.o_parts 
val x284 = Record335(x283) 
x284 
} 
val x286 = x285.distinct 
val M_ctx3 = x286
val x287 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x289 = M_ctx3 
val x291 = Query1Extended__D_2c_orders_2o_parts_1
val x296 = { val out1 = x289.map{ case x292 => ({val x294 = x292.lbl 
val x295 = x294.o2__Fo_parts 
x295}, x292) }
out1.cogroup(x291.flatMapValues(identity)).flatMap{ pair =>
 for (k <- pair._2._1.iterator; w <- pair._2._2.iterator) yield (k,w)
}
}
         
val x301 = P__D_1.map(x297 => { val x298 = x297.p_retailprice 
val x299 = x297.p_name 
val x300 = Record336(x298, x299) 
x300 }) 
val x307 = { val out1 = x296.map{ case (x302, x303) => ({val x305 = x303.p_name 
x305}, (x302, x303)) }
  val out2 = x301.map{ case x304 => ({val x306 = x304.p_name 
x306}, x304) }
  out2.joinSkewLeft(out1).map{ case (k,v) => v }
} 
val x319 = x307.flatMap{ case (x310, (x308, x309)) => val x318 = (x308,x310) 
x318 match {
   case (_,null) => Nil
   case x317 => List(({val x311 = x310.p_name 
val x312 = Record338(x311) 
val x313 = (x308,x312) 
x313}, {val x314 = x309.l_qty 
val x315 = x310.p_retailprice 
val x316 = x314 * x315 
x316}))
 }
}.reduceByKey(_ + _) 
val x325 = x319.map{ case ((x320, x321), x322) => 
   val x323 = x320.lbl 
val x324 = (x323, Record388(x321.p_name, x322)) 
x324 
}.groupByLabel()
val M__D_3 = x325
val x326 = M__D_3
//M__D_3.collect.foreach(println(_))
x326.count
var end0 = System.currentTimeMillis() - start0
println("ShredQuery4Filter2Spark,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)

var start1 = System.currentTimeMillis()
val x357 = M__D_2
val x361 = x357.flatMap{
 case x358 => {val x359 = x358._2
x359}.map{ case v2 => (x358._1, v2) }
}

val x366 = { val out1 = x361.map{ case (x362, x363) => ({val x365 = x363.o_parts
x365}, (x362, x363)) }
out1.cogroup(M__D_3).flatMap{
 case (_, (left, x364)) => left.map{ case (x362, x363) => ((x362, x363), x364.flatten) }}
}

val x373 = x366.map{ case ((x367, x368), x369) =>
   val x370 = x368.o_orderdate
val x371 = Record389(x370, x369)
val x372 = (x367, x371)
x372
}
val newc_orders__D_1 = x373
val x374 = newc_orders__D_1
//newc_orders__D_1.collect.foreach(println(_))
val x376 = M__D_1
val x380 = { val out1 = x376.map{ case x377 => ({val x379 = x377.c_orders
x379}, x377) }
out1.cogroup(newc_orders__D_1).flatMap{
 case (_, (left, x378)) => left.map{ case x377 => (x377, x378) }}
}

val x385 = x380.map{ case (x381, x382) =>
   val x383 = x381.c_name
val x384 = Record390(x383, x382)
x384
}
val newM__D_1 = x385
val x386 = newM__D_1
//newM__D_1.collect.foreach(println(_))
x386.count
var end1 = System.currentTimeMillis() - start1
println("ShredQuery4Filter2Spark,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("ShredQuery4Filter2Spark"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
