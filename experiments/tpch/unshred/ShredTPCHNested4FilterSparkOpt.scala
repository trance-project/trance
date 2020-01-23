
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
case class Record325(c_name: String, c_orders: Record170)
case class Record326(c2__Fc_orders: Record170)
case class Record327(c_name: String, c_orders: Record326)
case class Record328(lbl: Record326)
case class Record330(o2__Fo_parts: Record175)
case class Record331(o_orderdate: String, o_parts: Record330)
case class Record332(lbl: Record330)
case class Record333(p_retailprice: Double, p_name: String)
case class Record335(p_name: String)
case class Record388(p_name: String, _2: Double)
case class Record389(o_orderdate: String, o_parts: Iterable[Record388])
case class Record390(c_name: String, c_orders: Iterable[Record389])
object ShredTPCHNested4FilterSparkOpt {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredTPCHNested4FilterSparkOpt"+sf)
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
val c_orders_ctx1 = x94
val x95 = c_orders_ctx1
//c_orders_ctx1.collect.foreach(println(_))
val x97 = c_orders_ctx1 
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
val c_orders__D_1 = x124
val x125 = c_orders__D_1
//c_orders__D_1.collect.foreach(println(_))
val x127 = c_orders__D_1 
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
val o_parts_ctx1 = x137
val x138 = o_parts_ctx1
//o_parts_ctx1.collect.foreach(println(_))
val x140 = o_parts_ctx1 
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
val o_parts__D_1 = x162
val x163 = o_parts__D_1
//o_parts__D_1.collect.foreach(println(_))
val Query1WK__D_1 = M__D_1//flat1
Query1WK__D_1.cache
Query1WK__D_1.count
val Query1WK__D_2c_orders_1 = c_orders__D_1//M_flat2
Query1WK__D_2c_orders_1.cache
Query1WK__D_2c_orders_1.count
val Query1WK__D_2c_orders_2o_parts_1 = o_parts__D_1//M_flat3
Query1WK__D_2c_orders_2o_parts_1.cache
Query1WK__D_2c_orders_2o_parts_1.count
 def f = {
 
var start0 = System.currentTimeMillis()
val x226 = () 
val x227 = Record165(x226) 
val x228 = List(x227) 
val M_ctx1 = x228
val x229 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x235 = Query1WK__D_1.filter(c => c.c_custkey <= 1500000 ).map(x230 => 
  { val x232 = x230.c_name 
val x233 = x230.c_orders 
val x234 = Record325(x232, x233) 
x234 }) 
val x241 = x235.map{ case x236 => 
   val x237 = x236.c_name 
val x238 = x236.c_orders 
val x239 = Record326(x238) 
val x240 = Record327(x237, x239) 
x240 
} 
val M__D_1 = x241
val x242 = M__D_1
//M__D_1.collect.foreach(println(_))
val x244 = M__D_1 
/**val x248 = x244.map{ case x245 => 
   val x246 = x245.c_orders 
val x247 = Record328(x246) 
x247 
} 
val x249 = x248.distinct 
val c_orders_ctx1 = x249
val x250 = c_orders_ctx1
//c_orders_ctx1.collect.foreach(println(_))
val x252 = c_orders_ctx1 
val x257 = { val out1 = x252.map{ case x253 => ({val x255 = x253.lbl 
val x256 = x255.c2__Fc_orders 
x256}, x253) }
out1.cogroup(Query1WK__D_2c_orders_1.flatMapValues(identity)).flatMap{ pair =>
 for (k <- pair._2._1.iterator; w <- pair._2._2.iterator) yield (k,w)
}
}**/
//val x257 = Query1WK__D_2c_orders_1.flatMapValues(identity)     
val x257 = Query1WK__D_2c_orders_1.flatMap{
  case (lbl, obag) => obag.flatMap{
    o => if (o.o_orderkey <= 150000000) List((lbl, o))
    else Nil
  }
}
val x267 = x257.flatMap{ case (x258, x259) => val x266 = (x259) 
x266 match {
   case (null) => Nil 
   case x265 => List(({val x260 = (x258) 
x260}, {val x261 = x259.o_orderdate 
val x262 = x259.o_parts 
val x263 = Record330(x262) 
val x264 = Record331(x261, x263) 
x264}))
 }
}.groupByLabel() 
val x272 = x267.map{ case (x268, x269) => 
   val x270 = Record326(x268)//.lbl 
val x271 = (x270, x269) 
x271 
} 
val c_orders__D_1 = x272
/**val x273 = c_orders__D_1
//c_orders__D_1.collect.foreach(println(_))
val x275 = c_orders__D_1 
val x279 = x275.flatMap{ 
 case x276 => {val x277 = x276._2 
x277}.map{ case v2 => (x276._1, v2) }
}
         
val x284 = x279.map{ case (x280, x281) => 
   val x282 = x281.o_parts 
val x283 = Record332(x282) 
x283 
} 
val x285 = x284.distinct 
val o_parts_ctx1 = x285
val x286 = o_parts_ctx1
//o_parts_ctx1.collect.foreach(println(_))
val x288 = o_parts_ctx1 
val x293 = { val out1 = x288.map{ case x289 => ({val x291 = x289.lbl 
val x292 = x291.o2__Fo_parts 
x292}, x289) }
out1.cogroup(Query1WK__D_2c_orders_2o_parts_1.flatMapValues(identity)).flatMap{ pair =>
 for (k <- pair._2._1.iterator; w <- pair._2._2.iterator) yield (k,w)
}
}**/
val x293 = Query1WK__D_2c_orders_2o_parts_1.flatMapValues(identity)
         
val x298 = P__D_1.map(x294 => { val x295 = x294.p_retailprice 
val x296 = x294.p_name 
val x297 = Record333(x295, x296) 
x297 }) 
val x304 = { val out1 = x293.map{ case (x299, x300) => ({val x302 = x300.p_name 
x302}, (x299, x300)) }
  val out2 = x298.map{ case x301 => ({val x303 = x301.p_name 
x303}, x301) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x316 = x304.flatMap{ case ((x305, x306), x307) => val x315 = (x305,x307) 
x315 match {
   case (_,null) => Nil
   case x314 => List(({val x308 = x307.p_name 
val x309 = Record335(x308) 
val x310 = (x305,x309) 
x310}, {val x311 = x306.l_qty 
val x312 = x307.p_retailprice 
val x313 = x311 * x312 
x313}))
 }
}.reduceByKey(_ + _) 
val x322 = x316.map{ case ((x317, x318), x319) => 
   val x320 = Record330(x317)//.lbl 
val x321 = (x320, Record388(x318.p_name, x319)) 
x321 
}.groupByLabel() 
val o_parts__D_1 = x322
val x323 = o_parts__D_1
//o_parts__D_1.collect.foreach(println(_))
x323.count
var end0 = System.currentTimeMillis() - start0
println("ShredTPCHNested4FilterSparkOpt,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    

var start1 = System.currentTimeMillis()
val x357 = c_orders__D_1 
val x361 = x357.flatMap{ 
 case x358 => {val x359 = x358._2 
x359}.map{ case v2 => (x358._1, v2) }
}
         
val x366 = { val out1 = x361.map{ case (x362, x363) => ({val x365 = x363.o_parts 
x365}, (x362, x363)) }
out1.cogroup(o_parts__D_1).flatMap{
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
newc_orders__D_1.cogroupSkewLeft(out1).flatMap{
 case (_, (x378, left)) => left.map{ case x377 => (x377, x378) }}
}
         
val x385 = x380.map{ case (x381, x382) => 
   val x383 = x381.c_name 
val x384 = Record390(x383, x382) 
x384 
} 
val newM__D_1 = x385
val x386 = newM__D_1
newM__D_1.collect.foreach(println(_))
x386.count
var end1 = System.currentTimeMillis() - start1
println("ShredTPCHNested4FilterSparkOpt,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)

}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("ShredTPCHNested4FilterSparkOpt"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
