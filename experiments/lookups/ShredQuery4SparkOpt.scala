
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
import sprkloader.SkewDictRDD._
import sprkloader.DomainRDD._
case class Record159(lbl: Unit)
case class Record160(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record161(p_name: String, p_partkey: Int)
//case class Record162(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record162(l_orderkey: Int, p_partkey: Int, l_qty: Double)
case class Record163(c__Fc_custkey: Int)
case class Record164(c_name: String, c_orders: Record163)
case class Record165(lbl: Record163)
case class Record166(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record168(o__Fo_orderkey: Int)
case class Record169(o_orderdate: String, o_parts: Record168)
case class Record170(lbl: Record168)
//case class Record172(p_name: String, l_qty: Double)
case class Record172(p_partkey: Int, l_qty: Double)
case class Record311(c2__Fc_orders: Record163)
case class Record312(c_name: String, c_orders: Record311)
case class Record313(lbl: Record311)
case class Record315(o2__Fo_parts: Record168)
case class Record316(o_orderdate: String, o_parts: Record315)
case class Record317(lbl: Record315)
case class Record318(p_partkey: Int, p_retailprice: Double, p_name: String)
case class Record320(p_name: String)
case class Record373(p_name: String, _2: Double)
case class Record374(o_orderdate: String, o_parts: Iterable[Record373])
case class Record375(c_name: String, c_orders: Iterable[Record374])
object ShredQuery4SparkOpt {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4SparkOpt"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)

val L__F = 3
val L__D_1 = tpch.loadLineitemProj//Bzip
L__D_1.cache
spark.sparkContext.runJob(L__D_1, (iter: Iterator[_]) => {})
val P__F = 4
val P__D_1 = tpch.loadPartProj4
P__D_1.cache
spark.sparkContext.runJob(P__D_1, (iter: Iterator[_]) => {})
val C__F = 1
val C__D_1 = tpch.loadCustomersProj
C__D_1.cache
spark.sparkContext.runJob(C__D_1, (iter: Iterator[_]) => {})
val O__F = 2
val O__D_1 = tpch.loadOrdersProj//Bzip
O__D_1.cache
spark.sparkContext.runJob(O__D_1, (iter: Iterator[_]) => {})

tpch.triggerGC

val x55 = L__D_1
val x60 = P__D_1
val x65_out1 = x55.map{ case x61 => ({val x63 = x61.l_partkey 
x63}, x61) }
val x65 = x65_out1.joinSkew(x60, (p: PartProj4) => p.p_partkey)

val x72 = x65.map{ case (x66, x67) => 
   val x68 = x66.l_orderkey 
val x69 = x67.p_partkey 
val x70 = x66.l_quantity 
val x71 = Record162(x68, x69, x70) 
x71 
} 
val ljp__D_1 = x72
val x73 = ljp__D_1
//ljp__D_1.collect.foreach(println(_))

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
val c_orders_ctx1 = x83.createDomain(l => Record165(l.c_orders))

val x91 = c_orders_ctx1 
val x97 = O__D_1.map(x92 => { val x93 = x92.o_orderdate 
val x94 = x92.o_orderkey 
val x95 = x92.o_custkey 
val x96 = Record166(x93, x94, x95) 
x96 }) 
/**val x103_out1 = x91.map{ case x98 => ({val x100 = x98.lbl 
val x101 = x100.c__Fc_custkey 
x101}, x98) }**/
val x103_out2 = x97.map{ case x99 => ({val x102 = x99.o_custkey 
x102}, x99) }
val x103 = x103_out2.joinDomainSkew(x91, (l: Record165) => l.lbl.c__Fc_custkey)
val x113 = x103.map{ case (x105, x104) => 
   ({val x106 = (x104) 
x106}, {val x107 = x105.o_orderdate 
val x108 = x105.o_orderkey 
val x109 = Record168(x108) 
val x110 = Record169(x107, x109) 
x110})
}.groupByLabel() 
val c_orders__D_1 = x113
val x119 = c_orders__D_1
//c_orders__D_1.collect.foreach(println(_))

val x121 = c_orders__D_1 
val o_parts_ctx1 = x121.createDomain(l => Record170(l.o_parts))
val x132 = o_parts_ctx1
//o_parts_ctx1.collect.foreach(println(_))

val x134 = o_parts_ctx1 
val x136 = ljp__D_1 
/**val x142_out1 = x134.map{ case x137 => ({val x139 = x137.lbl 
val x140 = x139.o__Fo_orderkey 
x140}, x137) }**/
val x142_out2 = x136.map{ case x138 => ({val x141 = x138.l_orderkey 
x141}, x138) }
val x142 = x142_out2.joinDomainSkew(x134, (l: Record170) => l.lbl.o__Fo_orderkey)
val x151 = x142.map{ case (x144, x143) =>
  ({val x145 = (x143) 
x145}, {val x146 = x144.p_partkey
val x147 = x144.l_qty 
val x148 = Record172(x146, x147) 
x148})
}.groupByLabel() 
val o_parts__D_1 = x151
val x157 = o_parts__D_1
//o_parts__D_1.collect.foreach(println(_))

val Query1__D_1 = M__D_1//M_flat1
Query1__D_1.cache
spark.sparkContext.runJob(Query1__D_1, (iter: Iterator[_]) => {})

val Query1__D_2c_orders_1 = c_orders__D_1//M_flat2
Query1__D_2c_orders_1.cache
spark.sparkContext.runJob(Query1__D_2c_orders_1, (iter: Iterator[_]) => {})

val Query1__D_2c_orders_2o_parts_1 = o_parts__D_1//M_flat3
Query1__D_2c_orders_2o_parts_1.cache
spark.sparkContext.runJob(Query1__D_2c_orders_2o_parts_1, (iter: Iterator[_]) => {})

 def f = {
 
var start0 = System.currentTimeMillis()
val x227 = Query1__D_1.map{ case x222 => 
   val x223 = x222.c_name 
val x224 = x222.c_orders 
val x225 = Record311(x224) 
val x226 = Record312(x223, x225) 
x226 
} 
val M__D_1 = x227
val x228 = M__D_1
spark.sparkContext.runJob(M__D_1, (iter: Iterator[_]) => {})

val x258 = Query1__D_2c_orders_1.map{
  case (lbl, bag) => (Record311(lbl.lbl), bag.map{ v => Record316(v.o_orderdate, Record315(v.o_parts)) }) 
} 
val c_orders__D_1 = x258
val x259 = c_orders__D_1
spark.sparkContext.runJob(c_orders__D_1, (iter: Iterator[_]) => {})

val x279 = Query1__D_2c_orders_2o_parts_1

val x284 = P__D_1.map(x280 => { val x281 = x280.p_retailprice 
val x282 = x280.p_name 
val x283 = Record318(x280.p_partkey, x281, x282) 
x283 }) 
val x290_out1 = x279.flatMap{ case (lbl, bag) => bag.map(x286 => ({val x288 = x286.p_partkey
x288}, (lbl, x286))) }
val x290_out2 = x284.map{ case x287 => ({val x289 = x287.p_partkey
x289}, x287) }
val x290 = x290_out1.joinSkew(x290_out2)
val x302 = x290.map{ case ((x291, x292), x293) => 
   ({val x294 = x293.p_name 
val x295 = Record320(x294) 
val x296 = (x291,x295) 
x296}, {val x297 = x292.l_qty 
val x298 = x293.p_retailprice 
val x299 = x297 * x298 
x299})
}.reduceByKey(_ + _) 
val x308 = x302.map{ case ((x303, x304), x305) => 
   val x306 = x303.lbl 
val x307 = (Record315(x306), Record373(x304.p_name, x305)) 
x307 
}.groupByLabel() 
val o_parts__D_1 = x308
val x309 = o_parts__D_1
//o_parts__D_1.collect.foreach(println(_))
spark.sparkContext.runJob(o_parts__D_1, (iter: Iterator[_]) => {})
var end0 = System.currentTimeMillis() - start0
println("ShredQuery4SparkOpt,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    

var start1 = System.currentTimeMillis()
/**val x342 = c_orders__D_1 
val x346 = x342.flatMap{ 
 case x343 => {val x344 = x343._2 
x344}.map{ case v2 => (x343._1, v2) }
}
         
val x351 = { val out1 = x346.map{ case (x347, x348) => ({val x350 = x348.o_parts 
x350}, (x347, x348)) }
out1.cogroup(o_parts__D_1).flatMap{
 case (_, (left, x349)) => left.map{ case (x347, x348) => ((x347, x348), x349.flatten) }}
}
         
val x358 = x351.map{ case ((x352, x353), x354) => 
   val x355 = x353.o_orderdate 
val x356 = Record374(x355, x354) 
val x357 = (x352, x356) 
x357 
} 
val newc_orders__D_1 = x358
val x359 = newc_orders__D_1
//newc_orders__D_1.collect.foreach(println(_))
val x361 = M__D_1 
val x365 = { val out1 = x361.map{ case x362 => ({val x364 = x362.c_orders 
x364}, x362) }
out1.cogroup(newc_orders__D_1).flatMap{
 case (_, (left, x363)) => left.map{ case x362 => (x362, x363) }}
}
         
val x370 = x365.map{ case (x366, x367) => 
   val x368 = x366.c_name 
val x369 = Record375(x368, x367) 
x369 
} 
val newM__D_1 = x370
val x371 = newM__D_1
newM__D_1.collect.foreach(println(_))
spark.sparkContext.runJob(x371, (iter: Iterator[_]) => {})**/
var end1 = System.currentTimeMillis() - start1
println("ShredQuery4SparkOpt,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("ShredQuery4SparkOpt"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
