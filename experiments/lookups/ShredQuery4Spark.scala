
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
import sprkloader.SkewDictRDD._
import sprkloader.DomainRDD._
case class Record159(lbl: Unit)
case class Record160(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record161(p_pname: String, p_partkey: Int)
case class Record162(l_orderkey: Int, p_partkey: Int, l_qty: Double)
case class Record163(c__Fc_custkey: Int)
case class Record164(c_name: String, c_orders: Record163)
case class Record165(lbl: Record163)
case class Record166(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record168(o__Fo_orderkey: Int)
case class Record169(o_orderdate: String, o_parts: Record168)
case class Record170(lbl: Record168)
case class Record172(p_partkey: Int, l_qty: Double)
case class Record311(c2__Fc_orders: Record163)
case class Record312(c_name: String, c_orders: Record311)
case class Record313(lbl: Record311)
case class Record315(o2__Fo_parts: Record168)
case class Record316(o_orderdate: String, o_parts: Record315)
case class Record317(lbl: Record315)
case class Record318(p_retailprice: Double, p_name: String)
case class Record320(p_name: String)
case class Record373(p_name: String, _2: Double)
case class Record374(o_orderdate: String, o_parts: Iterable[Record373])
case class Record375(c_name: String, c_orders: Iterable[Record374])
object ShredQuery4Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4Spark"+sf)
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

val x62 = x55.map{ case x61 => ({val x63 = x61.l_partkey 
x63}, x61) }
val x65 = x62.joinSkew(x60, (p: PartProj4) => p.p_partkey) 

val x72 = x65.map{ case (x66, x67) => 
   val x68 = x66.l_orderkey 
val x69 = x67.p_partkey
val x70 = x66.l_quantity 
val x71 = Record162(x68, x69, x70) 
x71 
} 
val ljp__D_1 = x72
val x73 = ljp__D_1

val x80 = C__D_1.map{ case x75 => 
   val x76 = x75.c_name 
val x77 = x75.c_custkey 
val x78 = Record163(x77) 
val x79 = Record164(x76, x78) 
x79 
} 
val M__D_1 = x80
val x81 = M__D_1
val x83 = M__D_1 

val x84 = x83.createDomain(l => Record165(l.c_orders))//.distinct
val c_orders_ctx1 = x84

val x91 = c_orders_ctx1 
val x97 = O__D_1.map(x92 => { val x93 = x92.o_orderdate 
val x94 = x92.o_orderkey 
val x95 = x92.o_custkey 
val x96 = Record166(x93, x94, x95) 
x96 }) 
val x100 = x97.map{ case x99 => ({val x102 = x99.o_custkey 
x102}, x99) }
val x103 = x100.joinDomainSkew(x91, (l: Record165) => l.lbl.c__Fc_custkey)

val x113 = x103.map{ case (x105, x104) => 
  ({val x106 = (x104) 
  x106.lbl}, {val x107 = x105.o_orderdate 
  val x108 = x105.o_orderkey 
  val x109 = Record168(x108) 
  val x110 = Record169(x107, x109) 
  x110})
}.groupByLabel() 
val c_orders__D_1 = x113
val x119 = c_orders__D_1

val x121 = c_orders__D_1
val x131 = x121.createDomain(l => Record170(l.o_parts))//.distinct 
val o_parts_ctx1 = x131
val x132 = o_parts_ctx1

val x134 = o_parts_ctx1 
val x136 = ljp__D_1 
val x139 = x136.map{ case x138 => ({val x141 = x138.l_orderkey 
x141}, x138) }
val x142 = x139.joinDomainSkew(x134, (l: Record170) => l.lbl.o__Fo_orderkey)
val x151 = x142.map{ case (x144, x143) => 
  ({val x145 = (x143) 
  x145.lbl}, {val x146 = x144.p_partkey
val x147 = x144.l_qty 
val x148 = Record172(x146, x147) 
x148})
}.groupByLabel() 
val o_parts__D_1 = x151
val x157 = o_parts__D_1

val Query1__D_1 = M__D_1
Query1__D_1.cache
spark.sparkContext.runJob(Query1__D_1, (iter: Iterator[_]) => {})

val Query1__D_2c_orders_1 = c_orders__D_1
Query1__D_2c_orders_1.cache
spark.sparkContext.runJob(Query1__D_2c_orders_1, (iter: Iterator[_]) => {})

val Query1__D_2c_orders_2o_parts_1 = o_parts__D_1
Query1__D_2c_orders_2o_parts_1.cache
spark.sparkContext.runJob(Query1__D_2c_orders_2o_parts_1, (iter: Iterator[_]) => {})

//tpch.triggerGC

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

val x230 = M__D_1

// don't wrap a label in a label wrapper, just collect it
val x235 = x230.createDomain(l => l.c_orders)//.distinct
val c_orders_ctx1 = x235
val x236 = c_orders_ctx1

val x238 = c_orders_ctx1
/**val x239 = x238.map{ case x239 => {val x241 = x239.lbl 
val x242 = x241.c2__Fc_orders 
x242}}**/


val x243 = Query1__D_2c_orders_1
// generate inverse extract function 
// this is an optimization
val x244 = x243.lookupSkew(x238, (l: Record311) => l.c2__Fc_orders, (l: Record163) => Record311(l),
  (o: Record169) => Record316(o.o_orderdate, Record315(o.o_parts)))

val c_orders__D_1 = x244
val x259 = c_orders__D_1

// again don't wrap a label 
val o_parts_ctx1 = c_orders__D_1.createDomain(v => v.o_parts)//.distinct
val x274 = o_parts_ctx1

val x275 = o_parts_ctx1
/**val x276 = x275.map{ case x275 => ({val x277 = x275.lbl 
val x278 = x277.o2__Fo_parts 
x278}, x275) }**/

val x277 = Query1__D_2c_orders_2o_parts_1
//val x279 = x277.lookupSkew(x275, (l: Record317) => l.lbl.o2__Fo_parts)
val x279 = x277.lookupSkewIterator(x275, (l:Record315) => l.o2__Fo_parts, (p: Record172) => p.p_partkey)
val x284 = P__D_1
val x285 = x279
//val x285 = x279.flatMap{ case (lbl, bag) => bag.map{ case x286 => ({val x288 = x286.p_partkey 
//x288}, (lbl, x286)) }}
/**val x291 = x284.map{ case x287 => ({val x289 = x287.p_name 
x289}, x287) }**/
val x290 = x285.joinSkew(x284, (p: PartProj4) => p.p_partkey)
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
   val x306 = x303 
val x307 = (x306, Record373(x304.p_name, x305)) 
x307 
}.groupByLabel() 
val o_parts__D_1 = x308
val x309 = o_parts__D_1
//o_parts__D_1.collect.foreach(println(_))
spark.sparkContext.runJob(x309, (iter: Iterator[_]) => {})
var end0 = System.currentTimeMillis() - start0
println("ShredQuery4Spark,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)

var start1 = System.currentTimeMillis()
val x342 = c_orders__D_1.mapPartitions(
    it => it.flatMap(v => v._2.map(o => (o.o_parts, (v._1, o.o_orderdate)))), false
  ).cogroup(o_parts__D_1).flatMap{
    case (_, (left, x349)) => left.map{ case (lbl, date) => (lbl, (date, x349.flatten)) }
  }
val result = M__D_1.map(c => c.c_orders -> c.c_name).cogroup(x342).flatMap{
  case (_, (left, x349)) => left.map(cname => cname -> x349)
}
//result.collect.foreach(println(_))
spark.sparkContext.runJob(result, (iter: Iterator[_]) => {})
var end1 = System.currentTimeMillis() - start1
println("ShredQuery4Spark,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)

}
f
 }
}
