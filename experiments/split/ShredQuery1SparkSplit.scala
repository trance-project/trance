
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
import sprkloader.SkewDictRDD._
import sprkloader.DomainRDD._
import sprkloader.UtilPairRDD._
case class Record323(lbl: Unit)
case class Record324(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record325(p_name: String, p_partkey: Int)
case class Record326(l_orderkey: Int, p_partkey: Int, l_qty: Double)
case class Record327(c__Fc_custkey: Int)
case class Record328(c_name: String, c_orders: Record327)
case class Record329(lbl: Record327)
case class Record330(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record332(o__Fo_orderkey: Int)
case class Record333(o_orderdate: String, o_parts: Record332)
case class Record334(lbl: Record332)
case class Record336(p_partkey: Int, l_qty: Double)
object ShredQuery1SparkSplit {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1SparkSplit"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)

val L__F = 3
val L__D_1 = tpch.loadLineitemProjBzip
L__D_1.cache
spark.sparkContext.runJob(L__D_1, (iter: Iterator[_]) => {})
val P__F = 4
val P__D_1 = tpch.loadPartProj
P__D_1.cache
spark.sparkContext.runJob(P__D_1, (iter: Iterator[_]) => {})
val C__F = 1
val C__D_1 = tpch.loadCustomersProj
C__D_1.cache
spark.sparkContext.runJob(C__D_1, (iter: Iterator[_]) => {})
val O__F = 2
val O__D_1 = tpch.loadOrdersProjBzip
O__D_1.cache
spark.sparkContext.runJob(O__D_1, (iter: Iterator[_]) => {})

 def f = {
var start0 = System.currentTimeMillis() 

val x219 = L__D_1
val x224 = P__D_1
val x225 = x224.map{ case x225 => ({val x227 = x225.p_partkey
x227}, x225) }
val x226 = x219.map{ case x225 => ({val x227 = x225.l_partkey 
x227}, x225) }
val (x227_L, x227_H, hpks) = x226.joinSplit(x225)
 
val x236_L = x227_L.mapPartitions( it =>
  it.map{ case (x230, x231) => 
   val x232 = x230.l_orderkey 
val x233 = x231.p_partkey
val x234 = x230.l_quantity 
val x235 = Record326(x232, x233, x234) 
x235 
}, true) 
val x236_H = x227_H.mapPartitions(it =>
  it.map{ case (x230, x231) => 
   val x232 = x230.l_orderkey 
val x233 = x231.p_partkey
val x234 = x230.l_quantity 
val x235 = Record326(x232, x233, x234) 
x235 
}, true) 

// top level stays light
val x244 = C__D_1.map{ case x239 => 
   val x240 = x239.c_name 
val x241 = x239.c_custkey 
val x242 = Record327(x241) 
val x243 = Record328(x240, x242) 
x243 
} 
val M__D_1 = x244
val x245 = M__D_1

val x247 = M__D_1.createDomain(l => Record329(l.c_orders)) 
val c_orders_ctx1 = x247
val x253 = c_orders_ctx1

val x255 = c_orders_ctx1 
val x261 = O__D_1
val x264 = x261.map{ case x263 => ({val x266 = x263.o_custkey 
x266}, x263) }
val (x267_L, x267_H, hcks) = x264.joinSplit(x255, (l: Record329) => l.lbl.c__Fc_custkey)
val hkeys1 = hcks.map(l => Record327(l))

// don't even need this mapping...
val x277_L = x267_L.mapPartitions(it =>
  it.map{ case (x269, x268) => 
   ({val x270 = (x268) 
x270.lbl}, {val x271 = x269.o_orderdate 
val x272 = x269.o_orderkey 
val x273 = Record332(x272) 
val x274 = Record333(x271, x273) 
x274})
}, true)
val x277_H = x267_H.mapPartitions( it =>
  it.map{ case (x269, x268) => 
   ({val x270 = (x268) 
x270.lbl}, {val x271 = x269.o_orderdate 
val x272 = x269.o_orderkey 
val x273 = Record332(x272) 
val x274 = Record333(x271, x273) 
x274})
}, true)

// we know the heavy ones from the lookup...
val (c_orders__D_1_L, c_orders__D_1_H) = x277_L.groupBySplit(x277_H, hkeys1)

val x285_L = c_orders__D_1_L.createDomain(l => Record334(l.o_parts))
val x285_H = c_orders__D_1_H.createDomain(l => Record334(l.o_parts))

// partitioning information lost 
val x303_L = x236_L.mapPartitions( it =>
  it.map{ case x302 => ({val x305 = x302.l_orderkey 
x305}, x302) })
val x303_H = x236_H.mapPartitions( it =>
  it.map{ case x302 => ({val x305 = x302.l_orderkey 
x305}, x302) })

// need a case for this
val (x304_L, x304_H, hoks) = (x303_L.unionPartitions(x303_H)).joinSplit(x285_L.unionPartitions(x285_H), (l: Record334) => l.lbl.o__Fo_orderkey)
val hkeys2 = hoks.map(l => Record332(l))

val x315_L = x304_L.mapPartitions(it =>
  it.map{ case (x308, x307) => 
  ({val x309 = (x307) 
x309.lbl}, {val x310 = x308.p_partkey 
val x311 = x308.l_qty 
val x312 = Record336(x310, x311) 
x312})
})

val x315_H = x304_H.mapPartitions(it =>
  it.map{ case (x308, x307) =>
  ({val x309 = (x307)
x309.lbl}, {val x310 = x308.p_partkey
val x311 = x308.l_qty
val x312 = Record336(x310, x311)
x312})
})

val (o_parts__D_1_L, o_parts__D_1_H) = x315_L.groupBySplit(x315_H, hkeys2)
spark.sparkContext.runJob(o_parts__D_1_L, (iter: Iterator[_]) => {})
spark.sparkContext.runJob(o_parts__D_1_H, (iter: Iterator[_]) => {})

var end0 = System.currentTimeMillis() - start0
println("ShredQuery1SparkSplit,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)

var start1 = System.currentTimeMillis()
/**val x201 = (c_orders__D_1_L union c_orders__D_1_H).mapPartitions(
  it => it.flatMap(v => v._2.map(o => (o.o_parts, (v._1, o.o_orderdate)))), false
).cogroup((o_parts__D_1_L union o_parts__D_1_H)).mapPartitions(
  it => it.flatMap{ case (_, (left, x208)) => left.map{ case (x206, x207) => (x206, (x207, x208.flatten)) }}, false
)
val result = M__D_1.map(c => c.c_orders -> c.c_name).cogroup(x201).mapPartitions(
  it => it.flatMap{ case (_, (left, x208)) => left.map( cname => cname -> x208)}, false
)
//result.collect.foreach(println(_))
spark.sparkContext.runJob(result, (iter: Iterator[_]) => {})**/
var end = System.currentTimeMillis() - start0
var end1 = System.currentTimeMillis() - start1
println("ShredQuery1SparkSplit,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
println("ShredQuery1SparkSplit,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)

/**result.flatMap{ case (cname, orders) =>
  if (orders.isEmpty) List((cname, null, null, null))
  else orders.flatMap{ case (date, parts) =>
    if (parts.isEmpty) List((cname, date, null, null))
    else parts.map(p => (cname, date, p.p_partkey, p.l_qty)) } }.collect.foreach(println(_))**/
}
f
 }
}
