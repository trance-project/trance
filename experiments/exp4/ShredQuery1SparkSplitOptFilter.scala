
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
case class Record328(c_name: String, c_orders: Int)
case class Record330(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record333(o_orderdate: String, o_parts: Int)
case class Record336(p_name: String, l_qty: Double)
object ShredQuery1SparkSplitOptFilter {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1SparkSplitOptFilter"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)

val L__F = 3
val L__D_1 = tpch.loadLineitemProj()
L__D_1.cache
spark.sparkContext.runJob(L__D_1, (iter: Iterator[_]) => {})
val P__F = 4
val P__D_1 = tpch.loadPartProj()
P__D_1.cache
spark.sparkContext.runJob(P__D_1, (iter: Iterator[_]) => {})
val C__F = 1
val C__D_1 = tpch.loadCustomersProj5()
C__D_1.cache
spark.sparkContext.runJob(C__D_1, (iter: Iterator[_]) => {})
val O__F = 2
val O__D_1 = tpch.loadOrdersProj()
O__D_1.cache
spark.sparkContext.runJob(O__D_1, (iter: Iterator[_]) => {})

 def f = {
var start0 = System.currentTimeMillis() 


// top level stays light
val x244 = C__D_1.flatMap{ case x239 => 
  if (x239.c_nationkey < 13) List(Record328(x239.c_name, x239.c_custkey)) else Nil }
val M__D_1 = x244
val x245 = M__D_1
spark.sparkContext.runJob(M__D_1, (iter: Iterator[_]) => {})

val (c_orders__D_1_L, c_orders__D_1_H, hkeys1) = 
  O__D_1.mapPartitions(it =>
    it.map{ case x268 => 
      (x268.o_custkey, {val x271 = x268.o_orderdate 
      val x272 = x268.o_orderkey 
      val x274 = Record333(x271, x272) 
      x274})
    }).groupBySplit()
spark.sparkContext.runJob(c_orders__D_1_L, (iter: Iterator[_]) => {})
spark.sparkContext.runJob(c_orders__D_1_H, (iter: Iterator[_]) => {})

val x219 = L__D_1
val x224 = P__D_1
val x225 = x224.map{ case x225 => ({val x227 = x225.p_partkey
x227}, x225) }
val x226 = x219.map{ case x225 => ({val x227 = x225.l_partkey 
x227}, x225) }
val (x227_L, x227_H, hpks) = x226.joinSplit(x225)

val x315_L = x227_L.mapPartitions(it =>
  it.map{ case (x308, x307) => 
  (x308.l_orderkey, 
    {val x310 = x307.p_name
      val x311 = x308.l_quantity
      val x312 = Record336(x310, x311) 
    x312})
  })

val x315_H = x227_H.mapPartitions(it =>
  it.map{ case (x308, x307) => 
  (x308.l_orderkey, 
    {val x310 = x307.p_name
      val x311 = x308.l_quantity
      val x312 = Record336(x310, x311) 
    x312})
  })

val (o_parts__D_1_L, o_parts__D_1_H, hkeys2) = x315_L.groupBySplit(x315_H)
spark.sparkContext.runJob(o_parts__D_1_L, (iter: Iterator[_]) => {})
spark.sparkContext.runJob(o_parts__D_1_H, (iter: Iterator[_]) => {})

var end0 = System.currentTimeMillis() - start0
println("ShredQuery1SparkSplitOptFilter,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)

var start1 = System.currentTimeMillis()
/**val x201 = c_orders__D_1_L.unionPartitions(c_orders__D_1_H, false).flatMap{
  case (lbl, bag) => bag.map(o => (o.o_parts, (lbl, o.o_orderdate)))
}.cogroup(o_parts__D_1_L.unionPartitions(o_parts__D_1_H)).flatMap{ 
  case (_, (left, x208)) => left.map{ case (x206, x207) => (x206, (x207, x208.flatten)) }
}
val result = M__D_1.map(c => c.c_orders -> c.c_name).cogroup(x201).flatMap{ 
  case (_, (left, x208)) => left.map( cname => cname -> x208)
}
//result.collect.foreach(println(_))
spark.sparkContext.runJob(result, (iter: Iterator[_]) => {})**/
var end = System.currentTimeMillis() - start0
var end1 = System.currentTimeMillis() - start1
println("ShredQuery1SparkSplitOptFilter,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
println("ShredQuery1SparkSplitOptFilter,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)

/**result.flatMap{ case (cname, orders) =>
  if (orders.isEmpty) List((cname, null, null, null))
  else orders.flatMap{ case (date, parts) =>
    if (parts.isEmpty) List((cname, date, null, null))
    else parts.map(p => (cname, date, p.p_name, p.l_qty)) } }.collect.foreach(println(_))**/
}
f
 }
}
