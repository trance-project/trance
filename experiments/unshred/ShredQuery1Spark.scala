
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
object ShredQuery1Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1Spark"+sf)
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
val C__D_1 = tpch.loadCustomersProj()
C__D_1.cache
spark.sparkContext.runJob(C__D_1, (iter: Iterator[_]) => {})
val O__F = 2
val O__D_1 = tpch.loadOrdersProj()
O__D_1.cache
spark.sparkContext.runJob(O__D_1, (iter: Iterator[_]) => {})

 def f = {
var start0 = System.currentTimeMillis() 

// top level stays light
val x244 = C__D_1.map{ case x239 => 
   val x240 = x239.c_name 
val x241 = x239.c_custkey 
val x243 = Record328(x240, x241) 
x243 
} 
val M__D_1 = x244
val x245 = M__D_1
M__D_1.cache

val x247 = M__D_1.createDomain(l => l.c_orders)
val c_orders_ctx1 = x247
val x253 = c_orders_ctx1

val x255 = c_orders_ctx1 
val x261 = O__D_1
val x264 = x261.map{ case x263 => 
  (x263.o_custkey, Record333(x263.o_orderdate, x263.o_orderkey)) }
val c_orders__D_1 = x264.cogroupDomain(x255)
c_orders__D_1.cache

val x285 = c_orders__D_1.createDomain(l => l.o_parts)

val x219 = L__D_1
val x224 = P__D_1
val x225 = x224.map{ case x225 => ({val x227 = x225.p_partkey
x227}, x225) }
val x226 = x219.map{ case x225 => ({val x227 = x225.l_partkey 
x227}, x225) }
val x227 = x226.cogroup(x225).flatMap{
  case (_, (ls, ps)) => 
    for (l <- ls.iterator; p <- ps.iterator) 
      yield (l.l_orderkey, Record336(p.p_name, l.l_quantity))
}
 
val o_parts__D_1 = x227.cogroupDomain(x285)
o_parts__D_1.cache
spark.sparkContext.runJob(o_parts__D_1, (iter: Iterator[_]) => {})

var end0 = System.currentTimeMillis() - start0
println("ShredQuery1Spark,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)

var start1 = System.currentTimeMillis()
val x201 = c_orders__D_1.flatMap{
  case (lbl, bag) => bag.map(o => (o.o_parts, (lbl, o.o_orderdate)))
}.joinDropKey(o_parts__D_1).map{ 
  case ((lbl, odate), parts) => lbl -> (odate, parts)
}
val result = M__D_1.map(c => c.c_orders -> c.c_name).cogroup(x201).flatMap{
  case (_, (left, x208)) => left.map( cname => cname -> x208)
}
//result.collect.foreach(println(_))
spark.sparkContext.runJob(result, (iter: Iterator[_]) => {})
var end = System.currentTimeMillis() - start0
var end1 = System.currentTimeMillis() - start1
println("ShredQuery1Spark,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
println("ShredQuery1Spark,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)

/**result.flatMap{ case (cname, orders) =>
  if (orders.isEmpty) List((cname, null, null, null))
  else orders.flatMap{ case (date, parts) =>
    if (parts.isEmpty) List((cname, date, null, null))
    else parts.map(p => (cname, date, p.p_name, p.l_qty)) } }.collect.foreach(println(_))**/
}
f
 }
}
