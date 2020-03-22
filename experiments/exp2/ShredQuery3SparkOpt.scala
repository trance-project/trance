
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
import sprkloader.SkewDictRDD._
import sprkloader.DomainRDD._
import sprkloader.UtilPairRDD._
import scala.collection.mutable.HashMap
case class Record323(lbl: Unit)
case class Record324(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record325(p_name: String, p_partkey: Int)
case class Record326(l_orderkey: Int, p_partkey: Int, l_qty: Double)
case class Record328(c_name: String, c_orders: Int)
case class Record330(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record333(o_orderdate: String, o_parts: Int)
case class Record336(p_partkey: Int, l_qty: Double)

case class Record413(c_name: String, totals: Int)
case class Record244(p_retailprice: Double, p_name: String)
case class Record246(c_name: String, p_name: String)
object ShredQuery3SparkOpt {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery3SparkOpt"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)

val L__F = 3
val L__D_1 = tpch.loadLineitemProj()
L__D_1.cache
spark.sparkContext.runJob(L__D_1, (iter: Iterator[_]) => {})
val P__F = 4
val P__D_1 = tpch.loadPartProj4()
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

val x219 = L__D_1
val x224 = P__D_1
val x225 = x224.map{ case x225 => ({val x227 = x225.p_partkey
x227}, x225) }
val x226 = x219.map{ case x225 => ({val x227 = x225.l_partkey 
x227}, x225) }
val x227 = x226.joinDropKey(x225)
 
val x236 = x227.mapPartitions( it =>
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
val x243 = Record328(x240, x241) 
x243 
} 
val M__D_1 = x244
val x245 = M__D_1

val x247 = M__D_1.createDomain(l => l.c_orders) 
val c_orders_ctx1 = x247
val x253 = c_orders_ctx1

val x255 = c_orders_ctx1 
val x261 = O__D_1
val x264 = x261.map{ case x263 => ({val x266 = x263.o_custkey 
x266}, OrdersProj(x263.o_orderkey, x263.o_custkey, x263.o_orderdate)) }
val x267 = x264.joinDomain(x255, (l: Int) => l)

val x277 = x267.mapPartitions(it =>
  it.map{ case (x269, x268) => 
   ({val x270 = (x268) 
x270}, {val x271 = x269.o_orderdate 
val x272 = x269.o_orderkey 
val x274 = Record333(x271, x272) 
x274})
}, true)

val c_orders__D_1 = x277.groupByKey()

val x285 = c_orders__D_1.createDomain(l => l.o_parts)

val x303 = x236.mapPartitions( it =>
  it.map{ case x302 => ({val x305 = x302.l_orderkey 
x305}, x302) })

val x304 = x303.joinDomain(x285, (l: Int) => l)

val x315 = x304.mapPartitions(it =>
  it.map{ case (x308, x307) => 
  ({val x309 = (x307) 
x309}, {val x310 = x308.p_partkey 
val x311 = x308.l_qty 
val x312 = Record336(x310, x311) 
x312})
}, true)

val o_parts__D_1 = x315.groupByKey()

//o_parts__D_1.collect.foreach(println(_))
val query1__D_1 = M__D_1
query1__D_1.cache
spark.sparkContext.runJob(query1__D_1, (iter: Iterator[_]) => {})

val query1__D_2c_orders_1 = c_orders__D_1
query1__D_2c_orders_1.cache
spark.sparkContext.runJob(query1__D_2c_orders_1, (iter: Iterator[_]) => {})

val query1__D_2c_orders_2o_parts_1 = o_parts__D_1
query1__D_2c_orders_2o_parts_1.cache
spark.sparkContext.runJob(query1__D_2c_orders_2o_parts_1, (iter: Iterator[_]) => {})

 def f = {
 
var start0 = System.currentTimeMillis()
val x221 = P__D_1


val x223 = query1__D_2c_orders_2o_parts_1.mapPartitions( it =>
      it.map{ case (lbl, bag) => (lbl, bag.foldLeft(HashMap.empty[Int, Double].withDefaultValue(0))(
            (acc, p) => {acc(p.p_partkey) += p.l_qty; acc})) }, true)

val x224 = x223.flatMap{
  case (lbl, bag) => bag.map(p => p._1 -> (lbl, p._2))
}

val x225p = x221.map(p => p.p_partkey -> p)

val x225 = x224.joinDropKey(x225p)

val x226 = x225.map{ 
  case ((lbl, tot), p) => (lbl, (p.p_name, tot*p.p_retailprice))
}

val x208 = query1__D_1.map(c => c.c_orders -> c.c_name)

val x222 = query1__D_2c_orders_1.joinDropKey(x208)

val x231 = x222.flatMap{
  case (dates, cname) => dates.map(d => d.o_parts -> cname )
}

val x227 = x226.joinDropKey(x231)

val x228 = x227.map{
  case ((pname, tot), cname) => ((cname, pname), tot)
}

val m__D_1 = x228.reduceByKey(_+_)

//m__D_1.collect.foreach(println(_))
spark.sparkContext.runJob(m__D_1, (iter: Iterator[_]) => {})
var end0 = System.currentTimeMillis() - start0
println("ShredQuery3SparkOpt,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
println("ShredQuery3SparkOpt,"+sf+","+Config.datapath+",0,unshredding,"+spark.sparkContext.applicationId)

}
f
 }
}