
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
case class Record329(lbl: Int)
case class Record330(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record333(o_orderdate: String, o_parts: Int)
case class Record334(lbl: Int)
case class Record336(p_partkey: Int, l_qty: Double)
case class Record312(c_name: String, c_orders: Int)
case class Record313(lbl: Int)
case class Record316(o_orderdate: String, o_parts: Int)
case class Record317(lbl: Int)
case class Record318(p_retailprice: Double, p_name: String)
case class Record320(p_name: String)
case class Record373(p_name: String, _2: Double)
case class Record374(o_orderdate: String, o_parts: Iterable[Record373])
case class Record375(c_name: String, c_orders: Iterable[Record374])
object ShredQuery4SparkSplit {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4SparkSplit"+sf)
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
val x242 = x241 
val x243 = Record328(x240, x242) 
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
x266}, x263) }
val (x267_L, x267_H, hkeys1) = x264.joinSplit(x255, (l: Int) => l)

// don't even need this mapping...
val x277_L = x267_L.mapPartitions(it =>
  it.map{ case (x269, x268) => 
   ({val x270 = (x268) 
x270}, {val x271 = x269.o_orderdate 
val x272 = x269.o_orderkey 
val x273 = (x272) 
val x274 = Record333(x271, x273) 
x274})
}, true)
val x277_H = x267_H.mapPartitions( it =>
  it.map{ case (x269, x268) => 
   ({val x270 = (x268) 
x270}, {val x271 = x269.o_orderdate 
val x272 = x269.o_orderkey 
val x273 = (x272) 
val x274 = Record333(x271, x273) 
x274})
}, true)

// we know the heavy ones from the lookup...
val (c_orders__D_1_L, c_orders__D_1_H) = x277_L.groupBySplit(x277_H, hkeys1)

val x285_L = c_orders__D_1_L.createDomain(l => l.o_parts)
val x285_H = c_orders__D_1_H.createDomain(l => l.o_parts)

// partitioning information lost 
val x303_L = x236_L.mapPartitions( it =>
  it.map{ case x302 => ({val x305 = x302.l_orderkey 
x305}, x302) })
val x303_H = x236_H.mapPartitions( it =>
  it.map{ case x302 => ({val x305 = x302.l_orderkey 
x305}, x302) })

// need a case for this
val (x304_L, x304_H, hkeys2) = (x303_L.unionPartitions(x303_H)).joinSplit(x285_L.unionPartitions(x285_H), (l: Int) => l)

val x315_L = x304_L.mapPartitions(it =>
  it.map{ case (x308, x307) => 
  ({val x309 = (x307) 
x309}, {val x310 = x308.p_partkey 
val x311 = x308.l_qty 
val x312 = Record336(x310, x311) 
x312})
}, true)

val x315_H = x304_H.mapPartitions(it =>
  it.map{ case (x308, x307) =>
  ({val x309 = (x307)
x309}, {val x310 = x308.p_partkey
val x311 = x308.l_qty
val x312 = Record336(x310, x311)
x312})
}, true)

val (o_parts__D_1_L, o_parts__D_1_H) = x315_L.groupBySplit(x315_H, hkeys2)

//o_parts__D_1.collect.foreach(println(_))
val (query1__D_1_L, query1__D_1_H) = M__D_1.splitDict()
query1__D_1_L.cache
query1__D_1_H.cache
spark.sparkContext.runJob(query1__D_1_L, (iter: Iterator[_]) => {})
spark.sparkContext.runJob(query1__D_1_H, (iter: Iterator[_]) => {})

val query1__D_2c_orders_1_L = c_orders__D_1_L
val query1__D_2c_orders_1_H = c_orders__D_1_H
query1__D_2c_orders_1_L.cache
query1__D_2c_orders_1_H.cache
spark.sparkContext.runJob(query1__D_2c_orders_1_L, (iter: Iterator[_]) => {})
spark.sparkContext.runJob(query1__D_2c_orders_1_H, (iter: Iterator[_]) => {})

val query1__D_2c_orders_2o_parts_1_L = o_parts__D_1_L
val query1__D_2c_orders_2o_parts_1_H = o_parts__D_1_H
query1__D_2c_orders_2o_parts_1_L.cache
query1__D_2c_orders_2o_parts_1_H.cache
spark.sparkContext.runJob(query1__D_2c_orders_2o_parts_1_L, (iter: Iterator[_]) => {})
spark.sparkContext.runJob(query1__D_2c_orders_2o_parts_1_H, (iter: Iterator[_]) => {})

 def f = {
 
var start0 = System.currentTimeMillis()
val x227 = query1__D_1_L
val m__D_1 = x227

val x235 = m__D_1.createDomain(l => l.c_orders)
val c_orders_ctx1 = x235

val (c_orders__D_1_L, c_orders__D_1_H) = query1__D_2c_orders_1_L.lookupSplit(query1__D_2c_orders_1_H, 
  c_orders_ctx1, (l: Int) => l, hkeys1)

val o_parts_ctx1_L = c_orders__D_1_L.createDomain(l => l.o_parts)
val o_parts_ctx1_H = c_orders__D_1_H.createDomain(l => l.o_parts)


val (x277_L, x277_H) = query1__D_2c_orders_2o_parts_1_L.lookupSplit(query1__D_2c_orders_2o_parts_1_H, 
  o_parts_ctx1_L.unionPartitions(o_parts_ctx1_H), (l: Int) => l, hkeys2)

val x223_L = x277_L.mapPartitions( it =>
      it.map{ case (lbl, bag) => (lbl, bag.foldLeft(HashMap.empty[Int, Double].withDefaultValue(0))(
            (acc, p) => {acc(p.p_partkey) += p.l_qty; acc})) }, true)

val x223_H = x277_H.mapPartitions( it =>
      it.map{ case (lbl, bag) => (lbl, bag.foldLeft(HashMap.empty[Int, Double].withDefaultValue(0))(
            (acc, p) => {acc(p.p_partkey) += p.l_qty; acc})) }, true)

val x284 = P__D_1.map(p => p.p_partkey -> p)

val x224_L = x223_L.flatMap{
  case (lbl, bag) => bag.map(p => p._1 -> (lbl, p._2))
}

val x224_H = x223_H.flatMap{
  case (lbl, bag) => bag.map(p => p._1 -> (lbl, p._2))
}

val (x290_L, x290_H, hkeys3) = x224_L.joinSplit(x224_H, x284)

val x226_L = x290_L.map{ 
  case ((lbl, tot), p) => ((lbl, p.p_name), tot*p.p_retailprice)
}
val x226_H = x290_H.map{
  case ((lbl, tot), p) => ((lbl, p.p_name), tot*p.p_retailprice)
}

val (x227_L, x227_H) = x226_L.reduceBySplit(x226_H, _+_)

val x390_L = x227_L.mapPartitions(it => 
  it.map{ case ((lbl, pname), tot) => (lbl, (pname, tot))})

val x390_H = x227_H.mapPartitions(it => 
  it.map{ case ((lbl, pname), tot) => (lbl, (pname, tot))})

val (o_parts__D_1_L, o_parts__D_1_H, hkeys4) = x390_L.groupBySplit(x390_H)
//o_parts__D_1.collect.foreach(println(_))
spark.sparkContext.runJob(o_parts__D_1_L, (iter: Iterator[_]) => {})
spark.sparkContext.runJob(o_parts__D_1_H, (iter: Iterator[_]) => {})
var end0 = System.currentTimeMillis() - start0
println("ShredQuery4SparkSplit,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)

var start1 = System.currentTimeMillis()
/**val x342 = c_orders__D_1_L.unionPartitions(c_orders__D_1_H).mapPartitions(
    it => it.flatMap(v => v._2.map(o => (o.o_parts, (v._1, o.o_orderdate)))), false
  ).cogroup(o_parts__D_1_L.unionPartitions(o_parts__D_1_H)).flatMap{
    case (_, (left, x349)) => left.map{ case (lbl, date) => (lbl, (date, x349.flatten)) }
  }
val result = m__D_1.map(c => c.c_orders -> c.c_name).cogroup(x342).flatMap{
  case (_, (left, x349)) => left.map(cname => cname -> x349)
}
//result.collect.foreach(println(_))
spark.sparkContext.runJob(result, (iter: Iterator[_]) => {})**/
var end1 = System.currentTimeMillis() - start1
println("ShredQuery4SparkSplit,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)

/**result.flatMap{
  case (cname, corders) =>
    if (corders.isEmpty) List((cname, null, null, null))
    else corders.flatMap{
      case (date, parts) => 
        if (parts.isEmpty) List((cname, date, null, null))
        else parts.map(p => (cname, date, p._1, p._2))
    }
}.sortBy(_._1).collect.foreach(println(_))**/


}
f
 }
}
