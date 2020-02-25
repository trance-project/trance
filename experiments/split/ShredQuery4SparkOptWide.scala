
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
import org.apache.spark.HashPartitioner
case class Record323(lbl: Unit)
case class Record324(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record325(p_name: String, p_partkey: Int)
case class Record326(l_orderkey: Int, p_partkey: Int, l_qty: Double)
case class Record328(c_name: String, c_orders: Int)
case class Record330(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record333(o_orderdate: String, o_parts: Int)
case class Record336(p_name: Int, l_qty: Double)
case class PartTrunc(p_partkey: Int, p_mfgr: String, p_brand: String, p_type: String, p_size: Int, p_container: String, p_comment: String)
case class RecordLP(p: PartTrunc, l_orderkey: Int, l_qty: Double)
case class Record318(p_retailprice: Double, p_name: String)
case class Record319(p: PartTrunc, l_qty: Double)

case class Record373(p: PartTrunc, p_name: String, _2: Double)
case class Record374(o_orderdate: String, o_parts: Iterable[Record373])
case class Record375(c_name: String, c_orders: Iterable[Record374])

object ShredQuery4SparkOptWide {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4SparkOptWide"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)

val L__F = 3
val L__D_1 = tpch.loadLineitem()
L__D_1.cache
spark.sparkContext.runJob(L__D_1, (iter: Iterator[_]) => {})
val P__F = 4
val P__D_1 = tpch.loadPart()
P__D_1.cache
spark.sparkContext.runJob(P__D_1, (iter: Iterator[_]) => {})
val C__F = 1
val C__D_1 = tpch.loadCustomers()
C__D_1.cache
spark.sparkContext.runJob(C__D_1, (iter: Iterator[_]) => {})
val O__F = 2
val O__D_1 = tpch.loadOrders()
O__D_1.cache
spark.sparkContext.runJob(O__D_1, (iter: Iterator[_]) => {})

tpch.triggerGC

val x219 = L__D_1
val x224 = P__D_1
val x225 = x224.map{ case x225 => ({val x227 = x225.p_partkey
x227}, PartTrunc(x225.p_partkey, x225.p_mfgr, x225.p_brand, x225.p_type, 
  x225.p_size, x225.p_container, x225.p_comment)) }
val x226 = x219.map{ case x225 => ({val x227 = x225.l_partkey 
x227}, LineitemProj(x225.l_orderkey, x225.l_partkey, x225.l_quantity)) }
val x227 = x226.joinDropKey(x225)
 
val x236 = x227.mapPartitions( it =>
  it.map{ case (x230, x231) => 
   val x232 = x230.l_orderkey 
val x233 = x231
val x234 = x230.l_quantity 
val x235 = RecordLP(x233, x232, x234) 
x235}, true) 

val M__D_1 = C__D_1

val c_orders__D_1 = O__D_1.mapPartitions(it =>
  it.map{ case x269 => (x269.o_custkey, x269) }
).groupByKey(new HashPartitioner(400))


val o_parts__D_1 = x236.mapPartitions(it =>
  it.map{ case x308 => 
  (x308.l_orderkey, x308)}).groupByKey(new HashPartitioner(1000))

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
val m__D_1 = query1__D_1
spark.sparkContext.runJob(m__D_1, (iter: Iterator[_]) => {})

val c_orders__D_1 = query1__D_2c_orders_1
spark.sparkContext.runJob(c_orders__D_1, (iter: Iterator[_]) => {})

val x223 = query1__D_2c_orders_2o_parts_1.mapPartitions( it =>
      it.map{ case (lbl, bag) => (lbl, bag.foldLeft(HashMap.empty[PartTrunc, Double].withDefaultValue(0))(
            (acc, p) => {acc(p.p) += p.l_qty; acc})) }, true)

val x284 = P__D_1.map(p => p.p_partkey -> Record318(p.p_retailprice,p.p_name))

val x224 = x223.flatMap{
  case (lbl, bag) => bag.map(p => p._1.p_partkey -> (lbl, Record319(p._1, p._2)))
}

val x290 = x224.joinDropKey(x284)

val x226 = x290.map{ 
  case ((lbl, tot), p) => ((lbl, tot.p, p.p_name), tot.l_qty*p.p_retailprice)
}

val x227 = x226.reduceByKey(_+_)

val x390 = x227.mapPartitions(it => 
  it.map{ case ((lbl, p, pname), tot) => (lbl, Record373(p, pname, tot))})

val o_parts__D_1 = x390.groupByKey()
//o_parts__D_1.collect.foreach(println(_))
spark.sparkContext.runJob(o_parts__D_1, (iter: Iterator[_]) => {})
var end0 = System.currentTimeMillis() - start0
println("ShredQuery4SparkOptWide,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)

var start1 = System.currentTimeMillis()
/**val x342 = c_orders__D_1.mapPartitions(
    it => it.flatMap{ case (lbl, bag) => bag.map(o => (o.o_orderkey, (lbl, o)))}, false
  ).cogroup(o_parts__D_1).flatMap{
    case (_, (left, x349)) => left.map{ case (lbl, date) => (lbl, (date, x349.flatten)) }
  }
val result = m__D_1.map(c => c.c_custkey -> c).cogroup(x342).flatMap{
  case (_, (left, x349)) => left.map(cname => cname -> x349)
}
spark.sparkContext.runJob(result, (iter: Iterator[_]) => {})**/
var end1 = System.currentTimeMillis() - start1
println("ShredQuery4SparkOptWide,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)

/**result.flatMap{
  case (cname, corders) =>
    if (corders.isEmpty) List((cname.c_name, null, null, null))
    else corders.flatMap{
      case (date, parts) => 
        if (parts.isEmpty) List((cname.c_name, date.o_orderdate, null, null))
        else parts.map(p => (cname.c_name, date.o_orderdate, p.p_name, p._2))
    }
}.sortBy(_._1).collect.foreach(println(_))**/


}
f
 }
}
