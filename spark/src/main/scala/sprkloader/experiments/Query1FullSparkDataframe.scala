
package sprkloader.experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.HashMap
import sprkloader._
import sprkloader.PairRDDOperations._
import sprkloader.DictRDDOperations._
import sprkloader.TopRDD._
import org.apache.spark.sql._

object Query1FullSparkDataframe extends App {
 override def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query1FullSparkDataframe"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
   import spark.implicits._
val L = tpch.loadLineitemDF()
L.cache
L.count
// val IBag_P__D = tpch.loadPartDF()
// IBag_P__D.cache
// IBag_P__D.count
val C = tpch.loadCustomersDF()
C.cache
C.count

val O = tpch.loadOrdersDF()
O.cache
O.count
implicit val ncodec = Encoders.product[TmpC]
implicit val ncode1 = Encoders.product[TmpO]

    def f = {
 
var start1 = System.currentTimeMillis()

val dict2 = O.groupByKey(x => x.o_orderkey)
val dict1 = L.groupByKey(x => x.l_orderkey)
val dict3 = dict2.cogroup(dict1)( 
  (key, orders, lines) => orders.map(o => TmpO(o.o_shippriority, o.o_orderdate, o.o_custkey,
   o.o_orderpriority, o.o_clerk, o.o_orderstatus, o.o_totalprice, o.o_orderkey, o.o_comment, lines.toSeq))
  ).groupByKey(x => x.o_custkey)

val result = C.groupByKey(x => x.c_custkey).cogroup(dict3)(
  (key, custs, orders) => custs.map(c => TmpC(c.c_acctbal, c.c_name, c.c_nationkey, c.c_custkey, 
    c.c_comment, c.c_address, c.c_mktsegment, c.c_phone, orders.toSeq)))

result.count

var end1 = System.currentTimeMillis() - start1
println("Flat++,Standard,Query1,"+sf+","+Config.datapath+","+end1+",query,"+spark.sparkContext.applicationId)
  
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("Shred,Standard,Query1,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
