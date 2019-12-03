
package experiments
/** 
Manual code from Slender experiments
**/
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._

object Query2SparkManual {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query2SparkManual"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()

val tpch = TPCHLoader(spark)
val C = tpch.loadCustomers
C.cache
C.count
val O = tpch.loadOrders
O.cache
O.count
val L = tpch.loadLineitem
L.cache
L.count
val P = tpch.loadPart
P.cache
P.count
val S = tpch.loadSupplier
S.cache
S.count
 
  var start0 = System.currentTimeMillis()
  
  val lineitem = L.map(l => l.l_orderkey -> l.l_suppkey)
  val orders = O.map(o => o.o_custkey -> o.o_orderkey) //o.o_custkey -> o.o_orderkey)
  val customers = C.map(c => c.c_custkey -> c.c_name)
  /**val resultInner = lineitem.join(orders).map{
     case (_, (l_suppkey, o_custkey)) => o_custkey -> l_suppkey
  }.joinSkewLeft(customers).map(_._2)**/
  val resultInner = orders.joinSkewLeft(customers).map{
	case (_, (o_orderkey, c_info)) => o_orderkey -> c_info
  }.join(lineitem).map(_._2.swap)

  val result = S.map(s => s.s_suppkey -> s.s_name)
  //.join(resultInner).map{
  //  case (_, (sname, cname)) => sname -> cname
  //}.groupByKey()
  .cogroup(resultInner).flatMap{
     case (_, (itV, itW)) => itV.map(v => (v, itW.toArray))
  }
  result.count
  var end0 = System.currentTimeMillis() - start0
  println("Query2SparkManual"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }

}
