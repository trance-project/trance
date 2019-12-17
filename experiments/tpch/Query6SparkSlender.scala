
package experiments
/** 
Manual code from Slender experiments
**/
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._

object Query6SparkSlender {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query6SparkSlender"+sf)
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
   
  val Q2 = {    
   val lineitem = L.map(l => l.l_orderkey -> l.l_suppkey)
   val orders = O.map(o => o.o_orderkey -> o.o_custkey) //o.o_custkey -> o.o_orderkey)
   val customers = C.map(c => c.c_custkey -> c.c_name)
   val resultInner = lineitem.join(orders).map{
     case (_, (l_suppkey, o_custkey)) => o_custkey -> l_suppkey
   }.joinSkewLeft(customers).map(_._2)

   S.map(s => s.s_suppkey -> s.s_name).cogroup(resultInner).flatMap{
     case (_, (itV, itW)) => itV.map(v => (v, itW.toArray))
   }

 }
 Q2.cache
 Q2.count
 var start0 = System.currentTimeMillis()
 val customers = C.map(c => c.c_name -> 1)
 val result = Q2.flatMap{
    case (s_name, customers2) => customers2.map{ c_name => c_name -> s_name }
  }.cogroup(customers).map{ case (c_name, (s_nameSet, _)) => c_name -> s_nameSet.toArray}
 result.count
 var end0 = System.currentTimeMillis() - start0
 println("Query6SparkSlender"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }

}
