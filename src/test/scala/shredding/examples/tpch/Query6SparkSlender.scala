
package experiments
/** Generated **/
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._

case class RecordLine1(l_partkey: Int, l_orderkey: Int, l_quantity: Double)
case class RecordLine2(l_partkey: Int, l_quantity: Double)
case class RecordLine3(l_quantity: Double)
case class RecordPart1(p_partkey: Int, p_name: String)
case class RecordPart2(p_name: String)
case class RecordOrder1(o_orderkey: Int, o_custkey: Int, o_orderdate: String)
case class RecordOrder2(o_orderdate: String)
case class RecordCustomer1(c_custkey: Int, c_name: String)
case class RecordCustomter2(c_name: String)

case class Record253(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record255(o_orderdate: String, o_parts: List[Record253], uniqueId: Long) extends CaseClassRecord
case class Record300(c_name: String, p_name: String, month: String, t_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Query1Out(c_name: String, c_orders: List[Record255], uniqueId: Long) extends CaseClassRecord

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
   
   /**C.map(c => c.c_custkey -> c.c_name).joinSkewLeft(orders).map{
     case (_, (c_info, o_orderkey)) => o_orderkey -> c_info
   }.join(lineitem).map(_._2.swap)**/

   val result = S.map(s => s.s_suppkey -> s.s_name).cogroup(resultInner, new HashPartitioner(Config.minPartitions)).flatMap{
     case (_, (itV, itW)) => itV.map(v => (v, itW.toArray))
   }

   result
 }
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
