
package experiments
/** Generated **/
import org.apache.spark.SparkConf
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

object Query1SparkSlenderShredDS {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query1SparkSlenderDS"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()

val tpch = TPCHLoader(spark)
val C = tpch.loadCustomersDF
C.cache
C.count
val O = tpch.loadOrdersDF
O.cache
O.count
val L = tpch.loadLineitemDF
L.cache
L.count
val P = tpch.loadPartDF
P.cache
P.count
   
  var start0 = System.currentTimeMillis()
   val result_F = C.map(c => (c.c_name, c.c_custkey))
   val result_G1 = O.map( o => o.o_custkey -> (o.o_orderdate, o.o_orderkey)).groupByLabel()
   val partRDD = P.map(p => p.p_partkey -> p.p_name)
   val result_G2 = L.map(l => l.l_partkey -> (l.l_orderkey, l.l_quantity)).joinSkewLeft(partRDD).map{ case (_, ((l_orderkey, l_quantity), p_name)) => l_orderkey -> (p_name, l_quantity) }.groupByLabel()
   println("Query1SparkSlenderShredDF"+sf+","+Config.datapath+","+end3+","+spark.sparkContext.applicationId)
 }

}
