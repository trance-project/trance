
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

object Query1SparkSlender {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query1SparkSlender"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()

val tpch = TPCHLoader(spark)
val C = tpch.loadCustomers
//val C = tpch.loadCustomers().map{ case c => RecordCustomer1(c.c_custkey, c.c_name) }
C.cache
C.count
val O = tpch.loadOrders
//val O = tpch.loadOrders().map{ case o => RecordOrder1(o.o_orderkey, o.o_custkey, o.o_orderdate) }
O.cache
O.count
val L = tpch.loadLineitem
//val L = tpch.loadLineitem().map{ case l => RecordLine1(l.l_partkey, l.l_orderkey, l.l_quantity) }
L.cache
L.count
val P = tpch.loadPart
//val P = tpch.loadPart().map{ case p => RecordPart1(p.p_partkey, p.p_name) }
P.cache
P.count
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
   
  var start0 = System.currentTimeMillis()
    val l = L.map(l => l.l_partkey -> (l.l_orderkey, l.l_quantity))
    val p = P.map(p => p.p_partkey -> p.p_name)
    val lpj = l.joinSkewLeft(p)
    // this throws error: value withFilter is not a member of org.apache.spark.rdd.RDD[(Int, (Int, String))]
    //for ((_, ((l_orderkey, p_name)) <- lpj) yield (l_orderkey, p_name)
    val OrderParts = lpj.map{ case (_, ((l_orderkey, l_quantity), p_name)) => (l_orderkey, (p_name, l_quantity)) }.groupByKey()
    val o = O.map(o => o.o_orderkey -> (o.o_custkey, o.o_orderdate)).join(OrderParts)
    val CustomerOrders = o.map{ case (_, ((o_custkey, o_orderdate), parts)) => (o_custkey, (o_orderdate, parts)) }.groupByKey()
    val c = C.map(c => c.c_custkey -> c.c_name).join(CustomerOrders).map{ case (_, (c_name, orders)) => (c_name, orders) }
   c.count
   var end0 = System.currentTimeMillis() - start0
   println("Query1SparkSlender"+sf+","+Config.datapath+","+end0)
 }

}
