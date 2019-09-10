
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

object Query4SparkSlenderShred {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query1SparkSlender"+sf)
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
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
   
  //var start0 = System.currentTimeMillis()
   val result_F = C.map(c => (c.c_name, c.c_custkey))
   result_F.cache
   result_F.count
   //var end0 = System.currentTimeMillis() - start0
   //var start1 = System.currentTimeMillis()
   val result_G1 = O.map( o => o.o_custkey -> (o.o_orderdate, o.o_orderkey)).groupByLabel()
   result_G1.cache
   result_G1.count
   //var end1 = System.currentTimeMillis() - start1
   //var start2 = System.currentTimeMillis()
   val partRDD = P.map(p => p.p_partkey -> p.p_name)
   val result_G2 = L.map(l => l.l_partkey -> (l.l_orderkey, l.l_quantity)).joinSkewLeft(partRDD).map{ case (_, ((l_orderkey, l_quantity), p_name)) => l_orderkey -> (p_name, l_quantity) }.groupByLabel()
   result_G2.cache
   result_G2.count//.collect.foreach(println(_))
   //var end2 = System.currentTimeMillis() - start2
   //var end3 = System.currentTimeMillis() - start2
   //println("Query1SparkSlenderShred"+sf+","+Config.datapath+",level1,"+end0)
   //println("Query1SparkSlenderShred"+sf+","+Config.datapath+",level2,"+end1)
   //println("Query1SparkSlenderShred"+sf+","+Config.datapath+",level3,"+end2)
  var start0 = System.currentTimeMillis() 
  val aux0 = result_G1.flatMap{ case (o_custkey, o_infoSeq) => o_infoSeq.map{ case (o_orderdate, o_orderkey) => 
                  o_orderkey -> (o_custkey, o_orderdate) } }.join(result_G2).flatMap{
                    case (_, ((o_custkey, o_orderdate), l_infoSeq)) => l_infoSeq.map{
                      case (p_name, l_quantity) => (o_custkey, p_name, o_orderdate) -> l_quantity
                    }
                  }
  //aux0.cache
  //aux0.count
  val aux = aux0.reduceByKey(_ + _).map{ case ((o_custkey, p_name, o_orderdate), l_quantity) =>
              o_custkey -> (p_name, o_orderdate, l_quantity) }
  //aux.cache
  //aux.count
  val result = aux.joinSkewLeft(result_F.map(_.swap)).map{ case (_, ((p_name, o_orderdate, l_quantity), c_name)) => (c_name, p_name, o_orderdate) -> l_quantity }
  //result.cache
  result.count
  //result.collect.foreach(println(_))
  var end0 = System.currentTimeMillis() - start0
   println("Query4SparkSlenderShred"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }

}
