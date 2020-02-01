
package experiments
/** 
This is manually written code based on the experiments
provided from Slender.s
**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._

case class Record167(p_name: String, p_partkey: Int)

object Query1SparkManual {
  def main(args: Array[String]){
    val sf = Config.datapath.split("/").last
    val conf = new SparkConf().setMaster(Config.master).setAppName("Query1SparkManual"+sf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val tpch = TPCHLoader(spark)
    val C = tpch.loadCustomers
    C.cache
    spark.sparkContext.runJob(C, (iter: Iterator[_]) => {})
    val O = tpch.loadOrders
    O.cache
    spark.sparkContext.runJob(O, (iter: Iterator[_]) => {})
    val L = tpch.loadLineitem
    L.cache
    spark.sparkContext.runJob(L, (iter: Iterator[_]) => {})
    val P = tpch.loadPart
    P.cache
    spark.sparkContext.runJob(P, (iter: Iterator[_]) => {})
       
	tpch.triggerGC

    var start0 = System.currentTimeMillis()

    val l = L.map(l => l.l_partkey -> Record166(l.l_orderkey, l.l_quantity, l.l_partkey))
    val p = P.map(p => p.p_partkey -> Record167(p.p_name, p.p_partkey))
    val lpj = l.joinSkewLeft(p)

    val OrderParts = lpj.map{ case (_, ((l_orderkey, l_quantity), p_name)) => l_orderkey -> (p_name, l_quantity) }
    val CustomerOrders = O.map(o => o.o_orderkey -> (o.o_custkey, o.o_orderdate)).cogroup(OrderParts).flatMap{
		  case (_, (order, parts)) => order.map{ case (ock, od) => ock -> (od, parts.toArray) }
	  }

    val c = C.map(c => c.c_custkey -> c.c_name).cogroup(CustomerOrders).flatMap{ 
	  			case (_, (c_name, orders)) => c_name.map(c => (c, orders.toArray)) 
			}
    spark.sparkContext.runJob(c, (iter: Iterator[_]) => {})
    var end0 = System.currentTimeMillis() - start0
    println("Query1SparkManual"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
  }
}
