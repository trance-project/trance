
package experiments
/** 
This is manually written code based on the experiments
provided from Slender.s
**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._

object Query1FilterSparkSlender {
  def main(args: Array[String]){
    val sf = Config.datapath.split("/").last
    val conf = new SparkConf().setMaster(Config.master).setAppName("Query1FilterSparkSlender"+sf)
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
       
    var start0 = System.currentTimeMillis()

    val l = L.map(l => l.l_partkey -> (l.l_orderkey, l.l_quantity))
    val p = P.map(p => p.p_partkey -> p.p_name)
    val lpj = l.joinSkewLeft(p)

    val OrderParts = lpj.map{ case (_, ((l_orderkey, l_quantity), p_name)) => l_orderkey -> (p_name, l_quantity) }
    val CustomerOrders = O.map(o => o.o_orderkey -> (o.o_custkey, o.o_orderdate))/**.filter(_._1 <= 150000000)**/.cogroup(OrderParts).flatMap{
		case (_, (order, parts)) => order.map{ case (ock, od) => ock -> (od, parts.toArray) }
	}

    val c = C.map(c => c.c_custkey -> c.c_name)/*8.filter(_._1 <= 1500000)**/.cogroup(CustomerOrders).flatMap{ 
	  			case (_, (c_name, orders)) => c_name.map(c => (c, orders.toArray)) 
			}
    c.count
    var end0 = System.currentTimeMillis() - start0
    println("Query1FilterSparkSlender"+sf+","+Config.datapath+","+end0)
  }
}
