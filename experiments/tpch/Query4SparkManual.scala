
package experiments
/** 
This is manually written code.
**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._

object Query4SparkManual {
  def main(args: Array[String]){
    val sf = Config.datapath.split("/").last
    val conf = new SparkConf().setMaster(Config.master).setAppName("Query4SparkManual"+sf)
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

    val o = O.map(o => o.o_custkey -> (o.o_orderkey, o.o_orderdate))
    val c = C.map(c => c.c_custkey -> c.c_name).join(o).map{ case (_, (c_name, orders)) => (c_name, orders) }.groupByKey()
    c.cache
    c.count
    var start0 = System.currentTimeMillis()

    val l = L.map(l => l.l_partkey -> (l.l_orderkey, l.l_quantity))
    val p = P.map(p => p.p_partkey -> p.p_name)
    val lpj = l.joinSkewLeft(p).map{ case (_, ((lorderkey, lqty), pname)) => lorderkey -> (pname, lqty) }

    val custords = c.flatMap{ case (cname, orders) => 
                    orders.map{ case (orderkey, orderdate) => orderkey -> (cname, orderdate) } 
                  }.join(lpj)
                    .map{ case (_, ((cname, orderdate), (pname, qty))) => 
                      ((cname, orderdate, pname), qty)
                   }.reduceByKey(_ + _).map{
                      case ((cname, orderdate, pname), qty) => (cname, (orderdate, pname, qty))
                   }.groupByKey()  
    custords.count
    var end0 = System.currentTimeMillis() - start0
    println("Query4SparkManual"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
  }
}
