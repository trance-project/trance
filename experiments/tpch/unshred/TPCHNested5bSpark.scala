
package experiments
/** 
This is manually written code.
**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._

object TPCHNested5bSpark {
  def main(args: Array[String]){
    val sf = Config.datapath.split("/").last
    val conf = new SparkConf().setMaster(Config.master).setAppName("TPCHNested5bSpark"+sf)
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

    val l = L.map(l => l.l_partkey -> (l.l_orderkey, l.l_quantity))
    val p = P.map(p => p.p_partkey -> p.p_name)
    val lpj = l.joinSkewLeft(p)

    val OrderParts = lpj.map{ case (_, ((l_orderkey, l_quantity), p_name)) => (l_orderkey, (p_name, l_quantity)) }.groupByKey()
    val o = O.map(o => o.o_orderkey -> (o.o_custkey, o.o_orderdate)).join(OrderParts)

    val CustomerOrders = o.map{ case (_, ((o_custkey, o_orderdate), parts)) => (o_custkey, (o_orderdate, parts)) }.groupByKey()
    val c = C.map(c => c.c_custkey -> c.c_name).join(CustomerOrders).map{ case (_, (c_name, orders)) => (c_name, orders) }
    c.cache
    c.count

    var start0 = System.currentTimeMillis()

    // w/o dedup
    val custords = c.flatMap{ case (cname, orders) => 
                    orders.map{ case (orderdate, parts) => (orderdate, cname) }
                   }.cogroup(O.map{ o => o.o_orderdate -> 1}).map{
                     case (orderdate, (_, names)) => orderdate -> names.toArray
                   }
    // w/ dedup
    /**val custords = c.flatMap{ case (cname, orders) => 
                    orders.flatMap{ case (orderdate, parts) => (orderdate, cname)
                   }.join(O.map{ o => o.o_orderdate -> 1}).map{
                     case (orderdate, (_, cname)) => orderdate -> c_name
                   }.groupByKey()**/
    custords.count
    var end0 = System.currentTimeMillis() - start0
    println("TPCHNested5bSpark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
  }
}
