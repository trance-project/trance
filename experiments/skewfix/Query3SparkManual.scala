
package experiments
/** 
This is manually written code based on the experiments
provided from Slender.s
**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._

object Query3SparkManual {
  def main(args: Array[String]){
    val sf = Config.datapath.split("/").last
    val conf = new SparkConf().setMaster(Config.master).setAppName("Query3SparkManual"+sf)
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

    val l = L.map(l => l.l_partkey -> (l.l_orderkey, l.l_quantity))
    val p = P.map(p => p.p_partkey -> p.p_name)
    val lpj = l.joinSkewLeft(p)

    val OrderParts = lpj.map{ case (_, ((l_orderkey, l_quantity), p_name)) => l_orderkey -> (p_name, l_quantity) }
    val CustomerOrders = O.map(o => o.o_orderkey -> (o.o_custkey, o.o_orderdate)).cogroup(OrderParts).flatMap{
      case (_, (order, parts)) => order.map{ case (ock, od) => ock -> (od, parts.toArray) }
    }

    val c = C.map(c => c.c_custkey -> c.c_name).cogroup(CustomerOrders).flatMap{
      case (_, (c_name, orders)) => c_name.map(c => (c, orders.toArray))
    }
    c.cache
    spark.sparkContext.runJob(c, (iter: Iterator[_]) => {})

    var start0 = System.currentTimeMillis()
	  val result = c.flatMap{ 
      case (cname, orders) => orders.flatMap{ case (date, parts) => 
        parts.map{ case (part, qty) => part -> (cname, qty)}}
    }.joinSkewLeft(P.map(p => p.p_name -> p.p_retailprice)).map{
      case (pname, ((cname, qty), price)) => (cname, pname) -> qty*price
    }.reduceByKey(_ + _)
    spark.sparkContext.runJob(result, (iter: Iterator[_]) => {})
	  var end0 = System.currentTimeMillis() - start0
    println("Query3SparkManual"+sf+","+Config.datapath+","+end0)
  }
}
