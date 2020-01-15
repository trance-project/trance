
package experiments
/** 
This is manually written code based on the experiments
provided from Slender.s
**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._

object TPCHNested4FilterSparkManual {
  def main(args: Array[String]){
    val sf = Config.datapath.split("/").last
    val conf = new SparkConf().setMaster(Config.master).setAppName("TPCHNested4FilterSparkManual"+sf)
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

    val OrderParts = lpj.map{ case (_, ((l_orderkey, l_quantity), p_name)) => (l_orderkey, (p_name, l_quantity)) }
    val CustomerOrders = O.map(o => o.o_orderkey -> (o.o_custkey, o.o_orderdate)).cogroup(OrderParts).flatMap{
      case (ok, (order, parts)) => order.map{ case (ock, od) => ock -> (ok, od, parts.toArray) }
    }
    val c = C.map(c => c.c_custkey -> c.c_name).cogroup(CustomerOrders).flatMap{ 
      case (c_custkey, (c_name, orders)) => c_name.map(c => (c_custkey, c, orders.toArray))
    }
    c.cache
    c.count
    var start0 = System.currentTimeMillis()
	  val custords = C.map{ c => 
      c.c_custkey -> c.c_name }.join(O.map(o => o.o_custkey -> o.o_orderdate)).map{
      case (_, (cname, date)) => (cname, date) -> 1
    }
    val result = c.filter(_._1 <= 1500000).flatMap{ 
      case (ck, cname, orders) => orders.filter(_._1 <= 150000000).flatMap{ case (ok, date, parts) => 
        parts.map{ case (part, qty) => part -> (cname, date, qty)}}
    }.join(P.map(p => p.p_name -> p.p_retailprice)).map{
      case (pname, ((cname, date, qty), price)) => (cname, date, pname) -> qty*price
    }.reduceByKey(_ + _).map{
      case ((cname, date, pname), total) =>  (cname, date) -> (pname, total)
    }.cogroup(custords).map{
      case ((cname, date), (bag, _)) => cname -> (date, bag)
    }.cogroup(C.map{c => c.c_name -> 1}).map{
      case (cname, (bag, _)) => cname -> bag
    }
    result.count
	var end0 = System.currentTimeMillis() - start0
    println("TPCHNested4FilterSparkManual"+sf+","+Config.datapath+","+end0)
  }
}
