
package experiments
/** 
This is manually written code based on the experiments
provided from Slender.s
**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._

object TPCHNested4SparkManual {
  def main(args: Array[String]){
    val sf = Config.datapath.split("/").last
    val conf = new SparkConf().setMaster(Config.master).setAppName("TPCHNested4SparkManual"+sf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val tpch = TPCHLoader(spark)
    val C = tpch.loadCustomers
    C.cache
    spark.sparkContext.runJob(C,  (iter: Iterator[_]) => {})
    val O = tpch.loadOrders
    O.cache
    spark.sparkContext.runJob(O,  (iter: Iterator[_]) => {})
    val L = tpch.loadLineitem
    L.cache
    spark.sparkContext.runJob(L,  (iter: Iterator[_]) => {})
    val P = tpch.loadPart
    P.cache
    spark.sparkContext.runJob(P,  (iter: Iterator[_]) => {})
       

    val l = L.map(l => l.l_partkey -> (l.l_orderkey, l.l_quantity))
    val p = P.map(p => p.p_partkey -> p.p_name)
    val lpj = l.joinSkewLeft(p)

    val OrderParts = lpj.map{ case (_, ((l_orderkey, l_quantity), p_name)) => (l_orderkey, (p_name, l_quantity)) }.groupByKey()
    val o = O.map(o => o.o_orderkey -> (o.o_custkey, o.o_orderdate)).join(OrderParts)

    val CustomerOrders = o.map{ case (_, ((o_custkey, o_orderdate), parts)) => (o_custkey, (o_orderdate, parts)) }.groupByKey()
    val c = C.map(c => c.c_custkey -> c.c_name).join(CustomerOrders).map{ case (_, (c_name, orders)) => (c_name, orders) }
    c.cache
    spark.sparkContext.runJob(c,  (iter: Iterator[_]) => {})
    var start0 = System.currentTimeMillis()
    val custords = c.flatMap{ case (cname, orders) => orders.flatMap{
        case (odate, parts) => parts.map{
          case (pname, lqty) => (cname, odate, pname) -> 1
        }
      }
    }
    val custdate = c.flatMap{ case (cname, orders) => orders.map{
        case (odate, parts) => (cname, odate) -> 1
      }
    }
    val result = c.flatMap{ 
      case (cname, orders) => orders.flatMap{ case (date, parts) => 
        parts.map{ case (part, qty) => part -> (cname, date, qty)}}
    }.joinSkewLeft(P.map(p => p.p_name -> p.p_retailprice)).map{
      case (pname, ((cname, date, qty), price)) => (cname, date, pname) -> qty*price
    }.reduceByKey(_ + _).cogroup(custords).map{
      case ((cname, date, pname), (bag, _)) => (cname, date) -> bag.map{ t => (pname, t) } 
    }.cogroup(custdate).map{
      case ((cname, date), (bag, _)) =>  cname -> bag.map{ p => (date, p) }
    }
    spark.sparkContext.runJob(result,  (iter: Iterator[_]) => {})
  	var end0 = System.currentTimeMillis() - start0
    println("TPCHNested4SparkManual"+sf+","+Config.datapath+","+end0)
  }
}
