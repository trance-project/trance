
package experiments
/** 
This is manually written code.
**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record159(lbl: Unit)
case class Record160(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record161(p_name: String, p_partkey: Int)
case class Record162(l_orderkey: Int, p_partkey: Int, l_qty: Double)
case class Record163(c__Fc_custkey: Int)
case class Record164(c_name: String, c_orders: Record163)
case class Record165(lbl: Record163)
case class Record166(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record168(o__Fo_orderkey: Int)
case class Record169(o_orderdate: String, o_parts: Record168)
case class Record170(lbl: Record168)
case class Record172(p_partkey: Int, l_qty: Double)
case class Record311(c2__Fc_orders: Record163)
case class Record312(c_name: String, c_orders: Int)
case class Record319(o_orderdate: String, o_parts: List[Record172])
case class Record321(c_name: String, c_orders: List[Record319])
case class Record416(orderdate: String, partkey: Int)
case class Record438(c_name: String, pname: String, totals: Double)
case class Record318(p_retailprice: Double, p_name: String)
object Query3SparkManual {
  def main(args: Array[String]){
    val sf = Config.datapath.split("/").last
    val conf = new SparkConf().setMaster(Config.master).setAppName("Query3SparkManual"+sf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val tpch = TPCHLoader(spark)
    val C = tpch.loadCustomersProj
    C.cache
    spark.sparkContext.runJob(C, (iter: Iterator[_]) => {})
    val O = tpch.loadOrdersProjBzip
    O.cache
    spark.sparkContext.runJob(O, (iter: Iterator[_]) => {})
    val L = tpch.loadLineitemProjBzip
    L.cache
    spark.sparkContext.runJob(L, (iter: Iterator[_]) => {})
    val P = tpch.loadPartProj4
    P.cache
    spark.sparkContext.runJob(P, (iter: Iterator[_]) => {})

    val l = L.map(l => l.l_partkey -> Record160(l.l_orderkey, l.l_quantity, l.l_partkey))
    val p = P.map(p => p.p_partkey -> Record161(p.p_name, p.p_partkey))
    val lpj = l.joinSkew(p)

    val OrderParts = lpj.map{ case (l, p) => 
      l.l_orderkey -> Record172(p.p_partkey, l.l_quantity) }
    val CustomerOrders = O.map(o => o.o_orderkey -> Record166(o.o_orderdate, o.o_orderkey, o.o_custkey)).cogroup(OrderParts).flatMap{
      case (_, (orders, parts)) => orders.map( order => order.o_custkey -> Record319(order.o_orderdate, parts.toList)) 
    }

    val c = C.map(c => c.c_custkey -> Record312(c.c_name, c.c_custkey)).cogroup(CustomerOrders).flatMap{
      case (_, (cnames, orders)) => cnames.map(c => Record321(c.c_name, orders.toList))
    }
    c.cache
    spark.sparkContext.runJob(c, (iter: Iterator[_]) => {})

    var start0 = System.currentTimeMillis()
    val result = c.zipWithIndex.flatMap{
      case (ctup, id1) => if (ctup.c_orders.isEmpty) List((-1, (id1, ctup.c_name, 0.0)))
        else ctup.c_orders.zipWithIndex.flatMap{
        case (otup, id2) => if (otup.o_parts.isEmpty) List((-1, (id1, ctup.c_name, 0.0)))
          else otup.o_parts.map{
            case ptup => ptup.p_partkey -> (id1, ctup.c_name, ptup.l_qty)
        }
      }
    }.joinSkew(P.map(p => p.p_partkey -> Record318(p.p_retailprice, p.p_name))).map{
      case ((id1, cname, qty), p) => (cname, p.p_name) -> qty*p.p_retailprice
    }.reduceByKey(_+_).map{
      case ((cname, pname), total) => Record438(cname, pname.asInstanceOf[String], total)
    }
    //result.collect.foreach(println(_))
    spark.sparkContext.runJob(result, (iter: Iterator[_]) => {})
    var end0 = System.currentTimeMillis() - start0
	  println("Query3SparkManual"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
  
  }
}
