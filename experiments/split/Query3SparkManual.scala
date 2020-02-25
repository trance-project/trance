
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
case class RecordLP(l: Lineitem, p: PartProj)
case class RecordOLP(o: Order, oparts: Iterable[RecordLP])
case class RecordCOLP(c: Customer, corders: Iterable[RecordOLP])
object Query3SparkManualWide {
  def main(args: Array[String]){
    val sf = Config.datapath.split("/").last
    val conf = new SparkConf().setMaster(Config.master).setAppName("Query3SparkManualWide"+sf)
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

    val l = L.map(l => l.l_partkey -> l)
    val p = P.map(p => p.p_partkey -> p)
    val lpj = l.join(p)

    val OrderParts = lpj.map{ case (k, (l, p)) => l.l_orderkey -> RecordLP(l, PartProj(p.p_partkey, p.p_name)) }
    val CustomerOrders = O.map(o => o.o_orderkey -> o).cogroup(OrderParts).flatMap{
      case (_, (orders, parts)) => orders.map( order => order.o_custkey -> RecordOLP(o, parts.toList)) 
    }

    val c = C.map(c => c.c_custkey -> c).cogroup(CustomerOrders).flatMap{
      case (_, (cnames, orders)) => cnames.map(c => RecordCOLP(c, orders.toList))
    }
    c.cache
    spark.sparkContext.runJob(c, (iter: Iterator[_]) => {})

    var start0 = System.currentTimeMillis()
    val result = c.zipWithIndex.flatMap{
      case (ctup, id1) => if (ctup.c_orders.isEmpty) List((-1, (id1, ctup, null)))
        else ctup.corders.zipWithIndex.flatMap{
        case (otup, id2) => if (otup.oparts.isEmpty) List((-1, (id1, ctup, null)))
          else otup.oparts.map(lp => (lp.p.p_partkey, (id1, ctup, lp)))
        }
    }.join(P.map(p => p.p_partkey -> p)).map{
      case (k, ((id1, cname, lp), p)) => (c, l.p) -> lp.l_qty*p.p_retailprice
    }.reduceByKey(_+_).map{
      case ((c, p), total) => (c, p, total)
    }
    result.collect.foreach(println(_))
    spark.sparkContext.runJob(result, (iter: Iterator[_]) => {})
    var end0 = System.currentTimeMillis() - start0
	  println("Query3SparkManualWide"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
  
  }
}
