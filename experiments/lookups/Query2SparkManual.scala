
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
case class Record438(orderdate: String, partkey: Int, _2: Double)
case class Record439(c_name: String, totals: List[Record438])
object Query2SparkManual {
  def main(args: Array[String]){
    val sf = Config.datapath.split("/").last
    val conf = new SparkConf().setMaster(Config.master).setAppName("Query2SparkManual"+sf)
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
    val accum1 = (acc: List[Record438], v: Record438) => v match {
      case Record438(_, 0, 0.0) => acc
      case _ => acc :+ v 
    }
    val accum2 = (acc1: List[Record438], acc2: List[Record438]) => acc1 ++ acc2
    val result = c.zipWithIndex.flatMap{
      case (ctup, id1) => if (ctup.c_orders.isEmpty) List(((id1, ctup.c_name, null, null),0.0))
        else ctup.c_orders.zipWithIndex.flatMap{
        case (otup, id2) => if (otup.o_parts.isEmpty) List(((id1, ctup.c_name, otup.o_orderdate, null),0.0))
          else otup.o_parts.map{
            case ptup => (id1, ctup.c_name, otup.o_orderdate, ptup.p_partkey) -> ptup.l_qty
        }
      }
    }.reduceByKey(_+_).map{
      case ((id1, cname, date, pk), total) => (id1, cname) -> Record438(date, pk.asInstanceOf[Int], total)
    }.aggregateByKey(List.empty[Record438])(accum1, accum2).map{
      case ((id1, cname), totals) => Record439(cname, totals)
    }
    //result.collect.foreach(println(_))
    spark.sparkContext.runJob(result, (iter: Iterator[_]) => {})
    var end0 = System.currentTimeMillis() - start0
	  println("Query2SparkManual"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
  
    /**result.flatMap(r => if (r.totals.isEmpty) List((r.c_name, null, null, null))
      else r.totals.map(o => (r.c_name, o.orderdate, o.partkey, o._2))
    ).sortBy(_._1).collect.foreach(println(_))**/
  }
}
