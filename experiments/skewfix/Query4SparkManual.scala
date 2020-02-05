
package experiments
/** 
This is manually written code based on the experiments
provided from Slender.
**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record159(lbl: Unit)
case class Record160(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record161(p_name: String, p_partkey: Int)
case class Record162(l_orderkey: Int, p_name: String, l_qty: Double)
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
case class Record313(lbl: Record311)
case class Record315(o2__Fo_parts: Record168)
case class Record316(o_orderdate: String, o_parts: Record315)
case class Record317(lbl: Record315)
case class Record318(p_retailprice: Double, p_name: String)
case class Record320(p_name: String)
case class Record373(p_name: String, _2: Double)
case class Record319(o_orderdate: String, o_parts: Array[Record172])
case class Record321(c_name: String, c_orders: Array[Record319])
case class Record374(o_orderdate: String, o_parts: Iterable[Record373])
case class Record375(c_name: String, c_orders: Iterable[Record374])
case class Record376(id1: Long, c_name: String, id2: Int, o_orderdate: String, l_qty: Double)
case class Record377(id1: Long, c_name: String, id2: Int, o_orderdate: String, p_name: String)
case class Record378(id1: Long, c_name: String, id2: Int, o_orderdate: String)
case class Record379(id1: Long, c_name: String)

object Query4SparkManual {
  def main(args: Array[String]){
    val sf = Config.datapath.split("/").last
    val conf = new SparkConf().setMaster(Config.master).setAppName("Query4SparkManual"+sf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val tpch = TPCHLoader(spark)
    val C = tpch.loadCustomersProj
    C.cache
    spark.sparkContext.runJob(C,  (iter: Iterator[_]) => {})
    val O = tpch.loadOrdersProj
    O.cache
    spark.sparkContext.runJob(O,  (iter: Iterator[_]) => {})
    val L = tpch.loadLineitemProjBzip
    L.cache
    spark.sparkContext.runJob(L,  (iter: Iterator[_]) => {})
    val P = tpch.loadPartProj
    P.cache
    spark.sparkContext.runJob(P,  (iter: Iterator[_]) => {})
    
    val l = L.map(l => l.l_partkey -> Record160(l.l_orderkey, l.l_quantity, l.l_partkey))
    val p = P.map(p => p.p_partkey -> Record161(p.p_name, p.p_partkey))
    val lpj = l.joinSkewLeft(p)

    val OrderParts = lpj.map{ case (l, p) => l.l_orderkey -> Record172(p.p_partkey, l.l_quantity) }
    val CustomerOrders = O.map(o => o.o_orderkey -> Record166(o.o_orderdate, o.o_orderkey, o.o_custkey)).cogroup(OrderParts).flatMap{
      case (_, (orders, parts)) => orders.map(order => order.o_custkey -> Record319(order.o_orderdate, parts.toArray))
    }

    val c = C.map(c => c.c_custkey -> Record312(c.c_name, c.c_custkey)).cogroup(CustomerOrders).flatMap{
          case (_, (cnames, orders)) => cnames.map(c => Record321(c.c_name, orders.toArray))
      }

	  c.cache
	  spark.sparkContext.runJob(c,  (iter: Iterator[_]) => {})

	P.unpersist()
    val P4 = tpch.loadPartProj4
    P4.cache
    spark.sparkContext.runJob(P4,  (iter: Iterator[_]) => {})

	tpch.triggerGC

    var start0 = System.currentTimeMillis()
    val result = c.zipWithIndex.flatMap{
      case (ctup, id1) => ctup.c_orders.zipWithIndex.flatMap{
        case (otup, id2) => otup.o_parts.map{
          case ptup => ptup.p_partkey -> Record376(id1, ctup.c_name, id2, otup.o_orderdate, ptup.l_qty) 
        }
      }
    }.joinSkewLeft(P4.map(p => p.p_partkey -> Record318(p.p_retailprice, p.p_name))).map{
      case (flat, part) => Record377(flat.id1, flat.c_name, flat.id2, flat.o_orderdate, part.p_name) -> flat.l_qty*part.p_retailprice
    }.reduceByKey(_ + _).map{
      case (key, total) => Record378(key.id1, key.c_name, key.id2, key.o_orderdate) -> Record373(key.p_name, total)
    }.groupByKey().map{
		  case (key, bag) => Record379(key.id1, key.c_name) -> Record374(key.o_orderdate, bag)
	  }.groupByKey().map{
		  case (key, bag) => Record375(key.c_name, bag)
	  }
    spark.sparkContext.runJob(result,  (iter: Iterator[_]) => {})
  	var end0 = System.currentTimeMillis() - start0
    println("Query4SparkManual,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
  }
}
