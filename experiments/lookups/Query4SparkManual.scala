
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
    val C = tpch.loadCustomers()
    C.cache
    spark.sparkContext.runJob(C,  (iter: Iterator[_]) => {})
    val O = tpch.loadOrders()
    O.cache
    spark.sparkContext.runJob(O,  (iter: Iterator[_]) => {})
    val L = tpch.loadLineitem()
    L.cache
    spark.sparkContext.runJob(L,  (iter: Iterator[_]) => {})
    val P = tpch.loadPart()
    P.cache
    spark.sparkContext.runJob(P,  (iter: Iterator[_]) => {})
 
 	tpch.triggerGC
   
    val l = L.map(l => l.l_partkey -> Record160(l.l_orderkey, l.l_quantity, l.l_partkey))
    val p = P.map(p => p.p_partkey -> Record161(p.p_name, p.p_partkey))
    val lpj = l.joinSkew(p)

    val OrderParts = lpj.map{ case (l, p) => l.l_orderkey -> Record172(p.p_partkey, l.l_quantity) }
    val CustomerOrders = O.map(o => o.o_orderkey -> Record166(o.o_orderdate, o.o_orderkey, o.o_custkey)).cogroup(OrderParts).flatMap{
      case (_, (orders, parts)) => orders.map(order => order.o_custkey -> Record319(order.o_orderdate, parts.toArray))
    }

    val c = C.map(c => c.c_custkey -> Record312(c.c_name, c.c_custkey)).cogroup(CustomerOrders).flatMap{
          case (_, (cnames, orders)) => cnames.map(c => Record321(c.c_name, orders.toArray))
      }

	  c.cache
	  spark.sparkContext.runJob(c,  (iter: Iterator[_]) => {})


    var start0 = System.currentTimeMillis()
    val accum1 = (acc: List[Record373], v: Record373) => v match {
      case Record373(null, 0.0) => acc
      case _ => acc :+ v
    }
    val accum2 = (acc1: List[Record373], acc2: List[Record373]) => acc1 ++ acc2
    val accum3 = (acc: List[Record374], v: Record374) => v match {
      case Record374(null, _) => acc
      case _ => acc :+ v
    }
    val accum4 = (acc1: List[Record374], acc2: List[Record374]) => acc1 ++ acc2
    val result = c.zipWithIndex.flatMap{
      case (ctup, id1) => if (ctup.c_orders.isEmpty) List((-1, (id1, ctup.c_name, null, null, null)))
        else ctup.c_orders.zipWithIndex.flatMap{
        case (otup, id2) => if (otup.o_parts.isEmpty) List((-1, (id1, ctup.c_name, id2, otup.o_orderdate, null)))
          else otup.o_parts.map{
            case ptup => ptup.p_partkey -> (id1, ctup.c_name, id2, otup.o_orderdate, ptup.l_qty) 
        }
      }
    }.outerJoinSkew(P.map(p => p.p_partkey -> Record318(p.p_retailprice, p.p_name))).map{
      case ((id1, cname, id2, date, qty:Double), Some(prec)) =>
        (id1, cname, id2, date, prec.p_name) -> qty*prec.p_retailprice
      case ((id1, cname, id2, date, _), _) => (id1, cname, id2, date, null) -> 0.0
    }.reduceByKey(_ + _).map{
      case ((id1, cname, id2, date, pname), total) => 
        (id1, cname, id2, date) -> Record373(pname, total)
    }.aggregateByKey(List.empty[Record373])(accum1, accum2).map{
		  case ((id1, cname, id2, date), bag) => (id1, cname) -> Record374(date.asInstanceOf[String], bag)
	  }.aggregateByKey(List.empty[Record374])(accum3, accum4).map{
		  case ((id1, cname), bag) => Record375(cname.asInstanceOf[String], bag.asInstanceOf[Iterable[Record374]])
	  }
    spark.sparkContext.runJob(result,  (iter: Iterator[_]) => {})
  	var end0 = System.currentTimeMillis() - start0
	  println("Query4SparkManual,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    
    result.flatMap{
      c =>
        if (c.c_orders.isEmpty) List((c.c_name, null, null, null))
        else c.c_orders.flatMap{
          o => 
            if (o.o_parts.isEmpty) List((c.c_name, o.o_orderdate, null, null))
            else o.o_parts.map(p => (c.c_name, o.o_orderdate, p.p_name, p._2))
         }
      }.sortBy(_._1).collect.foreach(println(_))
  
  }
}
