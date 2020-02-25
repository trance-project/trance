
package experiments
/** 
This is manually written code based on the experiments
provided from Slender.
**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
import scala.collection.mutable.HashMap
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
case class Record380(p: PartTrunc, l_qty: Double)
case class RecordLP(p: PartTrunc, l_orderkey: Int, l_qty: Double)
case class RecordOLP(o: Orders, oparts: Iterable[RecordLP])
case class RecordCOLP(c: Customer, corders: Iterable[RecordOLP])
case class PartTrunc(p_partkey: Int, p_mfgr: String, p_brand: String, p_type: String, p_size: Int, p_container: String, p_comment: String)
case class PartExtend(p: Record380, p_name: String, total: Double)
case class RecordOP(o: Orders, oparts: Iterable[PartExtend])
case class RecordCOP(c: Customer, corders: Iterable[RecordOP])
object Query4SparkManualAggWide {
  def main(args: Array[String]){
    val sf = Config.datapath.split("/").last
    val conf = new SparkConf().setMaster(Config.master).setAppName("Query4SparkManualAggWide"+sf)
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
    val p = P.map(p => p.p_partkey -> PartTrunc(p.p_partkey, p.p_mfgr, p.p_brand, p.p_type, p.p_size, p.p_container, p.p_comment))
    val lpj = l.join(p)

    val OrderParts = lpj.map{ case (k, (l, p)) => l.l_orderkey -> RecordLP(p, l.l_orderkey, l.l_quantity) }

    val CustomerOrders = O.zipWithIndex.map{ case (o, id) => o.o_orderkey -> (o, id) }.cogroup(OrderParts).flatMap{
      case (ok, (orders, parts)) => orders.map{ case (o, id) => o.o_custkey -> RecordOLP(o, parts) }}

    val cop = C.zipWithIndex.map{ case (c, id) => c.c_custkey -> (c, id) }.cogroup(CustomerOrders).flatMap{
      case (ck, (custs, orders)) => custs.map{ case (c, id) => RecordCOLP(c, orders) }
    }
	  cop.cache
    spark.sparkContext.runJob(cop, (iter: Iterator[_]) => {})


    var start0 = System.currentTimeMillis()
    val accum1 = (acc: List[PartExtend], v: PartExtend) => v match {
      case PartExtend(null, _, _) => acc
      case _ => acc :+ v
    }
    val accum2 = (acc1: List[PartExtend], acc2: List[PartExtend]) => acc1 ++ acc2
    val accum3 = (acc: List[RecordOP], v: RecordOP) => v match {
      case RecordOP(null, _) => acc
      case _ => acc :+ v
    }
    val accum4 = (acc1: List[RecordOP], acc2: List[RecordOP]) => acc1 ++ acc2
    val result = cop.zipWithIndex.flatMap{
      case (ctup, id1) => if (ctup.corders.isEmpty) List((-1, (id1, ctup.c, null, null, null)))
        else ctup.corders.zipWithIndex.flatMap{
        case (otup, id2) => if (otup.oparts.isEmpty) List((-1, (id1, ctup.c, id2, otup.o, null)))
          else otup.oparts.foldLeft(HashMap.empty[PartTrunc, Double].withDefaultValue(0.0))(
            (acc, p) => {acc(p.p) += p.l_qty; acc}).map{ case (p, tot) =>
              (p.p_partkey, (id1, ctup.c, id2, otup.o, Record380(p, tot))) }
      }
    }.leftOuterJoin(P.map(p => p.p_partkey -> Record318(p.p_retailprice, p.p_name))).map{
      case (_, ((id1, cust, id2, order, part), Some(pinfo))) =>
        (id1, cust, id2, order, part, pinfo.p_name) -> part.l_qty*pinfo.p_retailprice
      case (_, ((id1, cust, id2, order, part), _)) => (id1, cust, id2, order, part, null) -> 0.0
    }.reduceByKey(_ + _).map{
      case ((id1, cust, id2, order, part, pname), total) => 
        (id1, cust, id2, order) -> PartExtend(part, pname, total)
    }.aggregateByKey(List.empty[PartExtend])(accum1, accum2).map{
		  case ((id1, cust, id2, order), bag) => (id1, cust) -> RecordOP(order, bag)
	  }.aggregateByKey(List.empty[RecordOP])(accum3, accum4).map{
		  case ((id1, cust), bag) => RecordCOP(cust, bag)
	  }
    spark.sparkContext.runJob(result,  (iter: Iterator[_]) => {})
  	var end0 = System.currentTimeMillis() - start0
	  println("Query4SparkManualAggWide,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    
    /**result.flatMap{
      c =>
        if (c.corders.isEmpty) List((c.c.c_name, null, null, null))
        else c.corders.flatMap{
          o => 
            if (o.oparts.isEmpty) List((c.c.c_name, o.o.o_orderdate, null, null))
            else o.oparts.map(p => (c.c.c_name, o.o.o_orderdate, p.p_name, p.total))
         }
      }.sortBy(_._1).collect.foreach(println(_))**/
  
  }
}
