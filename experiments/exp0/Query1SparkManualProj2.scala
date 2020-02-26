
package experiments
/** 
This is manually written code based on the experiments
provided from Slender.s
**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
/** Inherit same case class from generated code **/
case class Record165(lbl: Unit)
case class Record166(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record167(p_name: String, p_partkey: Int)
case class Record168(l_orderkey: Int, p_partkey: Int, l_qty: Double)
case class Record169(c_name: String, c_custkey: Int)
case class Record170(c__Fc_custkey: Int)
case class Record171(c_name: String, c_orders: Record170)
case class Record172(lbl: Record170)
case class Record173(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record175(o__Fo_orderkey: Int)
case class Record176(o_orderdate: String, o_parts: Record175)
case class Record177(lbl: Record175)
case class Record179(p_name: String, l_qty: Double)
case class Record232(o_orderdate: String, o_parts: Iterable[Record179])
case class Record233(c: Customer, c_orders: List[Record232])
object Query1SparkManualProj2 {
  def main(args: Array[String]){
    val sf = Config.datapath.split("/").last
    val conf = new SparkConf().setMaster(Config.master).setAppName("Query1SparkManualProj2"+sf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val tpch = TPCHLoader(spark)
    val C = tpch.loadCustomers()
    C.cache
    spark.sparkContext.runJob(C, (iter: Iterator[_]) => {})
    val O = tpch.loadOrders()
    O.cache
    spark.sparkContext.runJob(O, (iter: Iterator[_]) => {})
    val L = tpch.loadLineitem()
    L.cache
    spark.sparkContext.runJob(L, (iter: Iterator[_]) => {})
    val P = tpch.loadPart()
    P.cache
    spark.sparkContext.runJob(P, (iter: Iterator[_]) => {})
       
	  tpch.triggerGC

    var start0 = System.currentTimeMillis()
    val accum1 = (acc: List[Record232], v: Record232) => v match {
      case Record232(null, _) => acc
      case _ => acc :+ v
    }
    val accum2 = (acc1: List[Record232], acc2: List[Record232]) => acc1 ++ acc2
    val accum3 = (acc: List[Record179], v: Record179) => v match {
      case Record179(null, _) => acc
      case _ => acc :+ v
    }
    val accum4 = (acc1: List[Record179], acc2: List[Record179]) => acc1 ++ acc2
    val customers = C.zipWithIndex.map{ case (c, id) => (c.c_custkey, (c, id)) }
    val orders = O.zipWithIndex.map{ case (o, id) => (o.o_custkey, (OrdersProj(o.o_orderkey, o.o_custkey, o.o_orderdate), id)) }
    val co = customers.leftOuterJoin(orders).map{
      case (ck, (c, Some(o))) => o._1.o_orderkey -> (c, o)
      case (ck, (c, None)) => -1 -> (c, null)
    }
    val lineitem = L.zipWithIndex.map{ case (l,id)  => (l.l_orderkey, (LineitemProj(l.l_orderkey, l.l_partkey, l.l_quantity), id)) }
    val col = co.leftOuterJoin(lineitem).map{
      case (ok, ((c,o), Some(l))) => l._1.l_partkey -> ((c, o), l)
      case (ok, ((c,o), None)) => -1 -> ((c, o), null)
    }
    val parts = P.zipWithIndex.map{ case (p, id) => (p.p_partkey, (PartProj(p.p_partkey, p.p_name), id)) }
    val colp = col.leftOuterJoin(parts).map{
      case (pk, (((c,o), l), Some(p))) => (c, o) -> Record179(p._1.p_name, l._1.l_quantity)
      case (pk, (((c,o), null), None)) => (c, o) -> Record179(null, 0.0)
      case (pk, (((c,o), l), None)) => (c, o) -> Record179(null, l._1.l_quantity)
    }.aggregateByKey(List.empty[Record179])(accum3, accum4).map{
      case ((c,null), parts) => c -> Record232(null, parts)
      case ((c,o), parts) => c -> Record232(o._1.o_orderdate, parts)
    }.aggregateByKey(List.empty[Record232])(accum1, accum2)
    val result = colp.map{
      case ((c, id), orders) => Record233(c, orders)
    }
    spark.sparkContext.runJob(result, (iter: Iterator[_]) => {})
    var end0 = System.currentTimeMillis() - start0
    println("Query1SparkManualProj2"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
  
    /**result.flatMap(c => 
      if (c.c_orders.isEmpty) List((c.c.c_name, null, null, null))
      else c.c_orders.flatMap(o => if (o.o_parts.isEmpty) List((c.c.c_name, o.o_orderdate, null, null))
         else o.o_parts.map(p => (c.c.c_name, o.o_orderdate, p.p_name, p.l_qty)))).sortBy(_._1).collect.foreach(println(_))**/

  }
}
