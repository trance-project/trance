
package experiments
/** 
This is manually written code.
**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record165(lbl: Unit)
case class Record166(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record167(p_name: String, p_partkey: Int)
case class Record168(l_orderkey: Int, p_partkey: Int, l_qty: Double)
case class Record169(c_name: String, c_custkey: Int)
case class Record170(c__Fc_custkey: Int)
case class Record171(c_name: String, c_orders: Record170)
case class Record172(lbl: Record170)
case class Record173(o_orderdate: String, o_custkey: Int)
case class Record175(o__Fo_orderkey: Int)
case class Record176(o_orderdate: String, o_parts: Record175)
case class Record177(lbl: Record175)
case class Record179(p_partkey: Int, l_qty: Double)
case class Record232(o_orderdate: String, o_parts: Iterable[Record179])
case class Record233(c_name: String, c_orders: Iterable[Record232])
case class Record416(orderdate: String, partkey: Int)
case class Record438(c_name: String, pname: String, totals: Double)
case class Record318(p_retailprice: Double, p_name: String)
case class RecordLP(p_name: String, total: Double)
case class RecordOLP(o_orderdate: String, oparts: Iterable[RecordLP])
case class RecordCOLP(c_name: String, corders: Iterable[RecordOLP])
object Query3SparkManual {
  def main(args: Array[String]){
    val sf = Config.datapath.split("/").last
    val conf = new SparkConf().setMaster(Config.master).setAppName("Query3SparkManualWide"+sf)
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

    val l = L.map(l => l.l_partkey -> Record166(l.l_orderkey, l.l_quantity, l.l_partkey))
    val p = P.map(p => p.p_partkey -> Record167(p.p_name, p.p_partkey))
    val lpj = l.join(p)

    val OrderParts = lpj.map{ case (k, (l, p)) => l.l_orderkey -> Record179(p.p_partkey, l.l_quantity) }

    val CustomerOrders = O.zipWithIndex.map{ case (o, id) => 
      o.o_orderkey ->  (Record173(o.o_orderdate, o.o_custkey), id) }.cogroup(OrderParts).flatMap{
        case (ok, (orders, parts)) => orders.map{ case (o, id) => o.o_custkey -> Record232(o.o_orderdate, parts) }}

    val cop = C.zipWithIndex.map{ case (c, id) => c.c_custkey -> (c.c_name, id) }.cogroup(CustomerOrders).flatMap{
      case (ck, (custs, orders)) => custs.map{ case (c, id) => Record233(c, orders) }
    }
    cop.cache
    spark.sparkContext.runJob(cop, (iter: Iterator[_]) => {})

    var start0 = System.currentTimeMillis()
    val result = cop.zipWithIndex.flatMap{
      case (ctup, id1) => if (ctup.c_orders.isEmpty) List((-1, (id1, ctup.c_name, null)))
        else ctup.corders.zipWithIndex.flatMap{
          else otup.o_parts.foldLeft(HashMap.empty[Int, Double].withDefaultValue(0.0))(
            (acc, p) => {acc(p.p_partkey) += p.l_qty; acc}).map{ case (pk, tot) => 
              pk -> (id1, ctup.c_name, id2, tot) }
    }.leftOuterJoin(P.map(p => p.p_partkey -> Record318(p.p_name, p.p_retailprice))).map{
      case (k, ((id1, cname), p)) => (c, l.p) -> lp.l_qty*p.p_retailprice
    }.reduceByKey(_+_).map{
      case ((c, p), total) => (c, p, total)
    }
    result.collect.foreach(println(_))
    spark.sparkContext.runJob(result, (iter: Iterator[_]) => {})
    var end0 = System.currentTimeMillis() - start0
	  println("Query3SparkManualWide"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
  
  }
}