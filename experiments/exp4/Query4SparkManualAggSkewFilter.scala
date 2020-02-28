
package experiments
/** 
This is manually written code based on the experiments
provided from Slender.
**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
import sprkloader.UtilPairRDD._
import scala.collection.mutable.HashMap
case class Record165(lbl: Unit)
case class Record166(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record167(p_name: String, p_partkey: Int)
case class Record168(l_orderkey: Int, p_partkey: Int, l_qty: Double)
case class Record169(c_name: String, c_custkey: Int)
case class Record170(c__Fc_custkey: Int)
case class Record171(c_name: String, c_orders: Record170)
case class Record172(lbl: Record170)
case class Record173(o_orderdate: String, o_priority: String, o_custkey: Int)
case class Record175(o__Fo_orderkey: Int)
case class Record176(o_orderdate: String, o_priority: String, o_parts: Record175)
case class Record177(lbl: Record175)
case class Record179(p_partkey: Int, l_qty: Double)
case class Record232(o_orderdate: String, o_priority: String, o_parts: Iterable[Record179])
case class Record233(c_name: String, c_orders: Iterable[Record232])
case class Record318(p_retailprice: Double, p_name: String)
case class Record373(p_name: String, _2: Double)
case class Record319(o_orderdate: String, o_parts: Array[Record172])
case class Record321(c_name: String, c_orders: Array[Record319])
case class Record374(o_orderdate: String, o_parts: Iterable[Record373])
case class Record375(c_name: String, c_orders: Iterable[Record374])
object Query4SparkManualAggSkewFilter {
  def main(args: Array[String]){
    val sf = Config.datapath.split("/").last
    val conf = new SparkConf().setMaster(Config.master).setAppName("Query4SparkManualAggSkewFilter"+sf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val tpch = TPCHLoader(spark)
    val C = tpch.loadCustomersProj()
    C.cache
    spark.sparkContext.runJob(C,  (iter: Iterator[_]) => {})
    val O = tpch.loadOrdersProj4()
    O.cache
    spark.sparkContext.runJob(O,  (iter: Iterator[_]) => {})
    val L = tpch.loadLineitemProj()
    L.cache
    spark.sparkContext.runJob(L,  (iter: Iterator[_]) => {})
    val P = tpch.loadPartProj4()
    P.cache
    spark.sparkContext.runJob(P,  (iter: Iterator[_]) => {})
 
 	  tpch.triggerGC
   
    val l = L.map(l => l.l_partkey -> Record166(l.l_orderkey, l.l_quantity, l.l_partkey))
    val p = P.map(p => p.p_partkey -> Record167(p.p_name, p.p_partkey))
    val lpj = l.joinSkew(p)

    val OrderParts = lpj.map{ case (l, p) => l.l_orderkey -> Record179(p.p_partkey, l.l_quantity) }

    val CustomerOrders = O.zipWithIndex.map{ case (o, id) => 
      o.o_orderkey ->  (Record173(o.o_orderdate, o.o_priority, o.o_custkey), id) }.cogroup(OrderParts).flatMap{
        case (ok, (orders, parts)) => orders.map{ case (o, id) => o.o_custkey -> Record232(o.o_orderdate, o.o_priority, parts) }}

    val cop = C.zipWithIndex.map{ case (c, id) => c.c_custkey -> (c.c_name, id) }.cogroup(CustomerOrders).flatMap{
      case (ck, (custs, orders)) => custs.map{ case (c, id) => Record233(c, orders) }
    }
    cop.cache
    spark.sparkContext.runJob(cop, (iter: Iterator[_]) => {})


    var start0 = System.currentTimeMillis()
    val cflat = cop.zipWithIndex.flatMap{
      case (ctup, id1) => if (ctup.c_orders.isEmpty) List((-1, (id1, ctup.c_name, null, null, 0.0)))
        else ctup.c_orders.zipWithIndex.flatMap{
        case (otup, id2) => 
          if (otup.o_priority.contains("HIGH")){
            if (otup.o_parts.isEmpty) List((-1, (id1, ctup.c_name, id2, otup.o_orderdate, 0.0)))
            else otup.o_parts.foldLeft(HashMap.empty[Int, Double].withDefaultValue(0.0))(
              (acc, p) => {acc(p.p_partkey) += p.l_qty; acc}).map{ case (pk, tot) => 
                pk -> (id1, ctup.c_name, id2, otup.o_orderdate, tot) }
          }else Nil
        }
    }
    val ps = P.map(p => p.p_partkey -> Record318(p.p_retailprice, p.p_name))
    val (j_L, j_H, hkeys1) = cflat.outerJoinSplit(ps)

    val mj_L = j_L.map{
      case ((id1, cust, id2, order, qty), Some(pinfo)) =>
        (id1, cust, id2, order, pinfo.p_name) -> qty*pinfo.p_retailprice
      case ((id1, cust, id2, order, qty), _) => (id1, cust, id2, order, null) -> 0.0
    }

    val mj_H = j_H.map{
      case ((id1, cust, id2, order, qty), Some(pinfo)) =>
        (id1, cust, id2, order, pinfo.p_name) -> qty*pinfo.p_retailprice
      case ((id1, cust, id2, order, qty), _) => (id1, cust, id2, order, null) -> 0.0
    }

    val (rb_L, rb_H) = mj_L.reduceBySplit(mj_H, _+_)

    val mr_L = rb_L.map{
      case ((id1, cust, id2, order, pname), total) => 
        (id1, cust, id2, order) -> Record373(pname, total)
    }

    val mr_H = rb_H.map{
      case ((id1, cust, id2, order, pname), total) => 
        (id1, cust, id2, order) -> Record373(pname, total)
    }

    val (ag1_L, ag1_H, hkeys2) = mr_L.aggregateBySplit(mr_H, Record373(null, 0.0))

    val mag1_L = ag1_L.map{
		  case ((id1, cust, id2, order), bag) => (id1, cust) -> Record374(order, bag)
	  }

    val mag1_H = ag1_H.map{
      case ((id1, cust, id2, order), bag) => (id1, cust) -> Record374(order, bag)
    }

    val (ag2_L, ag2_H, hkeys3) = mag1_L.aggregateBySplit(mag1_H, Record374(null, Iterable()))

    val res1_L = ag2_L.map{
		  case ((id1, cust), bag) => Record375(cust, bag)
	  }
    val res1_H = ag2_H.map{
      case ((id1, cust), bag) => Record375(cust, bag)
    }
    val result = res1_L.unionPartitions(res1_H)
    spark.sparkContext.runJob(result,  (iter: Iterator[_]) => {})
  	var end0 = System.currentTimeMillis() - start0
	  println("Query4SparkManualAggSkewFilter,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    
    /**result.flatMap{
      c =>
        if (c.c_orders.isEmpty) List((c.c_name, null, null, null))
        else c.c_orders.flatMap{
          o => 
            if (o.o_parts.isEmpty) List((c.c_name, o.o_orderdate, null, null))
            else o.o_parts.map(p => (c.c_name, o.o_orderdate, p.p_name, p._2))
         }
      }.sortBy(_._1).collect.foreach(println(_))**/
  
  }
}
