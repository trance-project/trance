
package experiments
/** 
ManualAggSkew code from Slender experiments
**/
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
import sprkloader.UtilPairRDD._
case class RecordC(c_name: String, c_nationkey: Int)
case class RecordS(s_name: String, s_nationkey: Int)
case class RecordSC(s: RecordS, customers: Iterable[RecordC])

object Query5SparkManualAggSkew {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query5SparkManualAggSkew"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()

   val tpch = TPCHLoader(spark)
   val C = tpch.loadCustomersProj5()
   C.cache
   C.count
   val O = tpch.loadOrdersProj()
   O.cache
   O.count
   val L = tpch.loadLineitemProj5()
   L.cache
   L.count
   val S = tpch.loadSupplierProj()
   S.cache
   S.count
 
  var start0 = System.currentTimeMillis()
  
  val lineitem = L.zipWithIndex.map{ case (l,id) => l.l_orderkey -> l.l_suppkey }
  val orders = O.zipWithIndex.map{ case (o, id) => o.o_custkey -> o.o_orderkey }
  val customers = C.zipWithIndex.map{ case (c, id) => c.c_custkey -> RecordC(c.c_name, c.c_nationkey) }
  
  val (co_L, co_H, hkeys1) = orders.joinSplit(customers)

  val com_L = co_L.map{
	  case (o_orderkey, c_info) => o_orderkey -> c_info
  }

  val com_H = co_H.map{
    case (o_orderkey, c_info) => o_orderkey -> c_info
  }

  val (col_L, col_H, hkeys2) = com_L.joinSplit(com_H, lineitem)

  val colm_L = col_L.map{
    case (custs, supkey) => supkey -> custs
  }

  val colm_H = col_H.map{
    case (custs, supkey) => supkey -> custs
  }

  val suppliers = S.zipWithIndex.map{ case (s, id) => 
      s.s_suppkey -> RecordS(s.s_name, s.s_nationkey)
    }

  val (cg_L, cg_H, hkeys3) = suppliers.cogroupSplit(colm_L.unionPartitions(colm_H))

  val result_L = cg_L.map{
    case (s, cust) => RecordSC(s, cust)
  }

  val result_H = cg_H.map{
    case (s, cust) => RecordSC(s, cust)
  }

  val result = result_L.unionPartitions(result_H)

  spark.sparkContext.runJob(result, (iter: Iterator[_]) => {})
  var end0 = System.currentTimeMillis() - start0
  println("Query5SparkManualAggSkew"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
  
  /**result.flatMap{ 
    case s => if (s.customers.isEmpty) List((s.s.s_name, s.s.s_nationkey, null, null))
      else s.customers.map(c => (s.s.s_name, s.s.s_nationkey, c.c_name, c.c_nationkey))
    }.sortBy(_._1).collect.foreach(println(_))**/

  }
}

