
package experiments
/** 
ManualAgg code from Slender experiments
**/
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._

case class RecordC(c_name: String, c_nationkey: Int)
case class RecordS(s_name: String, s_nationkey: Int)

object Query5SparkManualAgg {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query5SparkManualAgg"+sf)
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
  val customers = C.zipWithIndex.map{ case (c, id) => c.c_custkey -> (RecordC(c.c_name, c.c_nationkey), id) }
  val resultInner = orders.join(customers).map{
	  case (_, (o_orderkey, c_info)) => o_orderkey -> c_info
  }.join(lineitem).map{
    case (_, (custs, supkey)) => supkey -> custs
  }

  val result = S.zipWithIndex.map{ case (s, id) => 
      s.s_suppkey -> (RecordS(s.s_name, s.s_nationkey), id)
    }.join(resultInner).map{
      case (_, ((s, id), (c, id2))) => (s, id) -> c
    }.groupByKey().map{
      case ((s, id), customers) => s -> customers
    }
  spark.sparkContext.runJob(result, (iter: Iterator[_]) => {})
  var end0 = System.currentTimeMillis() - start0
  println("Query5SparkManualAgg"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
  
  /**result.flatMap{ 
    case (s, customers) => if (customers.isEmpty) List((s.s_name, s.s_nationkey, null, null))
      else customers.map(c => (s.s_name, s.s_nationkey, c.c_name, c.c_nationkey))
    }.sortBy(_._1).collect.foreach(println(_))**/

  }
}

