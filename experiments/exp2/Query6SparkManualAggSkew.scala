
package experiments
/** 
Manual code from Slender experiments
**/
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
import sprkloader.UtilPairRDD._

case class RecordC(c_custkey: Int, c_nationkey: Int)
case class RecordS(s_name: String, s_nationkey: Int)
case class RecordSC(s: RecordS, customers: Iterable[RecordC])
case class RecordCN(c_name: String, c_nationkey: Int)
case class RecordCS(c: RecordCN, suppliers: Iterable[RecordS])

object Query6SparkManualAggSkew {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query6SparkManualAggSkew"+sf)
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
  
  val lineitem = L.zipWithIndex.map{ case (l,id) => l.l_orderkey -> l.l_suppkey }
  val orders = O.zipWithIndex.map{ case (o, id) => o.o_custkey -> o.o_orderkey }
  val customers = C.zipWithIndex.map{ case (c, id) => c.c_custkey -> RecordC(c.c_custkey, c.c_nationkey) }
  val resultInner = orders.join(customers).map{
    case (_, (o_orderkey, c_info)) => o_orderkey -> c_info
  }.join(lineitem).map{
    case (_, (custs, supkey)) => supkey -> custs
  }

  val spc = S.zipWithIndex.map{ case (s, id) => 
      s.s_suppkey -> RecordS(s.s_name, s.s_nationkey)
    }.cogroup(resultInner).flatMap{
      case (_, (supps, custs)) => supps.map(s => RecordSC(s, custs))
    }
  spc.cache
  spark.sparkContext.runJob(spc, (iter: Iterator[_]) => {})

  var start0 = System.currentTimeMillis()
  val custs = C.zipWithIndex.map{ case (c, id) => c.c_custkey -> RecordCN(c.c_name, c.c_nationkey) }
  val spcFlat = spc.zipWithIndex.flatMap{
    case (s, id) => if (s.customers.isEmpty) List((-1, s.s))
      else s.customers.map( c => (c.c_custkey, s.s))
  }

  val (cg1_L, cg1_H, hkeys1) = custs.cogroupSplit(spcFlat)

  val r1_L = cg1_L.map{
    case (cname, suppliers) => RecordCS(cname, suppliers)
  }

  val r1_H = cg1_H.map{
    case (cname, suppliers) => RecordCS(cname, suppliers)
  }

  val result = r1_L.unionPartitions(r1_H)
  spark.sparkContext.runJob(result,  (iter: Iterator[_]) => {})
  var end0 = System.currentTimeMillis() - start0
  println("Query6SparkManualSkew"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
  
  /**result.flatMap{
    case cs => if (cs.suppliers.isEmpty) List((cs.c.c_name, cs.c.c_nationkey, null, null))
      else cs.suppliers.map( s => (cs.c.c_name, cs.c.c_nationkey, s.s_name, s.s_nationkey))
  }.collect.foreach(println(_))**/

  }

}
