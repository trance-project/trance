
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
import sprkloader.DomainRDD._
import sprkloader.UtilPairRDD._
import org.apache.spark.HashPartitioner
case class RecordS(s_name: String, s_nationkey: Int)
case class RecordSC(s_name: String, s_nationkey: Int, customers: Int)
case class RecordC(c_name: String, c_nationkey: Int)
case class RecordSCR(s_name: String, s_nationkey: Int, customers: Iterable[RecordC])
object ShredQuery5SparkSplitOpt {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery5SparkSplitOpt"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   
   val tpch = TPCHLoader(spark)
    val L__F = 3
    val L__D_1 = tpch.loadLineitemProj5()
    L__D_1.cache
    L__D_1.count
    val C__F = 1
    val C__D_1 = tpch.loadCustomersProj5()
    C__D_1.cache
    C__D_1.count
    val O__F = 2
    val O__D_1 = tpch.loadOrdersProj()
    O__D_1.cache
    O__D_1.count
    val S__F = 6
    val S__D_1 = tpch.loadSupplierProj()
    S__D_1.cache
    S__D_1.count

    def f = {
 
    var start0 = System.currentTimeMillis()


val x65 = S__D_1.map(s => RecordSC(s.s_name, s.s_nationkey, s.s_suppkey))
val m__D_1 = x65
// m__D_1.cache
spark.sparkContext.runJob(m__D_1, (iter: Iterator[_]) => {})


val x41 = O__D_1.map{ case x47 => (x47.o_custkey, x47.o_orderkey) }
val x46 = C__D_1.map{ case x48 => (x48.c_custkey, RecordC(x48.c_name, x48.c_nationkey)) }
val (x51_L, x51_H, hkeys1) = x41.joinSplit(x46)

// partitioning bug
val x52_L = x51_L.map(l => l)
val x52_H = x51_H.map(l => l)

val resultInner__D_1_L = x52_L
val resultInner__D_1_H = x52_H

//resultInner__D_1.collect.foreach(println(_))

val x81 = L__D_1.map{ case x83 => (x83.l_orderkey, x83.l_suppkey) }

val (x83_L, x83_H, hkeys2) = resultInner__D_1_L.unionPartitions(resultInner__D_1_H).joinSplit(x81)

val x84_L = x83_L.map{
  case (c, lbl) => lbl -> c
}
val x84_H = x83_H.map{
  case (c, lbl) => lbl -> c
}

val (x85_L, x85_H, hkeys3) = x84_L.groupBySplit(x84_H)
val customers__D_1_L = x85_L
val customers__D_1_H = x85_H

// customers__D_1.cache
spark.sparkContext.runJob(customers__D_1_L, (iter: Iterator[_]) => {})
spark.sparkContext.runJob(customers__D_1_H, (iter: Iterator[_]) => {})
var end0 = System.currentTimeMillis() - start0
println("ShredQuery5SparkSplitOpt,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)

var start = System.currentTimeMillis()
/**val customers__D_1 = customers__D_1_L.unionPartitions(customers__D_1_H)
val result = m__D_1.map(s => s.customers -> RecordS(s.s_name, s.s_nationkey)).cogroup(customers__D_1).flatMap{
  case (_, (supps, custs)) => supps.map(s => (s, custs.flatten))
}
spark.sparkContext.runJob(result, (iter: Iterator[_]) => {})**/
var end = System.currentTimeMillis() - start
println("ShredQuery5SparkSplitOpt,"+sf+","+Config.datapath+","+end+",unshredding,"+spark.sparkContext.applicationId)
   
/**result.flatMap{
  case (s, custs) => if (custs.isEmpty) List((s.s_name, s.s_nationkey, null, null))
    else custs.map(c => (s.s_name, s.s_nationkey, c.c_name, c.c_nationkey))
}.collect.foreach(println(_))**/

}
f

 }
}
