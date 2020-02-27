
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
import sprkloader.DomainRDD._
import org.apache.spark.HashPartitioner
case class RecordS(s_name: String, s_nationkey: Int)
case class RecordSC(s_name: String, s_nationkey: Int, customers: Int)
case class RecordC(c_name: String, c_nationkey: Int)
case class RecordSCR(s_name: String, s_nationkey: Int, customers: Iterable[RecordC])
object ShredQuery5SparkOpt {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery5SparkOpt"+sf)
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

val x41 = O__D_1.map{ case x47 => (x47.o_custkey, x47.o_orderkey) }
val x46 = C__D_1.map{ case x48 => (x48.c_custkey, x48) }
val x51 = x41.join(x46).values.map{
  case (ok, c) => ok -> RecordC(c.c_name, c.c_nationkey)
}

val resultInner__D_1 = x51

//resultInner__D_1.collect.foreach(println(_))

val x81 = L__D_1.map{ case x83 => (x83.l_orderkey, x83.l_suppkey) }

val x83 = resultInner__D_1.joinDropKey(x81)

val x84 = x83.map{
  case (c, lbl) => lbl -> c
}.groupByKey(new HashPartitioner(400))
val customers__D_1 = x84

// customers__D_1.cache
spark.sparkContext.runJob(customers__D_1, (iter: Iterator[_]) => {})
var end0 = System.currentTimeMillis() - start0
println("ShredQuery5SparkOpt,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)

var start = System.currentTimeMillis()
val result = m__D_1.map(s => s.customers -> RecordS(s.s_name, s.s_nationkey)).cogroup(customers__D_1).flatMap{
  case (_, (supps, custs)) => supps.map(s => (s, custs.flatten))
}
spark.sparkContext.runJob(result, (iter: Iterator[_]) => {})
var end = System.currentTimeMillis() - start
println("ShredQuery5SparkOpt,"+sf+","+Config.datapath+","+end+",unshredding,"+spark.sparkContext.applicationId)
   
result.flatMap{
  case (s, custs) => if (custs.isEmpty) List((s.s_name, s.s_nationkey, null, null))
    else custs.map(c => (s.s_name, s.s_nationkey, c.c_name, c.c_nationkey))
}.collect.foreach(println(_))

}
f

 }
}
