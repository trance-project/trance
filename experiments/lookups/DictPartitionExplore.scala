
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
import sprkloader.UtilPairRDD._
import sprkloader.Balancer
import org.apache.spark.broadcast.Broadcast
//import sprkloader.DomainRDD._
//import sprkloader.SkewDictRDD._
case class Record165(lbl: Unit)
case class Record166(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record167(p_name: String, p_partkey: Int)
case class Record168(l_orderkey: Int, p_name: String, l_qty: Double)
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
case class Record233(c_name: String, c_orders: Iterable[Record232])

object DictPartitionExplore {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("DictPartitionExplore"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)

val L__F = 3
val L__D_1 = tpch.loadLineitemProj
L__D_1.cache
spark.sparkContext.runJob(L__D_1, (iter: Iterator[_]) => {})
val P__F = 4
val P__D_1 = tpch.loadPartProj
P__D_1.cache
spark.sparkContext.runJob(P__D_1, (iter: Iterator[_]) => {})

	def f = {
 
var start0 = System.currentTimeMillis()


val start1 = System.currentTimeMillis()
val x67 = L__D_1
val x68 = P__D_1

val x69 = x67.map(l => (l.l_partkey, l))
val x71 = x68.map(p => (p.p_partkey, p))
val x70 = x69.joinSkew(x71)
spark.sparkContext.runJob(x70, (iter: Iterator[_]) => {})
val end1 = System.currentTimeMillis() - start1
//x70.collect.foreach(println(_))
println("DictPartitionExplore,"+sf+","+Config.datapath+","+end1+",standard skew,"+spark.sparkContext.applicationId)


val start2 = System.currentTimeMillis()
val hkeys = x69.sparkContext.broadcast(x69.heavyKeys())
val end2 = System.currentTimeMillis() - start2
//println("DictPartitionExplore,"+sf+","+Config.datapath+","+end2+",bc heavy keys,"+spark.sparkContext.applicationId)

val start3 = System.currentTimeMillis()
val (ll, lp) = x69.filterLight(x71, hkeys)
val (hl, hp) = x69.filterHeavy(x71, hkeys)
val end3 = System.currentTimeMillis() - start3
//println("DictPartitionExplore,"+sf+","+Config.datapath+","+end3+",split time,"+spark.sparkContext.applicationId)

val start4 = System.currentTimeMillis()
val standard = ll.joinDropKey(lp)
spark.sparkContext.runJob(standard, (iter: Iterator[_]) => {})
val end4 = System.currentTimeMillis() - start4
//standard.collect.foreach(println(_))
println("DictPartitionExplore,"+sf+","+Config.datapath+","+end2+",join light,"+spark.sparkContext.applicationId)

val start5 = System.currentTimeMillis()
val heavyParts = hl.sparkContext.broadcast(hp.collect.toMap).value
val heavyJoin = 
  hl.mapPartitions(it => 
    it.flatMap{ case (k,v) => heavyParts get k match {
      case Some(p) => List((v, p))
      case None => Nil
    }}, true)
spark.sparkContext.runJob(heavyJoin, (iter: Iterator[_]) => {})
//heavyJoin.collect.foreach(println(_))
val end5 = System.currentTimeMillis() - start5
println("DictPartitionExplore,"+sf+","+Config.datapath+","+end2+",join heavy,"+spark.sparkContext.applicationId)

    
}
f
  }
}
