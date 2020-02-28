
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
case class RecordC(c_custkey: Int, c_nationkey: Int)
case class RecordC2(c_name: String, c_nationkey: Int)
case class RecordCN(c_name: String, c_nationkey: Int, suppliers: Int)
case class RecordSCR(c_name: String, c_nationkey: Int, suppliers: Iterable[RecordS])

object ShredQuery6SparkOpt {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6SparkOpt"+sf)
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


val x65 = S__D_1.map(s => RecordSC(s.s_name, s.s_nationkey, s.s_suppkey))
val m__D_1 = x65
// m__D_1.cache


val customers_ctx1 = m__D_1.createDomain(l => l.customers)


val x41 = O__D_1.map{ case x47 => (x47.o_custkey, x47.o_orderkey) }
val x46 = C__D_1.map{ case x48 => (x48.c_custkey, RecordC(x48.c_custkey, x48.c_nationkey)) }
val x51 = x41.join(x46).values

val resultInner__D_1 = x51

//resultInner__D_1.collect.foreach(println(_))

val x81 = L__D_1.map{ case x83 => (x83.l_suppkey, x83.l_orderkey) }
val x82 = x81.joinDomain(customers_ctx1, (l: Int) => l)

val x83 = resultInner__D_1.join(x82).values

val x84 = x83.map{
  case (c, lbl) => lbl -> c
}.groupByKey(new HashPartitioner(400))
// customers__D_1.cache
val customers__D_1 = x84
//customers2__D_1.collect.foreach(println(_))

val query5__D_1 = m__D_1
query5__D_1.cache
spark.sparkContext.runJob(query5__D_1,  (iter: Iterator[_]) => {})

val query5__D_2customers_1 = customers__D_1
query5__D_2customers_1.cache
spark.sparkContext.runJob(query5__D_2customers_1,  (iter: Iterator[_]) => {})

def f = {
    var start0 = System.currentTimeMillis()
    val x169 = C__D_1.map{ case c => RecordCN(c.c_name, c.c_nationkey, c.c_custkey) }
    val m__D_1 = x169

    // m__D_1.cache
    spark.sparkContext.runJob(m__D_1,  (iter: Iterator[_]) => {})

    val x151 = query5__D_2customers_1.mapPartitions( it =>
      it.flatMap{ case (lbl, bag) => bag.map(c => (lbl, c)) 
    }, true).join(query5__D_1.map(s => 
      s.customers -> RecordS(s.s_name, s.s_nationkey))).map{
      case (_, (cust, supp)) => cust.c_custkey -> supp
    }.groupByKey(new HashPartitioner(400))

    val suppliers__D_1 = x151
    // suppliers__D_1.collect.foreach(println(_))
    // suppliers__D_1.cache
    spark.sparkContext.runJob(suppliers__D_1,  (iter: Iterator[_]) => {})
    var end0 = System.currentTimeMillis() - start0
    println("ShredQuery6SparkOpt,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    var start = System.currentTimeMillis()
    /**val result = m__D_1.map(c => c.suppliers -> (c.c_name, c.c_nationkey)).cogroup(suppliers__D_1).flatMap{
      case (_, (cs, supps)) => cs.map(c => RecordSCR(c._1, c._2, supps.flatten))
    }
    spark.sparkContext.runJob(result,  (iter: Iterator[_]) => {})**/
    var end = System.currentTimeMillis() - start
      
    println("ShredQuery6SparkOpt"+sf+","+Config.datapath+","+end+",unshredding,"+spark.sparkContext.applicationId)

    /**result.flatMap{
      case cs => if (cs.suppliers.isEmpty) List((cs.c_name, cs.c_nationkey, null, null))
        else cs.suppliers.map( s => (cs.c_name, cs.c_nationkey, s.s_name, s.s_nationkey))
    }.collect.foreach(println(_))**/
  }
  f
 }
}
