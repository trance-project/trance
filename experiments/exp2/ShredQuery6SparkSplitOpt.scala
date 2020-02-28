
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
import sprkloader.UtilPairRDD._
import sprkloader.DomainRDD._
import org.apache.spark.HashPartitioner

case class RecordS(s_name: String, s_nationkey: Int)
case class RecordSC(s_name: String, s_nationkey: Int, customers: Int)
case class RecordC(c_custkey: Int, c_nationkey: Int)
case class RecordC2(c_name: String, c_nationkey: Int)
case class RecordCN(c_name: String, c_nationkey: Int, suppliers: Int)
case class RecordSCR(c_name: String, c_nationkey: Int, suppliers: Iterable[RecordS])

object ShredQuery6SparkSplitOpt {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6SparkSplitOpt"+sf)
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
spark.sparkContext.runJob(m__D_1, (iter: Iterator[_]) => {})


val x41 = O__D_1.map{ case x47 => (x47.o_custkey, x47.o_orderkey) }
val x46 = C__D_1.map{ case x48 => (x48.c_custkey, RecordC(x48.c_custkey, x48.c_nationkey)) }
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

val query5__D_1 = m__D_1
query5__D_1.cache
spark.sparkContext.runJob(query5__D_1,  (iter: Iterator[_]) => {})

val query5__D_2customers_1_L = customers__D_1_L
val query5__D_2customers_1_H = customers__D_1_H
query5__D_2customers_1_L.cache
query5__D_2customers_1_H.cache
spark.sparkContext.runJob(query5__D_2customers_1_L,  (iter: Iterator[_]) => {})
spark.sparkContext.runJob(query5__D_2customers_1_H,  (iter: Iterator[_]) => {})

def f = {
    var start0 = System.currentTimeMillis()
    val x169 = C__D_1.map{ case c => RecordCN(c.c_name, c.c_nationkey, c.c_custkey) }
    val m__D_1 = x169

    // m__D_1.cache
    spark.sparkContext.runJob(m__D_1,  (iter: Iterator[_]) => {})

    val x151_L = query5__D_2customers_1_L.mapPartitions( it =>
      it.flatMap{ case (lbl, bag) => bag.map(c => (lbl, c)) 
    }, true)
    val x151_H = query5__D_2customers_1_H.mapPartitions( it =>
      it.flatMap{ case (lbl, bag) => bag.map(c => (lbl, c)) 
    }, true)

    val topd = query5__D_1.map(s => 
      s.customers -> RecordS(s.s_name, s.s_nationkey))

    val (x152_L, x152_H) = x151_L.joinSplit(x151_H, topd, hkeys3)

    val x153_L = x152_L.map{
      case (cust, supp) => cust.c_custkey -> supp
    }

    val x153_H = x152_H.map{
      case (cust, supp) => cust.c_custkey -> supp
    }

    val (x154_L, x154_H, hkeys4) = x153_L.groupBySplit(x153_H)

    val suppliers__D_1_L = x154_L
    val suppliers__D_1_H = x154_H
    // suppliers__D_1.collect.foreach(println(_))
    // suppliers__D_1.cache
    spark.sparkContext.runJob(suppliers__D_1_L,  (iter: Iterator[_]) => {})
    spark.sparkContext.runJob(suppliers__D_1_H,  (iter: Iterator[_]) => {})
    var end0 = System.currentTimeMillis() - start0
    println("ShredQuery6SparkSplitOpt,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    var start = System.currentTimeMillis()
    /**val suppliers__D_1 = suppliers__D_1_L.unionPartitions(suppliers__D_1_H).map(l => l)
    val result = m__D_1.map(c => c.suppliers -> (c.c_name, c.c_nationkey)).join(suppliers__D_1).map{
      case (_, ((cname, nk), supps)) => RecordSCR(cname, nk, supps)
    }
    spark.sparkContext.runJob(result,  (iter: Iterator[_]) => {})**/
    var end = System.currentTimeMillis() - start
    println("ShredQuery6SparkSplitOpt"+sf+","+Config.datapath+","+end+",unshredding,"+spark.sparkContext.applicationId)
    /**result.flatMap{
      case cs => if (cs.suppliers.isEmpty) List((cs.c_name, cs.c_nationkey, null, null))
        else cs.suppliers.map( s => (cs.c_name, cs.c_nationkey, s.s_name, s.s_nationkey))
    }.collect.foreach(println(_))**/
  }
  f
 }
}
