
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record93(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record94(p_name: String, p_partkey: Int)
case class Record95(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record96(c_name: String, c_custkey: Int)
case class Record97(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record99(p_name: String, l_qty: Double)
case class Record101(o_orderdate: String, o_parts: Iterable[Record99])
case class Record102(c_name: String, c_orders: Iterable[Record101])
object Query1Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query1Spark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L = tpch.loadLineitem
L.cache
L.count
val P = tpch.loadPart
P.cache
P.count
val C = tpch.loadCustomers
C.cache
C.count
val O = tpch.loadOrders
O.cache
O.count

   def f = { 
 val x25 = L.map(x20 => { val x21 = x20.l_orderkey 
val x22 = x20.l_quantity 
val x23 = x20.l_partkey 
val x24 = Record93(x21, x22, x23) 
x24 }) 
val x30 = P.map(x26 => { val x27 = x26.p_name 
val x28 = x26.p_partkey 
val x29 = Record94(x27, x28) 
x29 }) 
val x35 = { val out1 = x25.map{ case x31 => ({val x33 = x31.l_partkey 
x33}, x31) }
  val out2 = x30.map{ case x32 => ({val x34 = x32.p_partkey 
x34}, x32) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x42 = x35.map{ case (x36, x37) => 
   val x38 = x36.l_orderkey 
val x39 = x37.p_name 
val x40 = x36.l_quantity 
val x41 = Record95(x38, x39, x40) 
x41 
} 
val ljp = x42
val x43 = ljp
//ljp.collect.foreach(println(_))
val x48 = C.map(x44 => { val x45 = x44.c_name 
val x46 = x44.c_custkey 
val x47 = Record96(x45, x46) 
x47 }) 
val x54 = O.map(x49 => { val x50 = x49.o_orderdate 
val x51 = x49.o_orderkey 
val x52 = x49.o_custkey 
val x53 = Record97(x50, x51, x52) 
x53 }) 
val x59 = { val out1 = x48.map{ case x55 => ({val x57 = x55.c_custkey 
x57}, x55) }
  val out2 = x54.map{ case x56 => ({val x58 = x56.o_custkey 
x58}, x56) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x61 = ljp 
val x67 = { val out1 = x59.map{ case (x62, x63) => ({val x65 = x63.o_orderkey 
x65}, (x62, x63)) }
  val out2 = x61.map{ case x64 => ({val x66 = x64.l_orderkey 
x66}, x64) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x77 = x67.map{ case ((x68, x69), x70) => val x76 = (x70) 
x76 match {
   case (null) => ({val x71 = (x68,x69) 
x71}, null) 
   case x75 => ({val x71 = (x68,x69) 
x71}, {val x72 = x70.p_name 
val x73 = x70.l_qty 
val x74 = Record99(x72, x73) 
x74})
 }
}.groupByKey() 
val x86 = x77.map{ case ((x78, x79), x80) => val x85 = (x79,x80) 
x85 match {
   case (_,null) => ({val x81 = (x78) 
x81}, null) 
   case x84 => ({val x81 = (x78) 
x81}, {val x82 = x79.o_orderdate 
val x83 = Record101(x82, x80) 
x83})
 }
}.groupByKey() 
val x91 = x86.map{ case (x87, x88) => 
   val x89 = x87.c_name 
val x90 = Record102(x89, x88) 
x90 
} 
x91.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0
   println("Query1Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
