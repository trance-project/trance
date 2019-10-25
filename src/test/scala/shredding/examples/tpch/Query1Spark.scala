
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record275(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record276(p_name: String, p_partkey: Int)
case class Record277(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record278(c_name: String, c_custkey: Int)
case class Record279(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record281(p_name: String, l_qty: Double)
case class Record283(o_orderdate: String, o_parts: Iterable[Record281])
case class Record284(c_name: String, c_orders: Iterable[Record283])
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
 val x207 = L.map(x202 => { val x203 = x202.l_orderkey 
val x204 = x202.l_quantity 
val x205 = x202.l_partkey 
val x206 = Record275(x203, x204, x205) 
x206 }) 
val x212 = P.map(x208 => { val x209 = x208.p_name 
val x210 = x208.p_partkey 
val x211 = Record276(x209, x210) 
x211 }) 
val x217 = { val out1 = x207.map{ case x213 => ({val x215 = x213.l_partkey 
x215}, x213) }
  val out2 = x212.map{ case x214 => ({val x216 = x214.p_partkey 
x216}, x214) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x224 = x217.map{ case (x218, x219) => 
   val x220 = x218.l_orderkey 
val x221 = x219.p_name 
val x222 = x218.l_quantity 
val x223 = Record277(x220, x221, x222) 
x223 
} 
val ljp = x224
val x225 = ljp
//ljp.collect.foreach(println(_))
val x230 = C.map(x226 => { val x227 = x226.c_name 
val x228 = x226.c_custkey 
val x229 = Record278(x227, x228) 
x229 }) 
val x236 = O.map(x231 => { val x232 = x231.o_orderdate 
val x233 = x231.o_orderkey 
val x234 = x231.o_custkey 
val x235 = Record279(x232, x233, x234) 
x235 }) 
val x241 = { val out1 = x230.map{ case x237 => ({val x239 = x237.c_custkey 
x239}, x237) }
  val out2 = x236.map{ case x238 => ({val x240 = x238.o_custkey 
x240}, x238) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x243 = ljp 
val x249 = { val out1 = x241.map{ case (x244, x245) => ({val x247 = x245.o_orderkey 
x247}, (x244, x245)) }
  val out2 = x243.map{ case x246 => ({val x248 = x246.l_orderkey 
x248}, x246) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x259 = x249.map{ case ((x250, x251), x252) => val x258 = (x252) 
x258 match {
   case (null) => ({val x253 = (x250,x251) 
x253}, null) 
   case x257 => ({val x253 = (x250,x251) 
x253}, {val x254 = x252.p_name 
val x255 = x252.l_qty 
val x256 = Record281(x254, x255) 
x256})
 }
}.groupByKey() 
val x268 = x259.map{ case ((x260, x261), x262) => val x267 = (x261,x262) 
x267 match {
   case (_,null) => ({val x263 = (x260) 
x263}, null) 
   case x266 => ({val x263 = (x260) 
x263}, {val x264 = x261.o_orderdate 
val x265 = Record283(x264, x262) 
x265})
 }
}.groupByKey() 
val x273 = x268.map{ case (x269, x270) => 
   val x271 = x269.c_name 
val x272 = Record284(x271, x270) 
x272 
} 
x273.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0
   println("Query1Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
