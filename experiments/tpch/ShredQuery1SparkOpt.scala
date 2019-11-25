
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record302(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record303(p_name: String, p_partkey: Int)
case class Record304(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record305(c__Fc_custkey: Int)
case class Record306(c_name: String, c_orders: Record305)
case class Record307(o__Fo_orderkey: Int)
case class Record308(o_orderdate: String, o_parts: Record307)
case class Record309(p_name: String, l_qty: Double)
object ShredQuery1SparkOpt {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1SparkOpt"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L__F = 3
val L__D_1 = tpch.loadLineitem
L__D_1.cache
L__D_1.count
val P__F = 4
val P__D_1 = tpch.loadPart
P__D_1.cache
P__D_1.count
val C__F = 1
val C__D_1 = tpch.loadCustomers
C__D_1.cache
C__D_1.count
val O__F = 2
val O__D_1 = tpch.loadOrders
O__D_1.cache
O__D_1.count

   def f = { 
/**

ljp__D_1 :=  REDUCE[ (l_orderkey := x239.l_orderkey,p_name := x240.p_name,l_qty := x239.l_quantity) / true ]( <-- (x239,x240) -- (
 <-- (x239) -- SELECT[ true, (l_orderkey := x239.l_orderkey,l_quantity := x239.l_quantity,l_partkey := x239.l_partkey) ](L__D._1)) JOIN[x239.l_partkey = x240.p_partkey](

   <-- (x240) -- SELECT[ true, (p_name := x240.p_name,p_partkey := x240.p_partkey) ](P__D._1)))
**/

 val x244 = L__D_1 
val x250 = x244.map(x245 => { val x246 = x245.l_orderkey 
val x247 = x245.l_quantity 
val x248 = x245.l_partkey 
val x249 = Record302(x246, x247, x248) 
x249 }) 
val x251 = P__D_1 
val x256 = x251.map(x252 => { val x253 = x252.p_name 
val x254 = x252.p_partkey 
val x255 = Record303(x253, x254) 
x255 }) 
val x261 = { val out1 = x250.map{ case x257 => ({val x259 = x257.l_partkey 
x259}, x257) }
  val out2 = x256.map{ case x258 => ({val x260 = x258.p_partkey 
x260}, x258) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x268 = x261.map{ case (x262, x263) => 
   val x264 = x262.l_orderkey 
val x265 = x263.p_name 
val x266 = x262.l_quantity 
val x267 = Record304(x264, x265, x266) 
x267 
} 
val ljp__D_1 = x268
val x269 = ljp__D_1
//ljp__D_1.collect.foreach(println(_))

/**
M_flat1 :=  REDUCE[ (c_name := x241.c_name,c_orders := (c__Fc_custkey := x241.c_custkey)) / true ](C__D._1)
**/

val x270 = C__D_1 
val x276 = x270.map{ case x271 => 
   val x272 = x271.c_name 
val x273 = x271.c_custkey 
val x274 = Record305(x273) 
val x275 = Record306(x272, x274) 
x275 
} 
val M_flat1 = x276
val x277 = M_flat1
M_flat1.count
//M_flat1.collect.foreach(println(_))
/**
M_flat2 :=  REDUCE[ (key := (c__Fc_custkey := x242.o_custkey),value := { (o_orderdate := x242.o_orderdate,o_parts := (o__Fo_orderkey := x242.o_orderkey)) }) / true ](O__D._1)
**/

val x278 = O__D_1 
val x288 = x278.map{ case x279 => 
   val x280 = x279.o_custkey 
val x281 = Record305(x280) 
val x282 = x279.o_orderdate 
val x283 = x279.o_orderkey 
val x284 = Record307(x283) 
val x285 = Record308(x282, x284) 
val x286 = List(x285) 
val x287 = (x281, x286) 
x287 
} 
val M_flat2 = x288
val x289 = M_flat2
M_flat2.count
//M_flat2.collect.foreach(println(_))
/**
M_flat3 :=  REDUCE[ (key := (o__Fo_orderkey := x243.l_orderkey),value := { (p_name := x243.p_name,l_qty := x243.l_qty) }) / true ](ljp__D._1)
**/

val x290 = ljp__D_1 
val x299 = x290.map{ case x291 => 
   val x292 = x291.l_orderkey 
val x293 = Record307(x292) 
val x294 = x291.p_name 
val x295 = x291.l_qty 
val x296 = Record309(x294, x295) 
val x297 = List(x296) 
val x298 = (x293, x297) 
x298 
} 
val M_flat3 = x299
val x300 = M_flat3
//M_flat3.collect.foreach(println(_))
x300.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery1SparkOpt"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
