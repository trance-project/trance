
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record294(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record295(p_name: String, p_partkey: Int)
case class Record296(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record297(c_name: String, c_custkey: Int)
case class Record298(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record300(p_name: String, l_qty: Double)
case class Record302(o_orderdate: String, o_parts: Iterable[Record300])
case class Record303(c_name: String, c_orders: Iterable[Record302])
object Query1SparkUnnest {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query1SparkUnnest"+sf)
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
/**
ljp :=  REDUCE[ (l_orderkey := x214.l_orderkey,p_name := x215.p_name,l_qty := x214.l_quantity) / true ]( <-- (x214,x215) -- (
 <-- (x214) -- SELECT[ true, (l_orderkey := x214.l_orderkey,l_quantity := x214.l_quantity,l_partkey := x214.l_partkey) ](L)) JOIN[x214.l_partkey = x215.p_partkey](

   <-- (x215) -- SELECT[ true, (p_name := x215.p_name,p_partkey := x215.p_partkey) ](P)))
**/
 
 val x226 = L.map(x221 => { val x222 = x221.l_orderkey 
val x223 = x221.l_quantity 
val x224 = x221.l_partkey 
val x225 = Record294(x222, x223, x224) 
x225 }) 
val x231 = P.map(x227 => { val x228 = x227.p_name 
val x229 = x227.p_partkey 
val x230 = Record295(x228, x229) 
x230 }) 
val x236 = { val out1 = x226.map{ case x232 => ({val x234 = x232.l_partkey 
x234}, x232) }
  val out2 = x231.map{ case x233 => ({val x235 = x233.p_partkey 
x235}, x233) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x243 = x236.map{ case (x237, x238) => 
   val x239 = x237.l_orderkey 
val x240 = x238.p_name 
val x241 = x237.l_quantity 
val x242 = Record296(x239, x240, x241) 
x242 
} 
val ljp = x243
val x244 = ljp
//ljp.collect.foreach(println(_))
val x249 = C.map(x245 => { val x246 = x245.c_name 
val x247 = x245.c_custkey 
val x248 = Record297(x246, x247) 
x248 }) 
val x255 = O.map(x250 => { val x251 = x250.o_orderdate 
val x252 = x250.o_orderkey 
val x253 = x250.o_custkey 
val x254 = Record298(x251, x252, x253) 
x254 }) 
val x260 = { val out1 = x249.map{ case x256 => ({val x258 = x256.c_custkey 
x258}, x256) }
  val out2 = x255.map{ case x257 => ({val x259 = x257.o_custkey 
x259}, x257) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x262 = ljp 

/**
 REDUCE[ (c_name := x216.c_name,c_orders := x220) / true ]( <-- (x216,x220) -- NEST[ U / (o_orderdate := x217.o_orderdate,o_parts := x219) / (x216), true / (x217,x219) ]( <-- (x216,x217,x219) -- NEST[ U / (p_name := x218.p_name,l_qty := x218.l_qty) / (x216,x217), true / (x218) ]( <-- (x216,x217,x218) -- ( <-- (x216,x217) -- (
 <-- (x216) -- SELECT[ true, (c_name := x216.c_name,c_custkey := x216.c_custkey) ](C)) OUTERJOIN[x216.c_custkey = x217.o_custkey](

   <-- (x217) -- SELECT[ true, (o_orderdate := x217.o_orderdate,o_orderkey := x217.o_orderkey,o_custkey := x217.o_custkey) ](O))) OUTERJOIN[x217.o_orderkey = x218.l_orderkey](

   <-- (x218) -- SELECT[ true, x218 ](ljp)))))
**/

val x268 = { val out1 = x260.map{ case (x263, x264) => ({val x266 = x264.o_orderkey 
x266}, (x263, x264)) }
  val out2 = x262.map{ case x265 => ({val x267 = x265.l_orderkey 
x267}, x265) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x278 = x268.flatMap{ case ((x269, x270), x271) => val x277 = (x271) 
x277 match {
   case (null) => Nil 
   case x276 => List(({val x272 = (x269,x270) 
x272}, {val x273 = x271.p_name 
val x274 = x271.l_qty 
val x275 = Record300(x273, x274) 
x275}))
 }
}.groupByKey() 
val x287 = x278.flatMap{ case ((x279, x280), x281) => val x286 = (x280,x281) 
x286 match {
   case (_,null) => Nil 
   case x285 => List(({val x282 = (x279) 
x282}, {val x283 = x280.o_orderdate 
val x284 = Record302(x283, x281) 
x284}))
 }
}.groupByKey() 
val x292 = x287.map{ case (x288, x289) => 
   val x290 = x288.c_name 
val x291 = Record303(x290, x289) 
x291 
} 
x292.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("Query1SparkUnnest"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
