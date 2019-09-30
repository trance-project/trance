
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record320(c_name: String, c_custkey: Int)
case class Record321(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record322(l_quantity: Double, l_partkey: Int, l_orderkey: Int)
case class Record323(p_name: String, p_partkey: Int)
case class Record325(p_name: String, l_qty: Double)
case class Record327(o_orderdate: String, o_parts: Iterable[Record325])
case class Record374(c_name: String, p_name: String, month: String, t_qty: Double)
case class Query1Out(c_name: String, c_orders: Iterable[Record327])
object Query4Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query4Spark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   
val tpch = TPCHLoader(spark)
val C = tpch.loadCustomers
C.cache
C.count
val O = tpch.loadOrders
O.cache
O.count
val L = tpch.loadLineitem
L.cache
L.count
val P = tpch.loadPart
P.cache
P.count
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
   val Query4 = { val x259 = C.map(x255 => { val x256 = x255.c_name 
val x257 = x255.c_custkey 
val x258 = Record320(x256, x257) 
x258 }) 
val x265 = O.map(x260 => { val x261 = x260.o_orderdate 
val x262 = x260.o_orderkey 
val x263 = x260.o_custkey 
val x264 = Record321(x261, x262, x263) 
x264 }) 
val x270 = { val out1 = x259.map{ case x266 => ({val x268 = x266.c_custkey 
x268}, x266) }
  val out2 = x265.map{ case x267 => ({val x269 = x267.o_custkey 
x269}, x267) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x276 = L.map(x271 => { val x272 = x271.l_quantity 
val x273 = x271.l_partkey 
val x274 = x271.l_orderkey 
val x275 = Record322(x272, x273, x274) 
x275 }) 
val x282 = { val out1 = x270.map{ case (x277, x278) => ({val x280 = x278.o_orderkey 
x280}, (x277, x278)) }
  val out2 = x276.map{ case x279 => ({val x281 = x279.l_orderkey 
x281}, x279) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x287 = P.map(x283 => { val x284 = x283.p_name 
val x285 = x283.p_partkey 
val x286 = Record323(x284, x285) 
x286 }) 
val x294 = { val out1 = x282.map{ case ((x288, x289), x290) => ({val x292 = x290.l_partkey 
x292}, ((x288, x289), x290)) }
  val out2 = x287.map{ case x291 => ({val x293 = x291.p_partkey 
x293}, x291) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x305 = x294.map{ case (((x295, x296), x297), x298) => val x304 = (x297,x298) 
x304 match {
   case (_,null) => ({val x299 = (x295,x296) 
x299}, null) 
   case x303 => ({val x299 = (x295,x296) 
x299}, {val x300 = x298.p_name 
val x301 = x297.l_quantity 
val x302 = Record325(x300, x301) 
x302})
 }
}.groupByKey() 
val x314 = x305.map{ case ((x306, x307), x308) => val x313 = (x307,x308) 
x313 match {
   case (_,null) => ({val x309 = (x306) 
x309}, null) 
   case x312 => ({val x309 = (x306) 
x309}, {val x310 = x307.o_orderdate 
val x311 = Record327(x310, x308) 
x311})
 }
}.groupByKey() 
val x319 = x314.map{ case (x315, x316) => 
   val x317 = x315.c_name 
val x318 = Query1Out(x317, x316) 
x318 
} 
x319 } 
 Query4.cache 
 Query4.count
   var start0 = System.currentTimeMillis()
   def f() {
     val x334 = Query4 
val x338 = x334.flatMap{ case x335 => x335 match {
   case null => List((x335, null))
   case _ =>
   val x336 = x335.c_orders 
x336 match {
     case x337 => x337.map{ case v2 => (x335, v2) }
  }
 }} 
val x343 = x338.flatMap{ case (x339, x340) => (x339, x340) match {
   case (_, null) => List(((x339, x340), null))
   case _ =>
   val x341 = x340.o_parts 
x341 match {
     case x342 => x342.map{ case v2 => ((x339, x340), v2) }
  }
 }} 
val x349 = x343.flatMap{ case ((x344, x345), x346) => ((x344, x345), x346) match {
   case (_, null) => List((((x344, x345), x346), null))
   case _ => 
   {val x347 = x345.o_parts 
x347} match {
     case Nil => List((((x344, x345), x346), null))
     case lst => lst.map{ case x348 => (((x344, x345), x346), x348) }
  }
 }} 
val x362 = x349.map{ case (((x350, x351), x352), x353) => val x361 = (x353) 
x361 match {
   case (null) => ({val x354 = (x350,x351,x352) 
x354}, 0.0)
   case x360 => ({val x354 = (x350,x351,x352) 
x354}, {val x355 = x353.p_name 
val x356 = x352.p_name 
val x357 = x355 == x356 
val x358 = x353.l_qty 
val x359 = 
 if ({x357})
 {  x358}
 else 0.0  
x359})
 }
}.reduceByKey(_ + _) 
val x371 = x362.map{ case ((x363, x364, x365), x366) => 
   val x367 = x363.c_name 
val x368 = x365.p_name 
val x369 = x364.o_orderdate 
val x370 = Record374(x367, x368, x369, x366) 
x370 
} 
val x372 = x371.distinct 
x372.count
   }
   f
   var end0 = System.currentTimeMillis() - start0
   println("Query4Spark"+sf+","+Config.datapath+","+end0)
 }
}
