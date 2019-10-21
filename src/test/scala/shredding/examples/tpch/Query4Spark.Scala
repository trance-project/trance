
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record323(c_custkey: Int, c_name: String)
case class Record324(o_orderkey: Int, o_orderdate: String, o_custkey: Int)
case class Record325(l_quantity: Double, l_partkey: Int, l_orderkey: Int)
case class Record326(p_name: String, p_partkey: Int)
case class Record328(p_name: String, l_qty: Double)
case class Record330(o_id: Int, o_orderdate: String, o_parts: Iterable[Record328])
case class Record365(c_name: String, c_orders: Iterable[Record330])
case class Record367(c_name: String, p_name: String, month: String)
case class Query1Out(c_id: Int, c_name: String, c_orders: Iterable[Record330])
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
   val Query4 = { val x260 = C.map(x256 => { val x257 = x256.c_custkey 
val x258 = x256.c_name 
val x259 = Record323(x257, x258) 
x259 }) 
val x266 = O.map(x261 => { val x262 = x261.o_orderkey 
val x263 = x261.o_orderdate 
val x264 = x261.o_custkey 
val x265 = Record324(x262, x263, x264) 
x265 }) 
val x271 = { val out1 = x260.map{ case x267 => ({val x269 = x267.c_custkey 
x269}, x267) }
  val out2 = x266.map{ case x268 => ({val x270 = x268.o_custkey 
x270}, x268) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x277 = L.map(x272 => { val x273 = x272.l_quantity 
val x274 = x272.l_partkey 
val x275 = x272.l_orderkey 
val x276 = Record325(x273, x274, x275) 
x276 }) 
val x283 = { val out1 = x271.map{ case (x278, x279) => ({val x281 = x279.o_orderkey 
x281}, (x278, x279)) }
  val out2 = x277.map{ case x280 => ({val x282 = x280.l_orderkey 
x282}, x280) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x288 = P.map(x284 => { val x285 = x284.p_name 
val x286 = x284.p_partkey 
val x287 = Record326(x285, x286) 
x287 }) 
val x295 = { val out1 = x283.map{ case ((x289, x290), x291) => ({val x293 = x291.l_partkey 
x293}, ((x289, x290), x291)) }
  val out2 = x288.map{ case x292 => ({val x294 = x292.p_partkey 
x294}, x292) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x306 = x295.map{ case (((x296, x297), x298), x299) => val x305 = (x298,x299) 
x305 match {
   case (_,null) => ({val x300 = (x296,x297) 
x300}, null) 
   case x304 => ({val x300 = (x296,x297) 
x300}, {val x301 = x299.p_name 
val x302 = x298.l_quantity 
val x303 = Record328(x301, x302) 
x303})
 }
}.groupByKey() 
val x316 = x306.map{ case ((x307, x308), x309) => val x315 = (x308,x309) 
x315 match {
   case (_,null) => ({val x310 = (x307) 
x310}, null) 
   case x314 => ({val x310 = (x307) 
x310}, {val x311 = x308.o_orderkey 
val x312 = x308.o_orderdate 
val x313 = Record330(x311, x312, x309) 
x313})
 }
}.groupByKey() 
val x322 = x316.map{ case (x317, x318) => 
   val x319 = x317.c_custkey 
val x320 = x317.c_name 
val x321 = Query1Out(x319, x320, x318) 
x321 
} 
x322 } 
 Query4.cache 
 Query4.count
   var start0 = System.currentTimeMillis()
   def f() {
     val x342 = Query4.filter(x336 => { val x337 = x336.c_id 
val x338 = x337 < 1000 
x338 }).map(x336 => { val x339 = x336.c_name 
val x340 = x336.c_orders 
val x341 = Record365(x339, x340) 
x341 }) 
val x348 = x342.flatMap{ case x343 => x343 match {
   case null => List((x343, null))
   case _ =>
   val x344 = x343.c_orders 
x344 match {
     case x345 => x345.map{ case v2 => (x343, v2) }
  }
 }}.filter{ case (x343, x345) => {val x346 = x345.o_id 
val x347 = x346 < 1000 
x347} } 
val x353 = x348.flatMap{ case (x349, x350) => (x349, x350) match {
   case (_, null) => List(((x349, x350), null))
   case _ =>
   val x351 = x350.o_parts 
x351 match {
     case x352 => x352.map{ case v2 => ((x349, x350), v2) }
  }
 }} 
val x364 = x353.map{ case ((x354, x355), x356) => 
   ({val x357 = x354.c_name 
val x358 = x356.p_name 
val x359 = x355.o_orderdate 
val x360 = Record367(x357, x358, x359) 
x360}, {val x361 = x356.l_qty 
x361})
}.reduceByKey(_ + _) 
x364.count
   }
   f
   var end0 = System.currentTimeMillis() - start0
   println("Query4Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
