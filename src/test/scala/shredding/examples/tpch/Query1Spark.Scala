
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record319(c_name: String, c_custkey: Int)
case class Record320(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record321(l_quantity: Double, l_partkey: Int, l_orderkey: Int)
case class Record322(p_name: String, p_partkey: Int)
case class Record324(p_name: String, l_qty: Double)
case class Record326(o_orderdate: String, o_parts: Iterable[Record324])
case class Query1Out(c_name: String, c_orders: Iterable[Record326])
object Query1Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query1Spark"+sf)
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
   var start0 = System.currentTimeMillis()
   val x258 = C.map(x254 => { val x255 = x254.c_name 
val x256 = x254.c_custkey 
val x257 = Record319(x255, x256) 
x257 }) 
val x264 = O.map(x259 => { val x260 = x259.o_orderdate 
val x261 = x259.o_orderkey 
val x262 = x259.o_custkey 
val x263 = Record320(x260, x261, x262) 
x263 }) 
val x269 = { val out1 = x258.map{ case x265 => ({val x267 = x265.c_custkey 
x267}, x265) }
  val out2 = x264.map{ case x266 => ({val x268 = x266.o_custkey 
x268}, x266) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x275 = L.map(x270 => { val x271 = x270.l_quantity 
val x272 = x270.l_partkey 
val x273 = x270.l_orderkey 
val x274 = Record321(x271, x272, x273) 
x274 }) 
val x281 = { val out1 = x269.map{ case (x276, x277) => ({val x279 = x277.o_orderkey 
x279}, (x276, x277)) }
  val out2 = x275.map{ case x278 => ({val x280 = x278.l_orderkey 
x280}, x278) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x286 = P.map(x282 => { val x283 = x282.p_name 
val x284 = x282.p_partkey 
val x285 = Record322(x283, x284) 
x285 }) 
val x293 = { val out1 = x281.map{ case ((x287, x288), x289) => ({val x291 = x289.l_partkey 
x291}, ((x287, x288), x289)) }
  val out2 = x286.map{ case x290 => ({val x292 = x290.p_partkey 
x292}, x290) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x304 = x293.map{ case (((x294, x295), x296), x297) => val x303 = (x296,x297) 
x303 match {
   case (_,null) => ({val x298 = (x294,x295) 
x298}, null) 
   case x302 => ({val x298 = (x294,x295) 
x298}, {val x299 = x297.p_name 
val x300 = x296.l_quantity 
val x301 = Record324(x299, x300) 
x301})
 }
}.groupByKey() 
val x313 = x304.map{ case ((x305, x306), x307) => val x312 = (x306,x307) 
x312 match {
   case (_,null) => ({val x308 = (x305) 
x308}, null) 
   case x311 => ({val x308 = (x305) 
x308}, {val x309 = x306.o_orderdate 
val x310 = Record326(x309, x307) 
x310})
 }
}.groupByKey() 
val x318 = x313.map{ case (x314, x315) => 
   val x316 = x314.c_name 
val x317 = Query1Out(x316, x315) 
x317 
} 
x318.count
   var end0 = System.currentTimeMillis() - start0
   println("Query1Spark"+sf+","+Config.datapath+","+end0)
 }
}
