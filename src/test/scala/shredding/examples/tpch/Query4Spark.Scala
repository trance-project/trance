
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record317(c_name: String, c_custkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record318(o_orderdate: String, o_orderkey: Int, o_custkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record319(l_quantity: Double, l_partkey: Int, l_orderkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record320(p_name: String, p_partkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record322(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record324(o_orderdate: String, o_parts: Iterable[Record322], uniqueId: Long) extends CaseClassRecord
case class Record376(c_name: String, p_name: String, month: String, t_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Query1Out(c_name: String, c_orders: Iterable[Record324], uniqueId: Long) extends CaseClassRecord
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
   val Query4 = { val x256 = C.map(x252 => { val x253 = x252.c_name 
val x254 = x252.c_custkey 
val x255 = Record317(x253, x254, newId) 
x255 }) 
val x262 = O.map(x257 => { val x258 = x257.o_orderdate 
val x259 = x257.o_orderkey 
val x260 = x257.o_custkey 
val x261 = Record318(x258, x259, x260, newId) 
x261 }) 
val x267 = { val out1 = x256.map{ case x263 => ({val x265 = x263.c_custkey 
x265}, x263) }
  val out2 = x262.map{ case x264 => ({val x266 = x264.o_custkey 
x266}, x264) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x273 = L.map(x268 => { val x269 = x268.l_quantity 
val x270 = x268.l_partkey 
val x271 = x268.l_orderkey 
val x272 = Record319(x269, x270, x271, newId) 
x272 }) 
val x279 = { val out1 = x267.map{ case (x274, x275) => ({val x277 = x275.o_orderkey 
x277}, (x274, x275)) }
  val out2 = x273.map{ case x276 => ({val x278 = x276.l_orderkey 
x278}, x276) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x284 = P.map(x280 => { val x281 = x280.p_name 
val x282 = x280.p_partkey 
val x283 = Record320(x281, x282, newId) 
x283 }) 
val x291 = { val out1 = x279.map{ case ((x285, x286), x287) => ({val x289 = x287.l_partkey 
x289}, ((x285, x286), x287)) }
  val out2 = x284.map{ case x288 => ({val x290 = x288.p_partkey 
x290}, x288) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x302 = x291.map{ case (((x292, x293), x294), x295) => val x301 = (x294,x295) 
x301 match {
   case (_,null) => ({val x296 = (x292,x293) 
x296}, null) 
   case x300 => ({val x296 = (x292,x293) 
x296}, {val x297 = x295.p_name 
val x298 = x294.l_quantity 
val x299 = Record322(x297, x298, newId) 
x299})
 }
}.groupByKey() 
val x312 = x302.map{ case ((x303, x304), x305) => val x311 = (x304,x305) 
x311 match {
   case (_,null) => ({val x306 = x303.c_name 
val x307 = (x306) 
x307}, null) 
   case x310 => ({val x306 = x303.c_name 
val x307 = (x306) 
x307}, {val x308 = x304.o_orderdate 
val x309 = Record324(x308, x305, newId) 
x309})
 }
}.groupByKey() 
val x316 = x312.map{ case (x313, x314) => 
   val x315 = Query1Out(x313, x314, newId) 
x315 
} 
x316 } 
 Query4.cache 
 Query4.count
   var start0 = System.currentTimeMillis()
   def f() {
     val x334 = Query4.map(x330 => { val x331 = x330.c_name 
val x332 = x330.c_orders 
val x333 = Query1Out(x331, x332, newId) 
x333 }) 
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
val x368 = x349.map{ case (((x350, x351), x352), x353) => val x367 = (x353) 
x367 match {
   case (null) => ({val x354 = x350.c_name 
val x355 = x352.p_name 
val x356 = x351.o_orderdate 
val x357 = (x354,x355,x356) 
x357}, 0.0)
   case x366 => ({val x354 = x350.c_name 
val x355 = x352.p_name 
val x356 = x351.o_orderdate 
val x357 = (x354,x355,x356) 
x357}, {val x358 = x353.p_name 
val x359 = x352.p_name 
val x360 = x358 == x359 
val x361 = x353.l_qty 
val x362 = x352.l_qty 
val x363 = x361 == x362 
val x364 = x360 && x363 
val x365 = 
 if ({x364})
 {  x361}
 else 0.0  
x365})
 }
}.reduceByKey(_ + _) 
val x374 = x368.map{ case ((x369, x370, x371), x372) => 
   val x373 = Record376(x369, x371, x370, x372, newId) 
x373 
} 
x374.count
   }
   f
   var end0 = System.currentTimeMillis() - start0
   println("Query4Spark"+sf+","+Config.datapath+","+end0)
 }
}
