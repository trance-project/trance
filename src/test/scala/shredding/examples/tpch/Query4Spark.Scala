
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
case class Record278(p_name: String, l_qty: Double, uniqueId: Long)
case class Record280(o_orderdate: String, o_parts: List[Record278], uniqueId: Long)
case class Record325(c_name: String, p_name: String, month: String, t_qty: Double, uniqueId: Long)
case class Query1Out(c_name: String, c_orders: List[Record280], uniqueId: Long)
object Query4Spark {
 def main(args: Array[String]){
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query4Spark")
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
   val Query4 = { val x227 = C 
val x229 = O 
val x234 = { val out1 = x227.map{ case x230 => ({val x232 = x230.c_custkey 
x232}, x230) }
  val out2 = x229.map{ case x231 => ({val x233 = x231.o_custkey 
x233}, x231) }
  out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x236 = L 
val x242 = { val out1 = x234.map{ case (x237, x238) => ({val x240 = x238.o_orderkey 
x240}, (x237, x238)) }
  val out2 = x236.map{ case x239 => ({val x241 = x239.l_orderkey 
x241}, x239) }
  out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x244 = P 
val x251 = { val out1 = x242.map{ case ((x245, x246), x247) => ({val x249 = x247.l_partkey 
x249}, ((x245, x246), x247)) }
  val out2 = x244.map{ case x248 => ({val x250 = x248.p_partkey 
x250}, x248) }
  out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x262 = x251.map{ case (((x252, x253), x254), x255) => val x261 = (x254,x255) 
x261 match {
   case (_,null) => ({val x256 = (x252,x253) 
x256}, Nil) 
   case x260 => ({val x256 = (x252,x253) 
x256}, List({val x257 = x255.p_name 
val x258 = x254.l_quantity 
val x259 = Record278(x257, x258, newId) 
x259}))
 }
}.foldByKey(Nil){ case (acc277, x260) => acc277 ++ x260 } 
val x271 = x262.map{ case ((x263, x264), x265) => val x270 = (x264,x265) 
x270 match {
   case (_,null) => ({val x266 = (x263) 
x266}, Nil) 
   case x269 => ({val x266 = (x263) 
x266}, List({val x267 = x264.o_orderdate 
val x268 = Record280(x267, x265, newId) 
x268}))
 }
}.foldByKey(Nil){ case (acc279, x269) => acc279 ++ x269 } 
val x276 = x271.map{ case (x272, x273) => 
   val x274 = x272.c_name 
val x275 = Query1Out(x274, x273, newId) 
x275 
} 
x276 } 
 Query4.cache 
 Query4.count
   var start0 = System.currentTimeMillis()
   def f() {
     val x287 = Query4 
val x291 = x287.flatMap{ case x288 => x288 match {
   case null => List((x288, null))
   case _ =>
   val x289 = x288.c_orders 
x289 match {
     case x290 => x290.map{ case v2 => (x288, v2) }
  }
 }} 
val x296 = x291.flatMap{ case (x292, x293) => (x292, x293) match {
   case (_, null) => List(((x292, x293), null))
   case _ =>
   val x294 = x293.o_parts 
x294 match {
     case x295 => x295.map{ case v2 => ((x292, x293), v2) }
  }
 }} 
val x305 = x296.flatMap{ case ((x297, x298), x299) => ((x297, x298), x299) match {
   case (_, null) => List((((x297, x298), x299), null))
   case _ => 
   {val x300 = x298.o_parts 
x300} match {
     case Nil => List((((x297, x298), x299), null))
     case lst => lst.map{ case x301 => if ({val x302 = x301.p_name 
val x303 = x299.p_name 
val x304 = x302 == x303 
x304}) { (((x297, x298), x299), x301) } else { (((x297, x298), x299), null) } }
  }
 }} 
val x314 = x305.map{ case (((x306, x307), x308), x309) => val x313 = (x309) 
x313 match {
   case (null) => ({val x310 = (x306,x307,x308) 
x310}, 0.0)
   case x312 => ({val x310 = (x306,x307,x308) 
x310}, {val x311 = x309.l_qty 
x311})
 }
}.foldByKey(0.0){ case (acc324, x312) => acc324 + x312 } 
val x323 = x314.map{ case ((x315, x316, x317), x318) => 
   val x319 = x315.c_name 
val x320 = x317.p_name 
val x321 = x316.o_orderdate 
val x322 = Record325(x319, x320, x321, x318, newId) 
x322 
} 
x323.collect.foreach(println(_))//count
   }
   f
   var end0 = System.currentTimeMillis() - start0
   println(Query4Spark+","+Config.datapath+","+end0)
 }
}
