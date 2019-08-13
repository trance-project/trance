
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
case class Record253(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record255(o_orderdate: String, o_parts: List[Record253], uniqueId: Long) extends CaseClassRecord
case class Record300(c_name: String, p_name: String, month: String, t_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Query1Out(c_name: String, c_orders: List[Record255], uniqueId: Long) extends CaseClassRecord
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
   val Query4 = { val x202 = C 
val x204 = O 
val x209 = { val out1 = x202.map{ case x205 => ({val x207 = x205.c_custkey 
x207}, x205) }
  val out2 = x204.map{ case x206 => ({val x208 = x206.o_custkey 
x208}, x206) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x211 = L 
val x217 = { val out1 = x209.map{ case (x212, x213) => ({val x215 = x213.o_orderkey 
x215}, (x212, x213)) }
  val out2 = x211.map{ case x214 => ({val x216 = x214.l_orderkey 
x216}, x214) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x219 = P 
val x226 = { val out1 = x217.map{ case ((x220, x221), x222) => ({val x224 = x222.l_partkey 
x224}, ((x220, x221), x222)) }
  val out2 = x219.map{ case x223 => ({val x225 = x223.p_partkey 
x225}, x223) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x237 = x226.map{ case (((x227, x228), x229), x230) => val x236 = (x229,x230) 
x236 match {
   case (_,null) => ({val x231 = (x227,x228) 
x231}, Nil) 
   case x235 => ({val x231 = (x227,x228) 
x231}, List({val x232 = x230.p_name 
val x233 = x229.l_quantity 
val x234 = Record253(x232, x233, newId) 
x234}))
 }
}.foldByKey(Nil){ case (acc252, x235) => acc252 ++ x235 } 
val x246 = x237.map{ case ((x238, x239), x240) => val x245 = (x239,x240) 
x245 match {
   case (_,null) => ({val x241 = (x238) 
x241}, Nil) 
   case x244 => ({val x241 = (x238) 
x241}, List({val x242 = x239.o_orderdate 
val x243 = Record255(x242, x240, newId) 
x243}))
 }
}.foldByKey(Nil){ case (acc254, x244) => acc254 ++ x244 } 
val x251 = x246.map{ case (x247, x248) => 
   val x249 = x247.c_name 
val x250 = Query1Out(x249, x248, newId) 
x250 
} 
x251 } 
 Query4.cache 
 Query4.count
   var start0 = System.currentTimeMillis()
   def f() {
     val x262 = Query4 
val x266 = x262.flatMap{ case x263 => x263 match {
   case null => List((x263, null))
   case _ =>
   val x264 = x263.c_orders 
x264 match {
     case x265 => x265.map{ case v2 => (x263, v2) }
  }
 }} 
val x271 = x266.flatMap{ case (x267, x268) => (x267, x268) match {
   case (_, null) => List(((x267, x268), null))
   case _ =>
   val x269 = x268.o_parts 
x269 match {
     case x270 => x270.map{ case v2 => ((x267, x268), v2) }
  }
 }} 
val x280 = x271.flatMap{ case ((x272, x273), x274) => ((x272, x273), x274) match {
   case (_, null) => List((((x272, x273), x274), null))
   case _ => 
   {val x275 = x273.o_parts 
x275} match {
     case Nil => List((((x272, x273), x274), null))
     case lst => lst.map{ case x276 => if ({val x277 = x276.p_name 
val x278 = x274.p_name 
val x279 = x277 == x278 
x279}) { (((x272, x273), x274), x276) } else { (((x272, x273), x274), null) } }
  }
 }} 
val x289 = x280.map{ case (((x281, x282), x283), x284) => val x288 = (x284) 
x288 match {
   case (null) => ({val x285 = (x281,x282,x283) 
x285}, 0.0)
   case x287 => ({val x285 = (x281,x282,x283) 
x285}, {val x286 = x284.l_qty 
x286})
 }
}.foldByKey(0.0){ case (acc299, x287) => acc299 + x287 } 
val x298 = x289.map{ case ((x290, x291, x292), x293) => 
   val x294 = x290.c_name 
val x295 = x292.p_name 
val x296 = x291.o_orderdate 
val x297 = Record300(x294, x295, x296, x293, newId) 
x297 
} 
x298.count
   }
   f
   var end0 = System.currentTimeMillis() - start0
   println("Query4Spark"+sf+","+Config.datapath+","+end0)
 }
}
