
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
case class Record265(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record267(o_orderdate: String, o_parts: List[Record265], uniqueId: Long) extends CaseClassRecord
case class Record312(c_name: String, p_name: String, month: String, t_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Query1Out(c_name: String, c_orders: List[Record267], uniqueId: Long) extends CaseClassRecord
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
   val Query4 = { val x214 = C 
val x216 = O 
val x221 = { val out1 = x214.map{ case x217 => ({val x219 = x217.c_custkey 
x219}, x217) }
  val out2 = x216.map{ case x218 => ({val x220 = x218.o_custkey 
x220}, x218) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x223 = L 
val x229 = { val out1 = x221.map{ case (x224, x225) => ({val x227 = x225.o_orderkey 
x227}, (x224, x225)) }
  val out2 = x223.map{ case x226 => ({val x228 = x226.l_orderkey 
x228}, x226) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x231 = P 
val x238 = { val out1 = x229.map{ case ((x232, x233), x234) => ({val x236 = x234.l_partkey 
x236}, ((x232, x233), x234)) }
  val out2 = x231.map{ case x235 => ({val x237 = x235.p_partkey 
x237}, x235) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x249 = x238.map{ case (((x239, x240), x241), x242) => val x248 = (x241,x242) 
x248 match {
   case (_,null) => ({val x243 = (x239,x240) 
x243}, Nil) 
   case x247 => ({val x243 = (x239,x240) 
x243}, List({val x244 = x242.p_name 
val x245 = x241.l_quantity 
val x246 = Record265(x244, x245, newId) 
x246}))
 }
}.foldByKey(Nil){ case (acc264, x247) => acc264 ++ x247 } 
val x258 = x249.map{ case ((x250, x251), x252) => val x257 = (x251,x252) 
x257 match {
   case (_,null) => ({val x253 = (x250) 
x253}, Nil) 
   case x256 => ({val x253 = (x250) 
x253}, List({val x254 = x251.o_orderdate 
val x255 = Record267(x254, x252, newId) 
x255}))
 }
}.foldByKey(Nil){ case (acc266, x256) => acc266 ++ x256 } 
val x263 = x258.map{ case (x259, x260) => 
   val x261 = x259.c_name 
val x262 = Query1Out(x261, x260, newId) 
x262 
} 
x263 } 
 Query4.cache 
 Query4.count
   var start0 = System.currentTimeMillis()
   def f() {
     val x274 = Query4 
val x278 = x274.flatMap{ case x275 => x275 match {
   case null => List((x275, null))
   case _ =>
   val x276 = x275.c_orders 
x276 match {
     case x277 => x277.map{ case v2 => (x275, v2) }
  }
 }} 
val x283 = x278.flatMap{ case (x279, x280) => (x279, x280) match {
   case (_, null) => List(((x279, x280), null))
   case _ =>
   val x281 = x280.o_parts 
x281 match {
     case x282 => x282.map{ case v2 => ((x279, x280), v2) }
  }
 }} 
val x292 = x283.flatMap{ case ((x284, x285), x286) => ((x284, x285), x286) match {
   case (_, null) => List((((x284, x285), x286), null))
   case _ => 
   {val x287 = x285.o_parts 
x287} match {
     case Nil => List((((x284, x285), x286), null))
     case lst => lst.map{ case x288 => if ({val x289 = x288.p_name 
val x290 = x286.p_name 
val x291 = x289 == x290 
x291}) { (((x284, x285), x286), x288) } else { (((x284, x285), x286), null) } }
  }
 }} 
val x301 = x292.map{ case (((x293, x294), x295), x296) => val x300 = (x296) 
x300 match {
   case (null) => ({val x297 = (x293,x294,x295) 
x297}, 0.0)
   case x299 => ({val x297 = (x293,x294,x295) 
x297}, {val x298 = x296.l_qty 
x298})
 }
}.foldByKey(0.0){ case (acc311, x299) => acc311 + x299 } 
val x310 = x301.map{ case ((x302, x303, x304), x305) => 
   val x306 = x302.c_name 
val x307 = x304.p_name 
val x308 = x303.o_orderdate 
val x309 = Record312(x306, x307, x308, x305, newId) 
x309 
} 
x310.count
   }
   f
   var end0 = System.currentTimeMillis() - start0
   println("Query4Spark"+sf+","+Config.datapath+","+end0)
 }
}
