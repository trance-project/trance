
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
case class Record265(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record267(o_orderdate: String, o_parts: List[Record265], uniqueId: Long) extends CaseClassRecord
case class Query1Out(c_name: String, c_orders: List[Record267], uniqueId: Long) extends CaseClassRecord
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
   val x214 = C 
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
x263.count
   var end0 = System.currentTimeMillis() - start0
   println("Query1Spark"+sf+","+Config.datapath+","+end0)
 }
}
