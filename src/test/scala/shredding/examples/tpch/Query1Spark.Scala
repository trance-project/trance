
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.util.{HashMap => JHashMap}
import sprkloader._
case class Record278(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record280(o_orderdate: String, o_parts: Iterable[Record278], uniqueId: Long) extends CaseClassRecord
case class Query1Out(c_name: String, c_orders: Iterable[Record280], uniqueId: Long) extends CaseClassRecord
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
   val x227 = C 
val x229 = O 
val x234 = { val out1 = x227.map{ case x230 => ({val x232 = x230.c_custkey 
x232}, x230) }
  val out2 = x229.map{ case x231 => ({val x233 = x231.o_custkey 
x233}, x231) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x236 = L 
val x242 = { val out1 = x234.map{ case (x237, x238) => ({val x240 = x238.o_orderkey 
x240}, (x237, x238)) }
  val out2 = x236.map{ case x239 => ({val x241 = x239.l_orderkey 
x241}, x239) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x244 = P 
val x251 = { val out1 = x242.map{ case ((x245, x246), x247) => ({val x249 = x247.l_partkey 
x249}, ((x245, x246), x247)) }
  val out2 = x244.map{ case x248 => ({val x250 = x248.p_partkey 
x250}, x248) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x262 = x251.map{ case (((x252, x253), x254), x255) => val x261 = (x254,x255) 
x261 match {
   case (_,null) => ({val x256 = (x252,x253) 
x256}, null) 
   case x260 => ({val x256 = (x252,x253) 
x256}, {val x257 = x255.p_name 
val x258 = x254.l_quantity 
val x259 = Record278(x257, x258, newId) 
x259})
 }
}.groupByKey() 
val x271 = x262.map{ case ((x263, x264), x265) => val x270 = (x264,x265) 
x270 match {
   case (_,null) => ({val x266 = (x263) 
x266}, null) 
   case x269 => ({val x266 = (x263) 
x266}, {val x267 = x264.o_orderdate 
val x268 = Record280(x267, x265, newId) 
x268})
 }
}.groupByKey() 
val x276 = x271.map{ case (x272, x273) => 
   val x274 = x272.c_name 
val x275 = Query1Out(x274, x273, newId) 
x275 
} 
x276.count
   var end0 = System.currentTimeMillis() - start0
   println("Query1Spark"+sf+","+Config.datapath+","+end0)
 }
}
