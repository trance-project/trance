
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record362(c_name: String, c_custkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record363(o_orderdate: String, o_orderkey: Int, o_custkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record364(l_quantity: Double, l_partkey: Int, l_orderkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record365(p_name: String, p_partkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record367(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record369(o_orderdate: String, o_parts: Iterable[Record367], uniqueId: Long) extends CaseClassRecord
case class Query1Out(c_name: String, c_orders: Iterable[Record369], uniqueId: Long) extends CaseClassRecord
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
   val x301 = C.map(x297 => { val x298 = x297.c_name 
val x299 = x297.c_custkey 
val x300 = Record362(x298, x299, newId) 
x300 }) 
val x307 = O.map(x302 => { val x303 = x302.o_orderdate 
val x304 = x302.o_orderkey 
val x305 = x302.o_custkey 
val x306 = Record363(x303, x304, x305, newId) 
x306 }) 
val x312 = { val out1 = x301.map{ case x308 => ({val x310 = x308.c_custkey 
x310}, x308) }
  val out2 = x307.map{ case x309 => ({val x311 = x309.o_custkey 
x311}, x309) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x318 = L.map(x313 => { val x314 = x313.l_quantity 
val x315 = x313.l_partkey 
val x316 = x313.l_orderkey 
val x317 = Record364(x314, x315, x316, newId) 
x317 }) 
val x324 = { val out1 = x312.map{ case (x319, x320) => ({val x322 = x320.o_orderkey 
x322}, (x319, x320)) }
  val out2 = x318.map{ case x321 => ({val x323 = x321.l_orderkey 
x323}, x321) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x329 = P.map(x325 => { val x326 = x325.p_name 
val x327 = x325.p_partkey 
val x328 = Record365(x326, x327, newId) 
x328 }) 
val x336 = { val out1 = x324.map{ case ((x330, x331), x332) => ({val x334 = x332.l_partkey 
x334}, ((x330, x331), x332)) }
  val out2 = x329.map{ case x333 => ({val x335 = x333.p_partkey 
x335}, x333) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x347 = x336.map{ case (((x337, x338), x339), x340) => val x346 = (x339,x340) 
x346 match {
   case (_,null) => ({val x341 = (x337,x338) 
x341}, null) 
   case x345 => ({val x341 = (x337,x338) 
x341}, {val x342 = x340.p_name 
val x343 = x339.l_quantity 
val x344 = Record367(x342, x343, newId) 
x344})
 }
}.groupByKey() 
val x356 = x347.map{ case ((x348, x349), x350) => val x355 = (x349,x350) 
x355 match {
   case (_,null) => ({val x351 = (x348) 
x351}, null) 
   case x354 => ({val x351 = (x348) 
x351}, {val x352 = x349.o_orderdate 
val x353 = Record369(x352, x350, newId) 
x353})
 }
}.groupByKey() 
val x361 = x356.map{ case (x357, x358) => 
   val x359 = x357.c_name 
val x360 = Query1Out(x359, x358, newId) 
x360 
} 
x361.count
   var end0 = System.currentTimeMillis() - start0
   println("Query1Spark"+sf+","+Config.datapath+","+end0)
 }
}
