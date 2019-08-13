
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
case class Record425(P__F: Int, L__F: Int, O__F: Int, c__F: Customer, uniqueId: Long) extends CaseClassRecord
case class Record428(o__F: Orders, P__F: Int, L__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record429(o_orderdate: String, o_parts: Record428, uniqueId: Long) extends CaseClassRecord
case class Record433(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class RecM_flat2(_1: Record425, _2: List[Record429], uniqueId: Long) extends CaseClassRecord
case class RecM_flat3(_1: Record428, _2: List[Record433], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx3(lbl: Record428, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx2(lbl: Record425, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx1(lbl: Q1Flat, uniqueId: Long) extends CaseClassRecord
case class RecM_flat1(c_name: String, c_orders: Record425, uniqueId: Long) extends CaseClassRecord
object ShredQuery1Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1Spark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   
val tpch = TPCHLoader(spark)
val C__F = 1
val C__D_1 = tpch.loadCustomers()
C__D_1.cache
C__D_1.count
val O__F = 2
val O__D_1 = tpch.loadOrders()
O__D_1.cache
O__D_1.count
val L__F = 3
val L__D_1 = tpch.loadLineitem()
L__D_1.cache
L__D_1.count
val P__F = 4
val P__D_1 = tpch.loadPart()
P__D_1.cache
P__D_1.count
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
   var start0 = System.currentTimeMillis()
   val x320 = Q1Flat(P__F, C__F, L__F, O__F, newId) 
val x321 = RecM_ctx1(x320, newId) 
val M_ctx1 = x321
//println("M_ctx1")
val x322 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x323 = C__D_1 
val x325 = x323 
val x334 = x325.map{ case x326 => 
   val x327 = x326.c_name 
val x328 = M_ctx1.lbl 
val x329 = x328.P__F 
val x330 = x328.L__F 
val x331 = x328.O__F 
val x332 = Record425(x329, x330, x331, x326, newId) 
val x333 = RecM_flat1(x327, x332, newId) 
x333 
} 
val M_flat1 = x334
//println("M_flat1")
val x335 = M_flat1
//M_flat1.collect.foreach(println(_))
val x337 = M_flat1 
val x341 = x337.map{ case x338 => 
   val x339 = x338.c_orders 
val x340 = RecM_ctx2(x339, newId) 
x340 
} 
val x342 = x341.distinct 
val M_ctx2 = x342
//println("M_ctx2")
val x343 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x345 = M_ctx2 
val x346 = O__D_1 
val x348 = x346 
val x355 = { val out1 = x345.map{ case x349 => ({val x351 = x349.lbl 
val x352 = x351.c__F 
val x353 = x352.c_custkey 
x353}, x349) }
  val out2 = x348.map{ case x350 => ({val x354 = x350.o_custkey 
x354}, x350) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x367 = x355.map{ case (x356, x357) => val x366 = (x357) 
x366 match {
   case (null) => ({val x358 = (x356) 
x358}, Nil) 
   case x365 => ({val x358 = (x356) 
x358}, List({val x359 = x357.o_orderdate 
val x360 = x356.lbl 
val x361 = x360.P__F 
val x362 = x360.L__F 
val x363 = Record428(x357, x361, x362, newId) 
val x364 = Record429(x359, x363, newId) 
x364}))
 }
}.foldByKey(Nil){ case (acc427, x365) => acc427 ++ x365 } 
val x372 = x367.map{ case (x368, x369) => 
   val x370 = x368.lbl 
val x371 = RecM_flat2(x370, x369, newId) 
x371 
} 
val M_flat2 = x372
//println("M_flat2")
val x373 = M_flat2
//M_flat2.collect.foreach(println(_))
val x375 = M_flat2 
val x379 = x375.flatMap{ case x376 => x376 match {
   case null => List((x376, null))
   case _ =>
   val x377 = x376._2 
x377 match {
     case x378 => x378.map{ case v2 => (x376, v2) }
  }
 }} 
val x384 = x379.map{ case (x380, x381) => 
   val x382 = x381.o_parts 
val x383 = RecM_ctx3(x382, newId) 
x383 
} 
val x385 = x384.distinct 
val M_ctx3 = x385
//println("M_ctx3")
val x386 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x388 = M_ctx3 
val x389 = L__D_1 
val x391 = x389 
val x398 = { val out1 = x388.map{ case x392 => ({val x394 = x392.lbl 
val x395 = x394.o__F 
val x396 = x395.o_orderkey 
x396}, x392) }
  val out2 = x391.map{ case x393 => ({val x397 = x393.l_orderkey 
x397}, x393) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x399 = P__D_1 
val x401 = x399 
val x407 = { val out1 = x398.map{ case (x402, x403) => ({val x405 = x403.l_partkey 
x405}, (x402, x403)) }
  val out2 = x401.map{ case x404 => ({val x406 = x404.p_partkey 
x406}, x404) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x417 = x407.map{ case ((x408, x409), x410) => val x416 = (x409,x410) 
x416 match {
   case (_,null) => ({val x411 = (x408) 
x411}, Nil) 
   case x415 => ({val x411 = (x408) 
x411}, List({val x412 = x410.p_name 
val x413 = x409.l_quantity 
val x414 = Record433(x412, x413, newId) 
x414}))
 }
}.foldByKey(Nil){ case (acc432, x415) => acc432 ++ x415 } 
val x422 = x417.map{ case (x418, x419) => 
   val x420 = x418.lbl 
val x421 = RecM_flat3(x420, x419, newId) 
x421 
} 
val M_flat3 = x422
//println("M_flat3")
val x423 = M_flat3
//M_flat3.collect.foreach(println(_))
val res = x423.count
   var end0 = System.currentTimeMillis() - start0
   println("ShredQuery1Spark"+sf+","+Config.datapath+","+end0)
 }
}
