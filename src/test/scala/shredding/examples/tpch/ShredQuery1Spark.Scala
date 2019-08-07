
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
case class Record474(P__F: Int, L__F: Int, O__F: Int, c__F: Customer, uniqueId: Long)
case class Record475(c_name: String, c_orders: Record474, uniqueId: Long)
case class Record478(o__F: Orders, P__F: Int, L__F: Int, uniqueId: Long)
case class Record479(o_orderdate: String, o_parts: Record478, uniqueId: Long)
case class Record483(p_name: String, l_qty: Double, uniqueId: Long)
case class RecM_flat2(_1: Record474, _2: List[Record479], uniqueId: Long)
case class RecM_flat3(_1: Record478, _2: List[Record483], uniqueId: Long)
case class RecM_flat1(_1: Q1Flat, _2: List[Record475], uniqueId: Long)
case class RecM_ctx3(lbl: Record478, uniqueId: Long)
case class RecM_ctx2(lbl: Record474, uniqueId: Long)
case class RecM_ctx1(lbl: Q1Flat, uniqueId: Long)
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
   val x347 = Q1Flat(P__F, C__F, L__F, O__F, newId) 
val x348 = RecM_ctx1(x347, newId) 
val x349 = List(x348) 
val M_ctx1 = spark.sparkContext.parallelize(x349)
//println("M_ctx1")
val x350 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x352 = M_ctx1 
val x353 = C__D_1 
val x355 = x353 
val x358 = { val out1 = x352.map{ case x356 => ({true}, x356) }
  val out2 = x355.map{ case x357 => ({true}, x357) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x371 = x358.map{ case (x359, x360) => val x370 = (x360) 
x370 match {
   case (null) => ({val x361 = (x359) 
x361}, Nil) => ({val x361 = (x359) 
x361}, Nil) 
   case x369 => ({val x361 = (x359) 
x361}, List({val x362 = x360.c_name 
val x363 = x359.lbl 
val x364 = x363.P__F 
val x365 = x363.L__F 
val x366 = x363.O__F 
val x367 = Record474(x364, x365, x366, x360, newId) 
val x368 = Record475(x362, x367, newId) 
x368}))
 }
}.foldByKey(Nil){ case (acc473, x369) => acc473 ++ x369 } 
val x376 = x371.map{ case (x372, x373) => 
   val x374 = x372.lbl 
val x375 = RecM_flat1(x374, x373, newId) 
x375 
} 
val M_flat1 = x376
//println("M_flat1")
val x377 = M_flat1
//M_flat1.collect.foreach(println(_))
val x379 = M_flat1 
val x383 = x379.flatMap{ case x380 => x380 match {
   case null => List((x380, null))
   case _ =>
   val x381 = x380._2 
x381 match {
     case x382 => x382.map{ case v2 => (x380, v2) }
  }
 }} 
val x388 = x383.map{ case (x384, x385) => 
   val x386 = x385.c_orders 
val x387 = RecM_ctx2(x386, newId) 
x387 
} 
val x389 = x388.distinct 
val M_ctx2 = x389
//println("M_ctx2")
val x390 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x392 = M_ctx2 
val x393 = O__D_1 
val x395 = x393 
val x402 = { val out1 = x392.map{ case x396 => ({val x398 = x396.lbl 
val x399 = x398.c__F 
val x400 = x399.c_custkey 
x400}, x396) }
  val out2 = x395.map{ case x397 => ({val x401 = x397.o_custkey 
x401}, x397) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x414 = x402.map{ case (x403, x404) => val x413 = (x404) 
x413 match {
   case (null) => ({val x405 = (x403) 
x405}, Nil) => ({val x405 = (x403) 
x405}, Nil) 
   case x412 => ({val x405 = (x403) 
x405}, List({val x406 = x404.o_orderdate 
val x407 = x403.lbl 
val x408 = x407.P__F 
val x409 = x407.L__F 
val x410 = Record478(x404, x408, x409, newId) 
val x411 = Record479(x406, x410, newId) 
x411}))
 }
}.foldByKey(Nil){ case (acc477, x412) => acc477 ++ x412 } 
val x419 = x414.map{ case (x415, x416) => 
   val x417 = x415.lbl 
val x418 = RecM_flat2(x417, x416, newId) 
x418 
} 
val M_flat2 = x419
//println("M_flat2")
val x420 = M_flat2
//M_flat2.collect.foreach(println(_))
val x422 = M_flat2 
val x426 = x422.flatMap{ case x423 => x423 match {
   case null => List((x423, null))
   case _ =>
   val x424 = x423._2 
x424 match {
     case x425 => x425.map{ case v2 => (x423, v2) }
  }
 }} 
val x431 = x426.map{ case (x427, x428) => 
   val x429 = x428.o_parts 
val x430 = RecM_ctx3(x429, newId) 
x430 
} 
val x432 = x431.distinct 
val M_ctx3 = x432
//println("M_ctx3")
val x433 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x435 = M_ctx3 
val x436 = L__D_1 
val x438 = x436 
val x445 = { val out1 = x435.map{ case x439 => ({val x441 = x439.lbl 
val x442 = x441.o__F 
val x443 = x442.o_orderkey 
x443}, x439) }
  val out2 = x438.map{ case x440 => ({val x444 = x440.l_orderkey 
x444}, x440) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x446 = P__D_1 
val x448 = x446 
val x454 = { val out1 = x445.map{ case (x449, x450) => ({val x452 = x450.l_partkey 
x452}, (x449, x450)) }
  val out2 = x448.map{ case x451 => ({val x453 = x451.p_partkey 
x453}, x451) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x464 = x454.map{ case ((x455, x456), x457) => val x463 = (x456,x457) 
x463 match {
   case (_,null) => ({val x458 = (x455) 
x458}, Nil) => ({val x458 = (x455) 
x458}, Nil) 
   case x462 => ({val x458 = (x455) 
x458}, List({val x459 = x457.p_name 
val x460 = x456.l_quantity 
val x461 = Record483(x459, x460, newId) 
x461}))
 }
}.foldByKey(Nil){ case (acc482, x462) => acc482 ++ x462 } 
val x469 = x464.map{ case (x465, x466) => 
   val x467 = x465.lbl 
val x468 = RecM_flat3(x467, x466, newId) 
x468 
} 
val M_flat3 = x469
//println("M_flat3")
val x470 = M_flat3
//M_flat3.collect.foreach(println(_))
val res = x470.count
   var end0 = System.currentTimeMillis() - start0
   println("ShredQuery1Spark"+sf+","+Config.datapath+","+end0)
 }
}
