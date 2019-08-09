
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
case class Record449(P__F: Int, L__F: Int, O__F: Int, c__F: Customer, uniqueId: Long) extends CaseClassRecord
case class Input_M_flat1(_1: Q1Flat, _2: List[RecM_flat1], uniqueId: Long) extends CaseClassRecord
case class Record452(o__F: Orders, P__F: Int, L__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record453(o_orderdate: String, o_parts: Record452, uniqueId: Long) extends CaseClassRecord
case class Record457(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record532(lbl: Q1Flat2, uniqueId: Long) extends CaseClassRecord
case class Input_Query4__D(c_name: String, c_orders: Int, uniqueId: Long) extends CaseClassRecord
case class Record535(c_name: String, p_name: String, month: String, t_qty: Double, uniqueId: Long) extends CaseClassRecord
case class RecM_flat2(_1: Record449, _2: List[Record453], uniqueId: Long) extends CaseClassRecord
case class RecM_flat3(_1: Record452, _2: List[Record457], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx3(lbl: Record452, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx2(lbl: Record449, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx1(lbl: Q1Flat, uniqueId: Long) extends CaseClassRecord
case class RecM_flat1(c_name: String, c_orders: Record449, uniqueId: Long) extends CaseClassRecord
object ShredQuery4Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4Spark"+sf)
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
   val x332 = Q1Flat(P__F, C__F, L__F, O__F, newId) 
val x333 = RecM_ctx1(x332, newId) 
val x334 = List(x333) 
val M_ctx1 = x334.head
val x336 = C__D_1 
val x338 = x336 
val x342 = x338.flatMap{ case x339 => x339 match {
   case null => List((x339, null))
   case _ =>
   val x340 = x339._2 
x340 match {
     case x341 => x341.map{ case v2 => (x339, v2) }
  }
 }} 
val x352 = x342.map{ case (x343, x344) => 
   val x345 = x344.c_name 
val x346 = M_ctx1.lbl 
val x347 = x346.P__F 
val x348 = x346.L__F 
val x349 = x346.O__F 
val x350 = Record449(x347, x348, x349, x344, newId) 
val x351 = RecM_flat1(x345, x350, newId) 
x351 
} 
val M_flat1 = x352
//println("M_flat1")
val x353 = M_flat1
//M_flat1.collect.foreach(println(_))
val x355 = M_flat1 
val x359 = x355.flatMap{ case x356 => x356 match {
   case null => List((x356, null))
   case _ =>
   val x357 = x356._2 
x357 match {
     case x358 => x358.map{ case v2 => (x356, v2) }
  }
 }} 
val x364 = x359.map{ case (x360, x361) => 
   val x362 = x361.c_orders 
val x363 = RecM_ctx2(x362, newId) 
x363 
} 
val x365 = x364.distinct 
val M_ctx2 = x365
//println("M_ctx2")
val x366 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x368 = M_ctx2 
val x369 = O__D_1 
val x371 = x369 
val x378 = { val out1 = x368.map{ case x372 => ({val x374 = x372.lbl 
val x375 = x374.c__F 
val x376 = x375.c_custkey 
x376}, x372) }
  val out2 = x371.map{ case x373 => ({val x377 = x373.o_custkey 
x377}, x373) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x390 = x378.map{ case (x379, x380) => val x389 = (x380) 
x389 match {
   case (null) => ({val x381 = (x379) 
x381}, Nil) 
   case x388 => ({val x381 = (x379) 
x381}, List({val x382 = x380.o_orderdate 
val x383 = x379.lbl 
val x384 = x383.P__F 
val x385 = x383.L__F 
val x386 = Record452(x380, x384, x385, newId) 
val x387 = Record453(x382, x386, newId) 
x387}))
 }
}.foldByKey(Nil){ case (acc451, x388) => acc451 ++ x388 } 
val x395 = x390.map{ case (x391, x392) => 
   val x393 = x391.lbl 
val x394 = RecM_flat2(x393, x392, newId) 
x394 
} 
val M_flat2 = x395
//println("M_flat2")
val x396 = M_flat2
//M_flat2.collect.foreach(println(_))
val x398 = M_flat2 
val x402 = x398.flatMap{ case x399 => x399 match {
   case null => List((x399, null))
   case _ =>
   val x400 = x399._2 
x400 match {
     case x401 => x401.map{ case v2 => (x399, v2) }
  }
 }} 
val x407 = x402.map{ case (x403, x404) => 
   val x405 = x404.o_parts 
val x406 = RecM_ctx3(x405, newId) 
x406 
} 
val x408 = x407.distinct 
val M_ctx3 = x408
//println("M_ctx3")
val x409 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x411 = M_ctx3 
val x412 = L__D_1 
val x414 = x412 
val x421 = { val out1 = x411.map{ case x415 => ({val x417 = x415.lbl 
val x418 = x417.o__F 
val x419 = x418.o_orderkey 
x419}, x415) }
  val out2 = x414.map{ case x416 => ({val x420 = x416.l_orderkey 
x420}, x416) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x422 = P__D_1 
val x424 = x422 
val x430 = { val out1 = x421.map{ case (x425, x426) => ({val x428 = x426.l_partkey 
x428}, (x425, x426)) }
  val out2 = x424.map{ case x427 => ({val x429 = x427.p_partkey 
x429}, x427) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x440 = x430.map{ case ((x431, x432), x433) => val x439 = (x432,x433) 
x439 match {
   case (_,null) => ({val x434 = (x431) 
x434}, Nil) 
   case x438 => ({val x434 = (x431) 
x434}, List({val x435 = x433.p_name 
val x436 = x432.l_quantity 
val x437 = Record457(x435, x436, newId) 
x437}))
 }
}.foldByKey(Nil){ case (acc456, x438) => acc456 ++ x438 } 
val x445 = x440.map{ case (x441, x442) => 
   val x443 = x441.lbl 
val x444 = RecM_flat3(x443, x442, newId) 
x444 
} 
val M_flat3 = x445
//println("M_flat3")
val x446 = M_flat3
//M_flat3.collect.foreach(println(_))
val res = x446 
 
val Query4__F = M_ctx1.lbl
val Query4__D_1 = M_flat1.flatMap(r => r._2)
Query4__D_1.cache
Query4__D_1.count
val Query4__D_2c_orders_1 = M_flat2
Query4__D_2c_orders_1.cache
Query4__D_2c_orders_1.count
val Query4__D_2c_orders_2o_parts = M_flat3
Query4__D_2c_orders_2o_parts.cache
Query4__D_2c_orders_2o_parts.count
    
   var start0 = System.currentTimeMillis()
   def f() {
     val x467 = Q1Flat2(Query4__F, newId) 
val x468 = Record532(x467, newId) 
val x469 = List(x468) 
val M_ctx1 = x469.head
val x471 = Query4__D_1 
val x473 = x471 
val x477 = x473.flatMap{ case x474 => x474 match {
   case null => List((x474, null))
   case _ =>
   val x475 = x474._2 
x475 match {
     case x476 => x476.map{ case v2 => (x474, v2) }
  }
 }} 
val x479 = Query4__D_2c_orders_1 
val x482 = x479 
val x487 = { val out1 = x477.map{ case (a, null) => (null, (a, null)); case (x483, x484) => ({val x486 = x484.c_orders 
x486}, (x483, x484)) }
  val out2 = x482.flatMap(x485 => x485._2.map{case v2 => (x485._1, v2)})
  out1.join(out2).map{ case (k, v) => v }
} 
val x488 = Query4__D_2c_orders_2o_parts 
val x490 = x488 
val x492 = x490 
val x498 = { val out1 = x487.map{ case (a, null) => (null, (a, null)); case ((x493, x494), x495) => ({val x497 = x495.o_parts 
x497}, ((x493, x494), x495)) }
  val out2 = x492.flatMap(x496 => x496._2.map{case v2 => (x496._1, v2)})
  out1.join(out2).map{ case (k, v) => v }
} 
val x500 = x490 
val x509 = { val out1 = x498.map{ case (a, null) => (null, (a, null)); case (((x501, x502), x503), x504) => (({val x506 = x503.o_parts 
x506}, {val x508 = x504.p_name 
x508}), (((x501, x502), x503), x504)) }
  val out2 = x500.flatMap(v2 => v2._2.map{case x505 => ((v2._1, {val x507 = x505.p_name 
x507}), x505)})
  out1.cogroup(out2).flatMap { pair =>
     if (pair._2._2.isEmpty) {
       pair._2._1.iterator.map(v => (v, null))
     } else {
       for (v <- pair._2._1.iterator; w <- pair._2._2.iterator) yield (v, w)
      }
  }
} 
val x519 = x509.map{ case ((((x510, x511), x512), x513), x514) => val x518 = (x514) 
x518 match {
   case (null) => ({val x515 = (x510,x511,x512,x513) 
x515}, 0.0)
   case x517 => ({val x515 = (x510,x511,x512,x513) 
x515}, {val x516 = x514.l_qty 
x516})
 }
}.foldByKey(0.0){ case (acc534, x517) => acc534 + x517 } 
val x529 = x519.map{ case ((x520, x521, x522, x523), x524) => 
   val x525 = x521.c_name 
val x526 = x523.p_name 
val x527 = x522.o_orderdate 
val x528 = Record535(x525, x526, x527, x524, newId) 
x528 
} 
val M_flat1 = x529
//println("M_flat1")
val x530 = M_flat1
//M_flat1.collect.foreach(println(_))
val res = x530.count
   }
   f
   var end0 = System.currentTimeMillis() - start0
   println("ShredQuery4Spark"+sf+","+Config.datapath+","+end0)
 }
}
