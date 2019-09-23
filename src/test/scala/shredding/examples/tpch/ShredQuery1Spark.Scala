
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Input_M_ctx1(lbl: Int, uniqueId: Long) extends CaseClassRecord
case class Record527(c_name: String, c_custkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record529(c__F.c_custkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record530(c_name: String, c_orders: Record529, uniqueId: Long) extends CaseClassRecord
case class Record531(_1: (Int), _2: Iterable[Record530], uniqueId: Long) extends CaseClassRecord
case class Record533(o_orderdate: String, o_orderkey: Int, o_custkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record535(o__F.o_orderkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record536(o_orderdate: String, o_parts: Record535, uniqueId: Long) extends CaseClassRecord
case class Record538(l_quantity: Double, l_partkey: Int, l_orderkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record540(p_name: String, p_partkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record542(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx3(lbl: Record535, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx2(lbl: Record529, uniqueId: Long) extends CaseClassRecord
case class RecM_flat1(_1: Int, _2: Iterable[Record530], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx1(lbl: Unit, uniqueId: Long) extends CaseClassRecord
case class RecM_flat2(_1: Record529, _2: Iterable[Record536], uniqueId: Long) extends CaseClassRecord
case class RecM_flat3(_1: Record535, _2: Iterable[Record542], uniqueId: Long) extends CaseClassRecord
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
   val x398 = () 
val x399 = RecM_ctx1(x398, newId) 
val x400 = List(x399) 
val M_ctx1 = x400.head
val x403 = M_ctx1 
val x404 = C__D_1 
val x409 = x404.map(x405 => { val x406 = x405.c_name 
val x407 = x405.c_custkey 
val x408 = Record527(x406, x407, newId) 
x408 }) 
val x412 = x409.map{ case c => (x403, c) } 
val x423 = x412.map{ case (x413, x414) => val x422 = (x414) 
x422 match {
   case (null) => ({val x415 = x413.lbl 
val x416 = (x415) 
x416}, null) 
   case x421 => ({val x415 = x413.lbl 
val x416 = (x415) 
x416}, {val x417 = x414.c_name 
val x418 = x414.c_custkey 
val x419 = Record529(x418, newId) 
val x420 = Record530(x417, x419, newId) 
x420})
 }
}.groupByLabel() 
val x427 = x423.map{ case (x424, x425) => 
   val x426 = Record531(x424, x425, newId) 
x426 
} 
val M_flat1 = x427
//println("M_flat1")
val x428 = M_flat1
//M_flat1.collect.foreach(println(_))
val x430 = M_flat1 
val x434 = x430.flatMap{ case x431 => x431 match {
   case null => List((x431, null))
   case _ =>
   val x432 = x431._2 
x432 match {
     case x433 => x433.map{ case v2 => (x431, v2) }
  }
 }} 
val x439 = x434.map{ case (x435, x436) => 
   val x437 = x436.c_orders 
val x438 = RecM_ctx2(x437, newId) 
x438 
} 
val x440 = x439.distinct 
val M_ctx2 = x440
//println("M_ctx2")
val x441 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x443 = M_ctx2 
val x444 = O__D_1 
val x450 = x444.map(x445 => { val x446 = x445.o_orderdate 
val x447 = x445.o_orderkey 
val x448 = x445.o_custkey 
val x449 = Record533(x446, x447, x448, newId) 
x449 }) 
val x454 = { val out1 = x443.map{ case x451 => ({true}, x451) }
  val out2 = x450.map{ case x452 => ({val x453 = x452.o_custkey 
x453}, x452) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x465 = x454.map{ case (x455, x456) => val x464 = (x456) 
x464 match {
   case (null) => ({val x457 = x455.lbl 
val x458 = (x457) 
x458}, null) 
   case x463 => ({val x457 = x455.lbl 
val x458 = (x457) 
x458}, {val x459 = x456.o_orderdate 
val x460 = x456.o_orderkey 
val x461 = Record535(x460, newId) 
val x462 = Record536(x459, x461, newId) 
x462})
 }
}.groupByLabel() 
val x469 = x465.map{ case (x466, x467) => 
   val x468 = RecM_flat2(x466, x467, newId) 
x468 
} 
val M_flat2 = x469
//println("M_flat2")
val x470 = M_flat2
//M_flat2.collect.foreach(println(_))
val x472 = M_flat2 
val x476 = x472.flatMap{ case x473 => x473 match {
   case null => List((x473, null))
   case _ =>
   val x474 = x473._2 
x474 match {
     case x475 => x475.map{ case v2 => (x473, v2) }
  }
 }} 
val x481 = x476.map{ case (x477, x478) => 
   val x479 = x478.o_parts 
val x480 = RecM_ctx3(x479, newId) 
x480 
} 
val x482 = x481.distinct 
val M_ctx3 = x482
//println("M_ctx3")
val x483 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x485 = M_ctx3 
val x486 = L__D_1 
val x492 = x486.map(x487 => { val x488 = x487.l_quantity 
val x489 = x487.l_partkey 
val x490 = x487.l_orderkey 
val x491 = Record538(x488, x489, x490, newId) 
x491 }) 
val x496 = { val out1 = x485.map{ case x493 => ({true}, x493) }
  val out2 = x492.map{ case x494 => ({val x495 = x494.l_orderkey 
x495}, x494) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x497 = P__D_1 
val x502 = x497.map(x498 => { val x499 = x498.p_name 
val x500 = x498.p_partkey 
val x501 = Record540(x499, x500, newId) 
x501 }) 
val x508 = { val out1 = x496.map{ case (x503, x504) => ({val x506 = x504.l_partkey 
x506}, (x503, x504)) }
  val out2 = x502.map{ case x505 => ({val x507 = x505.p_partkey 
x507}, x505) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x519 = x508.map{ case ((x509, x510), x511) => val x518 = (x510,x511) 
x518 match {
   case (_,null) => ({val x512 = x509.lbl 
val x513 = (x512) 
x513}, null) 
   case x517 => ({val x512 = x509.lbl 
val x513 = (x512) 
x513}, {val x514 = x511.p_name 
val x515 = x510.l_quantity 
val x516 = Record542(x514, x515, newId) 
x516})
 }
}.groupByLabel() 
val x523 = x519.map{ case (x520, x521) => 
   val x522 = RecM_flat3(x520, x521, newId) 
x522 
} 
val M_flat3 = x523
//println("M_flat3")
val x524 = M_flat3
//M_flat3.collect.foreach(println(_))
val res = x524.count
   var end0 = System.currentTimeMillis() - start0
   println("ShredQuery1Spark"+sf+","+Config.datapath+","+end0)
 }
}
