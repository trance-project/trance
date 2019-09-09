
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record576(c_name: String, c_custkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record578(P__F: Int, L__F: Int, O__F: Int, c__F: Record576, uniqueId: Long) extends CaseClassRecord
case class Record579(c_name: String, c_orders: Record578, uniqueId: Long) extends CaseClassRecord
case class Record581(o_orderdate: String, o_custkey: Int, o_orderkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record583(o__F: Record581, P__F: Int, L__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record584(o_orderdate: String, o_parts: Record583, uniqueId: Long) extends CaseClassRecord
case class Record586(l_quantity: Double, l_partkey: Int, l_orderkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record588(p_name: String, p_partkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record590(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record676(lbl: Q1Flat2, uniqueId: Long) extends CaseClassRecord
case class Input_Query4__D(c_name: String, c_orders: Int, uniqueId: Long) extends CaseClassRecord
case class Record680(c_name: String, p_name: String, month: String, t_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record681(_1: Q1Flat2, _2: Iterable[Record680], uniqueId: Long) extends CaseClassRecord
case class RecM_flat2(_1: Record578, _2: Iterable[Record584], uniqueId: Long) extends CaseClassRecord
case class RecM_flat3(_1: Record583, _2: Iterable[Record590], uniqueId: Long) extends CaseClassRecord
case class RecM_flat1(_1: Q1Flat, _2: Iterable[Record579], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx3(lbl: Record583, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx2(lbl: Record578, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx1(lbl: Q1Flat, uniqueId: Long) extends CaseClassRecord
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
   val x436 = Q1Flat(P__F, C__F, L__F, O__F, newId) 
val x437 = RecM_ctx1(x436, newId) 
val x438 = List(x437) 
val M_ctx1 = x438.head
val x441 = M_ctx1 
val x442 = C__D_1 
val x447 = x442.map(x443 => { val x444 = x443.c_name 
val x445 = x443.c_custkey 
val x446 = Record576(x444, x445, newId) 
x446 }) 
val x450 = x447.map{ case c => (x441, c) } 
val x463 = x450.map{ case (x451, x452) => val x462 = (x452) 
x462 match {
   case (null) => ({val x453 = (x451) 
x453}, null) 
   case x461 => ({val x453 = (x451) 
x453}, {val x454 = x452.c_name 
val x455 = x451.lbl 
val x456 = x455.P__F 
val x457 = x455.L__F 
val x458 = x455.O__F 
val x459 = Record578(x456, x457, x458, x452, newId) 
val x460 = Record579(x454, x459, newId) 
x460})
 }
}.groupByLabel() 
val x468 = x463.map{ case (x464, x465) => 
   val x466 = x464.lbl 
val x467 = RecM_flat1(x466, x465, newId) 
x467 
} 
val M_flat1 = x468
//println("M_flat1")
val x469 = M_flat1
//M_flat1.collect.foreach(println(_))
val x471 = M_flat1 
val x475 = x471.flatMap{ case x472 => x472 match {
   case null => List((x472, null))
   case _ =>
   val x473 = x472._2 
x473 match {
     case x474 => x474.map{ case v2 => (x472, v2) }
  }
 }} 
val x480 = x475.map{ case (x476, x477) => 
   val x478 = x477.c_orders 
val x479 = RecM_ctx2(x478, newId) 
x479 
} 
val x481 = x480.distinct 
val M_ctx2 = x481
//println("M_ctx2")
val x482 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x484 = M_ctx2 
val x485 = O__D_1 
val x491 = x485.map(x486 => { val x487 = x486.o_orderdate 
val x488 = x486.o_custkey 
val x489 = x486.o_orderkey 
val x490 = Record581(x487, x488, x489, newId) 
x490 }) 
val x498 = { val out1 = x484.map{ case x492 => ({val x494 = x492.lbl 
val x495 = x494.c__F 
val x496 = x495.c_custkey 
x496}, x492) }
  val out2 = x491.map{ case x493 => ({val x497 = x493.o_custkey 
x497}, x493) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x510 = x498.map{ case (x499, x500) => val x509 = (x500) 
x509 match {
   case (null) => ({val x501 = (x499) 
x501}, null) 
   case x508 => ({val x501 = (x499) 
x501}, {val x502 = x500.o_orderdate 
val x503 = x499.lbl 
val x504 = x503.P__F 
val x505 = x503.L__F 
val x506 = Record583(x500, x504, x505, newId) 
val x507 = Record584(x502, x506, newId) 
x507})
 }
}.groupByLabel() 
val x515 = x510.map{ case (x511, x512) => 
   val x513 = x511.lbl 
val x514 = RecM_flat2(x513, x512, newId) 
x514 
} 
val M_flat2 = x515
//println("M_flat2")
val x516 = M_flat2
//M_flat2.collect.foreach(println(_))
val x518 = M_flat2 
val x522 = x518.flatMap{ case x519 => x519 match {
   case null => List((x519, null))
   case _ =>
   val x520 = x519._2 
x520 match {
     case x521 => x521.map{ case v2 => (x519, v2) }
  }
 }} 
val x527 = x522.map{ case (x523, x524) => 
   val x525 = x524.o_parts 
val x526 = RecM_ctx3(x525, newId) 
x526 
} 
val x528 = x527.distinct 
val M_ctx3 = x528
//println("M_ctx3")
val x529 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x531 = M_ctx3 
val x532 = L__D_1 
val x538 = x532.map(x533 => { val x534 = x533.l_quantity 
val x535 = x533.l_partkey 
val x536 = x533.l_orderkey 
val x537 = Record586(x534, x535, x536, newId) 
x537 }) 
val x545 = { val out1 = x531.map{ case x539 => ({val x541 = x539.lbl 
val x542 = x541.o__F 
val x543 = x542.o_orderkey 
x543}, x539) }
  val out2 = x538.map{ case x540 => ({val x544 = x540.l_orderkey 
x544}, x540) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x546 = P__D_1 
val x551 = x546.map(x547 => { val x548 = x547.p_name 
val x549 = x547.p_partkey 
val x550 = Record588(x548, x549, newId) 
x550 }) 
val x557 = { val out1 = x545.map{ case (x552, x553) => ({val x555 = x553.l_partkey 
x555}, (x552, x553)) }
  val out2 = x551.map{ case x554 => ({val x556 = x554.p_partkey 
x556}, x554) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x567 = x557.map{ case ((x558, x559), x560) => val x566 = (x559,x560) 
x566 match {
   case (_,null) => ({val x561 = (x558) 
x561}, null) 
   case x565 => ({val x561 = (x558) 
x561}, {val x562 = x560.p_name 
val x563 = x559.l_quantity 
val x564 = Record590(x562, x563, newId) 
x564})
 }
}.groupByLabel() 
val x572 = x567.map{ case (x568, x569) => 
   val x570 = x568.lbl 
val x571 = RecM_flat3(x570, x569, newId) 
x571 
} 
val M_flat3 = x572
//println("M_flat3")
val x573 = M_flat3
//M_flat3.collect.foreach(println(_))
val res = x573 
 
val Query4__F = M_ctx1.lbl
val Query4__D_1 = M_flat1.flatMap{ r => r._2 }
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
     val x602 = Q1Flat2(Query4__F, newId) 
val x603 = Record676(x602, newId) 
val x604 = List(x603) 
val M_ctx1 = x604.head
val x607 = M_ctx1 
val x608 = Query4__D_1 
val x610 = x608 
val x613 = x610.map{ case c => (x607, c) } 
val x615 = Query4__D_2c_orders_1 
val x618 = x615 
val x623 = { val out1 = x613.map{ case (a, null) => (null, (a, null)); case (x619, x620) => ({val x622 = x620.c_orders 
x622}, (x619, x620)) }
  val out2 = x618.flatMap(x621 => x621._2.map{case v2 => (x621._1, v2)})
  out1.cogroup(out2).flatMap { pair =>
     if (pair._2._2.isEmpty) {
       pair._2._1.iterator.map{ case (x619, x620) => ((x619, x620), null) }
     } else {
       for ((x619, x620) <- pair._2._1.iterator; w <- pair._2._2.iterator) yield ((x619, x620), w)
      }
  }
} 
val x624 = Query4__D_2c_orders_2o_parts 
val x626 = x624 
val x628 = x626 
val x634 = { val out1 = x623.map{ case (a, null) => (null, (a, null)); case ((x629, x630), x631) => ({val x633 = x631.o_parts 
x633}, ((x629, x630), x631)) }
  val out2 = x628.flatMap(x632 => x632._2.map{case v2 => (x632._1, v2)})
  out1.cogroup(out2).flatMap { pair =>
     if (pair._2._2.isEmpty) {
       pair._2._1.iterator.map{ case ((x629, x630), x631) => (((x629, x630), x631), null) }
     } else {
       for (((x629, x630), x631) <- pair._2._1.iterator; w <- pair._2._2.iterator) yield (((x629, x630), x631), w)
      }
  }
} 
val x636 = x626 
val x645 = { val out1 = x634.map{ case (a, null) => (null, (a, null)); case (((x637, x638), x639), x640) => (({val x642 = x639.o_parts 
x642}, {val x644 = x640.p_name 
x644}), (((x637, x638), x639), x640)) }
  val out2 = x636.flatMap(v2 => v2._2.map{case x641 => ((v2._1, {val x643 = x641.p_name 
x643}), x641)})
  out1.cogroup(out2).flatMap { pair =>
     if (pair._2._2.isEmpty) {
       pair._2._1.iterator.map{ case (((x637, x638), x639), x640) => ((((x637, x638), x639), x640), null) }
     } else {
       for ((((x637, x638), x639), x640) <- pair._2._1.iterator; w <- pair._2._2.iterator) yield ((((x637, x638), x639), x640), w)
      }
  }
} 
val x655 = x645.map{ case ((((x646, x647), x648), x649), x650) => val x654 = (x650) 
x654 match {
   case (null) => ({val x651 = (x646,x647,x648,x649) 
x651}, 0.0)
   case x653 => ({val x651 = (x646,x647,x648,x649) 
x651}, {val x652 = x650.l_qty 
x652})
 }
}.reduceByKey(_ + _) 
val x668 = x655.map{ case ((x656, x657, x658, x659), x660) => val x667 = (x660,x657,x658,x659) 
x667 match {
   case (_,null,_,_) => ({val x661 = (x656) 
x661}, null)
case (_,_,null,_) => ({val x661 = (x656) 
x661}, null)
case (_,_,_,null) => ({val x661 = (x656) 
x661}, null) 
   case x666 => ({val x661 = (x656) 
x661}, {val x662 = x657.c_name 
val x663 = x659.p_name 
val x664 = x658.o_orderdate 
val x665 = Record680(x662, x663, x664, x660, newId) 
x665})
 }
}.groupByLabel() 
val x673 = x668.map{ case (x669, x670) => 
   val x671 = x669.lbl 
val x672 = Record681(x671, x670, newId) 
x672 
} 
val M_flat1 = x673
//println("M_flat1")
val x674 = M_flat1
//M_flat1.collect.foreach(println(_))
val res = x674.count
   }
   f
   var end0 = System.currentTimeMillis() - start0
   println("ShredQuery4Spark"+sf+","+Config.datapath+","+end0)
 }
}
