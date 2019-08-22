
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record579(c_name: String, c_custkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record581(P__F: Int, L__F: Int, O__F: Int, c__F: Customer, uniqueId: Long) extends CaseClassRecord
case class Record582(c_name: String, c_orders: Record581, uniqueId: Long) extends CaseClassRecord
case class Record584(o_orderdate: String, o_custkey: Int, o_orderkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record586(o__F: Orders, P__F: Int, L__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record587(o_orderdate: String, o_parts: Record586, uniqueId: Long) extends CaseClassRecord
case class Record589(l_quantity: Double, l_partkey: Int, l_orderkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record591(p_name: String, p_partkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record593(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record684(lbl: Q1Flat2, uniqueId: Long) extends CaseClassRecord
case class Input_Query4__D(c_name: String, c_orders: Int, uniqueId: Long) extends CaseClassRecord
case class Record686(c_name: String, uniqueId: Long) extends CaseClassRecord
case class Record689(c_name: String, p_name: String, month: String, t_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record690(_1: Q1Flat2, _2: Iterable[Record689], uniqueId: Long) extends CaseClassRecord
case class RecM_flat2(_1: Record581, _2: Iterable[Record587], uniqueId: Long) extends CaseClassRecord
case class RecM_flat3(_1: Record586, _2: Iterable[Record593], uniqueId: Long) extends CaseClassRecord
case class RecM_flat1(_1: Q1Flat, _2: Iterable[Record582], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx3(lbl: Record586, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx2(lbl: Record581, uniqueId: Long) extends CaseClassRecord
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
   val x439 = Q1Flat(P__F, C__F, L__F, O__F, newId) 
val x440 = RecM_ctx1(x439, newId) 
val x441 = List(x440) 
val M_ctx1 = x441.head
val x444 = M_ctx1 
val x445 = C__D_1 
val x450 = x445.map(x446 => { val x447 = x446.c_name 
val x448 = x446.c_custkey 
val x449 = Record579(x447, x448, newId) 
x449 }) 
val x453 = x450.map{ case c => (x444, c) } 
val x466 = x453.map{ case (x454, x455) => val x465 = (x455) 
x465 match {
   case (null) => ({val x456 = (x454) 
x456}, null) 
   case x464 => ({val x456 = (x454) 
x456}, {val x457 = x455.c_name 
val x458 = x454.lbl 
val x459 = x458.P__F 
val x460 = x458.L__F 
val x461 = x458.O__F 
val x462 = Record581(x459, x460, x461, x455, newId) 
val x463 = Record582(x457, x462, newId) 
x463})
 }
}.groupByLabel() 
val x471 = x466.map{ case (x467, x468) => 
   val x469 = x467.lbl 
val x470 = RecM_flat1(x469, x468, newId) 
x470 
} 
val M_flat1 = x471
//println("M_flat1")
val x472 = M_flat1
//M_flat1.collect.foreach(println(_))
val x474 = M_flat1 
val x478 = x474.flatMap{ case x475 => x475 match {
   case null => List((x475, null))
   case _ =>
   val x476 = x475._2 
x476 match {
     case x477 => x477.map{ case v2 => (x475, v2) }
  }
 }} 
val x483 = x478.map{ case (x479, x480) => 
   val x481 = x480.c_orders 
val x482 = RecM_ctx2(x481, newId) 
x482 
} 
val x484 = x483.distinct 
val M_ctx2 = x484
//println("M_ctx2")
val x485 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x487 = M_ctx2 
val x488 = O__D_1 
val x494 = x488.map(x489 => { val x490 = x489.o_orderdate 
val x491 = x489.o_custkey 
val x492 = x489.o_orderkey 
val x493 = Record584(x490, x491, x492, newId) 
x493 }) 
val x501 = { val out1 = x487.map{ case x495 => ({val x497 = x495.lbl 
val x498 = x497.c__F 
val x499 = x498.c_custkey 
x499}, x495) }
  val out2 = x494.map{ case x496 => ({val x500 = x496.o_custkey 
x500}, x496) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x513 = x501.map{ case (x502, x503) => val x512 = (x503) 
x512 match {
   case (null) => ({val x504 = (x502) 
x504}, null) 
   case x511 => ({val x504 = (x502) 
x504}, {val x505 = x503.o_orderdate 
val x506 = x502.lbl 
val x507 = x506.P__F 
val x508 = x506.L__F 
val x509 = Record586(x503, x507, x508, newId) 
val x510 = Record587(x505, x509, newId) 
x510})
 }
}.groupByLabel() 
val x518 = x513.map{ case (x514, x515) => 
   val x516 = x514.lbl 
val x517 = RecM_flat2(x516, x515, newId) 
x517 
} 
val M_flat2 = x518
//println("M_flat2")
val x519 = M_flat2
//M_flat2.collect.foreach(println(_))
val x521 = M_flat2 
val x525 = x521.flatMap{ case x522 => x522 match {
   case null => List((x522, null))
   case _ =>
   val x523 = x522._2 
x523 match {
     case x524 => x524.map{ case v2 => (x522, v2) }
  }
 }} 
val x530 = x525.map{ case (x526, x527) => 
   val x528 = x527.o_parts 
val x529 = RecM_ctx3(x528, newId) 
x529 
} 
val x531 = x530.distinct 
val M_ctx3 = x531
//println("M_ctx3")
val x532 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x534 = M_ctx3 
val x535 = L__D_1 
val x541 = x535.map(x536 => { val x537 = x536.l_quantity 
val x538 = x536.l_partkey 
val x539 = x536.l_orderkey 
val x540 = Record589(x537, x538, x539, newId) 
x540 }) 
val x548 = { val out1 = x534.map{ case x542 => ({val x544 = x542.lbl 
val x545 = x544.o__F 
val x546 = x545.o_orderkey 
x546}, x542) }
  val out2 = x541.map{ case x543 => ({val x547 = x543.l_orderkey 
x547}, x543) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x549 = P__D_1 
val x554 = x549.map(x550 => { val x551 = x550.p_name 
val x552 = x550.p_partkey 
val x553 = Record591(x551, x552, newId) 
x553 }) 
val x560 = { val out1 = x548.map{ case (x555, x556) => ({val x558 = x556.l_partkey 
x558}, (x555, x556)) }
  val out2 = x554.map{ case x557 => ({val x559 = x557.p_partkey 
x559}, x557) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x570 = x560.map{ case ((x561, x562), x563) => val x569 = (x562,x563) 
x569 match {
   case (_,null) => ({val x564 = (x561) 
x564}, null) 
   case x568 => ({val x564 = (x561) 
x564}, {val x565 = x563.p_name 
val x566 = x562.l_quantity 
val x567 = Record593(x565, x566, newId) 
x567})
 }
}.groupByLabel() 
val x575 = x570.map{ case (x571, x572) => 
   val x573 = x571.lbl 
val x574 = RecM_flat3(x573, x572, newId) 
x574 
} 
val M_flat3 = x575
//println("M_flat3")
val x576 = M_flat3
//M_flat3.collect.foreach(println(_))
val res = x576 
 
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
     val x605 = Q1Flat2(Query4__F, newId) 
val x606 = Record684(x605, newId) 
val x607 = List(x606) 
val M_ctx1 = x607.head
val x610 = M_ctx1 
val x611 = Query4__D_1 
val x615 = x611.map(x612 => { val x613 = x612.c_name 
val x614 = Record686(x613, newId) 
x614 }) 
val x618 = x615.map{ case c => (x610, c) } 
val x620 = Query4__D_2c_orders_1 
val x624 = x620 
val x629 = { val out1 = x618.map{ case (a, null) => (null, (a, null)); case (x625, x626) => ({val x628 = x626.c_orders 
x628}, (x625, x626)) }
  val out2 = x624.flatMap(x627 => x627._2.map{case v2 => (x627._1, v2)})
  out1.cogroup(out2).flatMap { pair =>
     if (pair._2._2.isEmpty) {
       pair._2._1.iterator.map{ case (x625, x626) => ((x625, x626), null) }
     } else {
       for ((x625, x626) <- pair._2._1.iterator; w <- pair._2._2.iterator) yield ((x625, x626), w)
      }
  }
} 
val x630 = Query4__D_2c_orders_2o_parts 
val x632 = x630 
val x635 = x632.map(x633 => { val x634 = () 
x634 }) 
val x641 = { val out1 = x629.map{ case (a, null) => (null, (a, null)); case ((x636, x637), x638) => ({val x640 = x638.o_parts 
x640}, ((x636, x637), x638)) }
  val out2 = x635.flatMap(x639 => x639._2.map{case v2 => (x639._1, v2)})
  out1.cogroup(out2).flatMap { pair =>
     if (pair._2._2.isEmpty) {
       pair._2._1.iterator.map{ case ((x636, x637), x638) => (((x636, x637), x638), null) }
     } else {
       for (((x636, x637), x638) <- pair._2._1.iterator; w <- pair._2._2.iterator) yield (((x636, x637), x638), w)
      }
  }
} 
val x644 = x632.map(x642 => { val x643 = () 
x643 }) 
val x653 = { val out1 = x641.map{ case (a, null) => (null, (a, null)); case (((x645, x646), x647), x648) => (({val x650 = x647.o_parts 
x650}, {val x652 = x648.p_name 
x652}), (((x645, x646), x647), x648)) }
  val out2 = x644.flatMap(v2 => v2._2.map{case x649 => ((v2._1, {val x651 = x649.p_name 
x651}), x649)})
  out1.cogroup(out2).flatMap { pair =>
     if (pair._2._2.isEmpty) {
       pair._2._1.iterator.map{ case (((x645, x646), x647), x648) => ((((x645, x646), x647), x648), null) }
     } else {
       for ((((x645, x646), x647), x648) <- pair._2._1.iterator; w <- pair._2._2.iterator) yield ((((x645, x646), x647), x648), w)
      }
  }
} 
val x663 = x653.map{ case ((((x654, x655), x656), x657), x658) => val x662 = (x658) 
x662 match {
   case (null) => ({val x659 = (x654,x655,x656,x657) 
x659}, 0.0)
   case x661 => ({val x659 = (x654,x655,x656,x657) 
x659}, {val x660 = x658.l_qty 
x660})
 }
}.reduceByKey(_ + _) 
val x676 = x663.map{ case ((x664, x665, x666, x667), x668) => val x675 = (x668,x667,x666,x665) 
x675 match {
   case (_,null,_,_) => ({val x669 = (x664) 
x669}, null)
case (_,_,null,_) => ({val x669 = (x664) 
x669}, null)
case (_,_,_,null) => ({val x669 = (x664) 
x669}, null) 
   case x674 => ({val x669 = (x664) 
x669}, {val x670 = x665.c_name 
val x671 = x667.p_name 
val x672 = x666.o_orderdate 
val x673 = Record689(x670, x671, x672, x668, newId) 
x673})
 }
}.groupByLabel() 
val x681 = x676.map{ case (x677, x678) => 
   val x679 = x677.lbl 
val x680 = Record690(x679, x678, newId) 
x680 
} 
val M_flat1 = x681
//println("M_flat1")
val x682 = M_flat1
//M_flat1.collect.foreach(println(_))
val res = x682.count
   }
   f
   var end0 = System.currentTimeMillis() - start0
   println("ShredQuery4Spark"+sf+","+Config.datapath+","+end0)
 }
}
