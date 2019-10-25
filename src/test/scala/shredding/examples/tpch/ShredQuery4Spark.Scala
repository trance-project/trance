
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record573(lbl: Unit)
case class Record574(c_name: String, c_custkey: Int)
case class Record576(c__Fc_custkey: Int)
case class Record577(c_name: String, c_orders: Record576)
case class Record578(_1: Record573, _2: (Iterable[Record577]))
case class Record579(lbl: Record576)
case class Record580(o_orderkey: Int, o_orderdate: String, o_custkey: Int)
case class Record582(o_orderkey: Int, o_orderdate: String)
case class Record583(_1: Record579, _2: (Iterable[Record582]))
case class Record731(l_quantity: Double, l_orderkey: Int, l_partkey: Int)
case class Record732(p_name: String, p_partkey: Int)
case class Record734(l_orderkey: Int, p_name: String)
case class Record736(c_name: String, o_orderdate: String, p_name: String)
object ShredQuery4Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4Spark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L__F = 3
val L__D_1 = tpch.loadLineitem
L__D_1.cache
L__D_1.count
val P__F = 4
val P__D_1 = tpch.loadPart
P__D_1.cache
P__D_1.count
val C__F = 1
val C__D_1 = tpch.loadCustomers
C__D_1.cache
C__D_1.count
val O__F = 2
val O__D_1 = tpch.loadOrders
O__D_1.cache
O__D_1.count

   val CustOrders = {
 val x498 = () 
val x499 = Record573(x498) 
val x500 = List(x499) 
val M_ctx1 = x500
val x501 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x503 = M_ctx1 
val x504 = C__D_1 
val x509 = x504.map(x505 => { val x506 = x505.c_name 
val x507 = x505.c_custkey 
val x508 = Record574(x506, x507) 
x508 }) 
val x512 = x509.map{ case c => (x503.head, c) } 
val x522 = x512.map{ case (x513, x514) => val x521 = (x514) 
x521 match {
   case (null) => ({val x515 = (x513) 
x515}, null) 
   case x520 => ({val x515 = (x513) 
x515}, {val x516 = x514.c_name 
val x517 = x514.c_custkey 
val x518 = Record576(x517) 
val x519 = Record577(x516, x518) 
x519})
 }
}.groupByLabel() 
val x527 = x522.map{ case (x523, x524) => 
   val x525 = (x524) 
val x526 = Record578(x523, x525) 
x526 
} 
val M_flat1 = x527
val x528 = M_flat1
//M_flat1.collect.foreach(println(_))
val x530 = M_flat1 
val x534 = x530.flatMap{ case x531 => x531 match {
   case null => List((x531, null))
   case _ =>
   val x532 = x531._2 
x532 match {
     case x533 => x533.map{ case v2 => (x531, v2) }
  }
 }} 
val x539 = x534.map{ case (x535, x536) => 
   val x537 = x536.c_orders 
val x538 = Record579(x537) 
x538 
} 
val x540 = x539.distinct 
val M_ctx2 = x540
val x541 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x543 = M_ctx2 
val x544 = O__D_1 
val x550 = x544.map(x545 => { val x546 = x545.o_orderkey 
val x547 = x545.o_orderdate 
val x548 = x545.o_custkey 
val x549 = Record580(x546, x547, x548) 
x549 }) 
val x556 = { val out1 = x543.map{ case x551 => ({val x553 = x551.lbl 
val x554 = x553.c__Fc_custkey 
x554}, x551) }
  val out2 = x550.map{ case x552 => ({val x555 = x552.o_custkey 
x555}, x552) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x565 = x556.map{ case (x557, x558) => val x564 = (x558) 
x564 match {
   case (null) => ({val x559 = (x557) 
x559}, null) 
   case x563 => ({val x559 = (x557) 
x559}, {val x560 = x558.o_orderkey 
val x561 = x558.o_orderdate 
val x562 = Record582(x560, x561) 
x562})
 }
}.groupByLabel() 
val x570 = x565.map{ case (x566, x567) => 
   val x568 = (x567) 
val x569 = Record583(x566, x568) 
x569 
} 
val M_flat2 = x570
val x571 = M_flat2
//M_flat2.collect.foreach(println(_))
x571
}
CustOrders.cache
CustOrders.count
def f = { 
 val x634 = () 
val x635 = Record573(x634) 
val partcnts__F = x635
val x636 = partcnts__F
//partcnts__F.collect.foreach(println(_))
val x637 = List(partcnts__F) 
val x639 = x637 
val x640 = L__D_1 
val x646 = x640.map(x641 => { val x642 = x641.l_quantity 
val x643 = x641.l_orderkey 
val x644 = x641.l_partkey 
val x645 = Record731(x642, x643, x644) 
x645 }) 
val x649 = x646.map{ case c => (x639.head, c) } 
val x650 = P__D_1 
val x655 = x650.map(x651 => { val x652 = x651.p_name 
val x653 = x651.p_partkey 
val x654 = Record732(x652, x653) 
x654 }) 
val x661 = { val out1 = x649.map{ case (x656, x657) => ({val x659 = x657.l_partkey 
x659}, (x656, x657)) }
  val out2 = x655.map{ case x658 => ({val x660 = x658.p_partkey 
x660}, x658) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x672 = x661.map{ case ((x662, x663), x664) => 
   ({val x665 = x663.l_orderkey 
val x666 = x664.p_name 
val x667 = Record734(x665, x666) 
val x668 = (x662,x667) 
x668}, {val x669 = x663.l_quantity 
x669})
}.reduceByKey(_ + _) 
val x678 = x672.map{ case ((x673, x674), x675) => 
  (x673, (x674,x675))
}.groupByLabel() 
val partcnts__D_1 = x678
val x679 = partcnts__D_1
//partcnts__D_1.collect.foreach(println(_))
val x680 = List(x635) 
val M_ctx1 = x680
val x681 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x683 = M_ctx1 
val x684 = CustOrders__D_1 
val x686 = x684 
val x689 = x686.map{ case c => (x683.head, c) } 
val x691 = CustOrders__D_2c_orders_1 
val x694 = x691 
val x699 = { val out1 = x689.map{ case (a, null) => (null, (a, null)); case (x695, x696) => ({val x698 = x696.c_orders 
x698}, (x695, x696)) }
  val out2 = x694.flatMap(x697 => x697._2.map{case v2 => (x697._1.lbl, v2)})
  out1.outerLookup(out2)
} 
val x700 = partcnts__D_1 
val x702 = x700 
val x709 = { val out1 = x699.map{ case ((x703, x704), x705) => ({val x707 = x705.o_orderkey 
x707}, ((x703, x704), x705)) }
  val out2 = x702.map{ case x706 => ({val x708 = x706.l_orderkey 
x708}, x706) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x722 = x709.map{ case (((x710, x711), x712), x713) => 
   ({val x714 = x711.c_name 
val x715 = x712.o_orderdate 
val x716 = x713.p_name 
val x717 = Record736(x714, x715, x716) 
val x718 = (x710,x717) 
x718}, {val x719 = x713._2 
x719})
}.reduceByKey(_ + _) 
val x728 = x722.map{ case ((x723, x724), x725) => 
  (x723, (x724,x725))
}.groupByLabel() 
val M_flat1 = x728
val x729 = M_flat1
//M_flat1.collect.foreach(println(_))
x729.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis()
   println("ShredQuery4Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
