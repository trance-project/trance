
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record540(lbl: Unit)
case class Record541(c_name: String, c_custkey: Int)
case class Record543(c__Fc_custkey: Int)
case class Record544(c_name: String, c_orders: Record543)
case class Record545(_1: Record540, _2: (Iterable[Record544]))
case class Record546(lbl: Record543)
case class Record547(o_orderkey: Int, o_orderdate: String, o_custkey: Int)
case class Record549(o_orderkey: Int, o_orderdate: String)
case class Record550(_1: Record546, _2: (Iterable[Record549]))
case class Record698(l_quantity: Double, l_orderkey: Int, l_partkey: Int)
case class Record699(p_name: String, p_partkey: Int)
case class Record701(l_orderkey: Int, p_name: String)
case class Record703(c_name: String, o_orderdate: String, p_name: String)
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

//   val CustOrders = {
 val x465 = () 
val x466 = Record540(x465) 
val x467 = List(x466) 
val M_ctx1 = x467
val x468 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x470 = M_ctx1 
val x471 = C__D_1 
val x476 = x471.map(x472 => { val x473 = x472.c_name 
val x474 = x472.c_custkey 
val x475 = Record541(x473, x474) 
x475 }) 
val x479 = x476.map{ case c => (x470.head, c) } 
val x489 = x479.map{ case (x480, x481) => val x488 = (x481) 
x488 match {
   case (null) => ({val x482 = (x480) 
x482}, null) 
   case x487 => ({val x482 = (x480) 
x482}, {val x483 = x481.c_name 
val x484 = x481.c_custkey 
val x485 = Record543(x484) 
val x486 = Record544(x483, x485) 
x486})
 }
}.groupByLabel() 
val x494 = x489.map{ case (x490, x491) => 
   val x492 = (x491) 
val x493 = Record545(x490, x492) 
x493 
} 
val M_flat1 = x494
val x495 = M_flat1
//M_flat1.collect.foreach(println(_))
val x497 = M_flat1 
val x501 = x497.flatMap{ case x498 => x498 match {
   case null => List((x498, null))
   case _ =>
   val x499 = x498._2 
x499 match {
     case x500 => x500.map{ case v2 => (x498, v2) }
  }
 }} 
val x506 = x501.map{ case (x502, x503) => 
   val x504 = x503.c_orders 
val x505 = Record546(x504) 
x505 
} 
val x507 = x506.distinct 
val M_ctx2 = x507
val x508 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x510 = M_ctx2 
val x511 = O__D_1 
val x517 = x511.map(x512 => { val x513 = x512.o_orderkey 
val x514 = x512.o_orderdate 
val x515 = x512.o_custkey 
val x516 = Record547(x513, x514, x515) 
x516 }) 
val x523 = { val out1 = x510.map{ case x518 => ({val x520 = x518.lbl 
val x521 = x520.c__Fc_custkey 
x521}, x518) }
  val out2 = x517.map{ case x519 => ({val x522 = x519.o_custkey 
x522}, x519) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x532 = x523.map{ case (x524, x525) => val x531 = (x525) 
x531 match {
   case (null) => ({val x526 = (x524) 
x526}, null) 
   case x530 => ({val x526 = (x524) 
x526}, {val x527 = x525.o_orderkey 
val x528 = x525.o_orderdate 
val x529 = Record549(x527, x528) 
x529})
 }
}.groupByLabel() 
val x537 = x532.map{ case (x533, x534) => 
   val x535 = (x534) 
val x536 = Record550(x533, x535) 
x536 
} 
val M_flat2 = x537
val x538 = M_flat2
//M_flat2.collect.foreach(println(_))
//x538
//}
val CustOrders__D_1 = M_flat1.flatMap(r => r._2)
CustOrders__D_1.cache
CustOrders__D_1.count
val CustOrders__D_2c_orders_1 = M_flat2
CustOrders__D_2c_orders_1.cache
CustOrders__D_2c_orders_1.count
def f = { 
 val x601 = () 
val x602 = Record540(x601) 
val partcnts__F = x602
val x603 = partcnts__F
//partcnts__F.collect.foreach(println(_))
val x604 = List(partcnts__F) 
val x606 = x604 
val x607 = L__D_1 
val x613 = x607.map(x608 => { val x609 = x608.l_quantity 
val x610 = x608.l_orderkey 
val x611 = x608.l_partkey 
val x612 = Record698(x609, x610, x611) 
x612 }) 
val x616 = x613.map{ case c => (x606.head, c) } 
val x617 = P__D_1 
val x622 = x617.map(x618 => { val x619 = x618.p_name 
val x620 = x618.p_partkey 
val x621 = Record699(x619, x620) 
x621 }) 
val x628 = { val out1 = x616.map{ case (x623, x624) => ({val x626 = x624.l_partkey 
x626}, (x623, x624)) }
  val out2 = x622.map{ case x625 => ({val x627 = x625.p_partkey 
x627}, x625) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x639 = x628.map{ case ((x629, x630), x631) => 
   ({val x632 = x630.l_orderkey 
val x633 = x631.p_name 
val x634 = Record701(x632, x633) 
val x635 = (x629,x634) 
x635}, {val x636 = x630.l_quantity 
x636})
}.reduceByKey(_ + _) 
val x645 = x639.map{ case ((x640, x641), x642) => 
  (x640, (x641,x642))
}.groupByLabel() 
val partcnts__D_1 = x645.flatMap(r => r._2)
val x646 = partcnts__D_1
//partcnts__D_1.collect.foreach(println(_))
val x647 = List(x602) 
val M_ctx1 = x647
val x648 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x650 = M_ctx1 
val x651 = CustOrders__D_1 
val x653 = x651 
val x656 = x653.map{ case c => (x650.head, c) } 
val x658 = CustOrders__D_2c_orders_1 
val x661 = x658 
val x666 = { val out1 = x656.map{ case (a, null) => (null, (a, null)); case (x662, x663) => ({val x665 = x663.c_orders 
x665}, (x662, x663)) }
  val out2 = x661.flatMap(x664 => x664._2.map{case v2 => (x664._1.lbl, v2)})
  out1.outerLookup(out2)
} 
val x667 = partcnts__D_1 
val x669 = x667 
val x676 = { val out1 = x666.map{ case ((x670, x671), x672) => ({val x674 = x672.o_orderkey 
x674}, ((x670, x671), x672)) }
  val out2 = x669.map{ case x673 => ({val x675 = x673._1.l_orderkey 
x675}, x673) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x689 = x676.map{ case (((x677, x678), x679), x680) => 
   ({val x681 = x678.c_name 
val x682 = x679.o_orderdate 
val x683 = x680._1.p_name 
val x684 = Record703(x681, x682, x683) 
val x685 = (x677,x684) 
x685}, {val x686 = x680._2 
x686})
}.reduceByKey(_ + _) 
val x695 = x689.map{ case ((x690, x691), x692) => 
  (x690, (x691,x692))
}.groupByLabel() 
val M_flat1 = x695
val x696 = M_flat1
M_flat1.collect.foreach(println(_))
x696.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis()
   println("ShredQuery4Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
