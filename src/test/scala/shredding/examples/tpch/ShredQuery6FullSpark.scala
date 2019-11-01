
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record663(o_orderkey: Int, o_custkey: Int)
case class Record664(c_name: String, c_custkey: Int)
case class Record665(o_orderkey: Int, c_name: String)
case class Record666(s_name: String, s_suppkey: Int)
case class Record667(s__Fs_suppkey: Int)
case class Record668(s_name: String, customers2: Record667)
case class Record669(l_suppkey: Int, l_orderkey: Int)
case class Record671(c_name2: String)
case class Record710(c_name: String)
case class Record711(c__Fc_name: String)
case class Record712(c_name: String, suppliers: Record711)
case class Record713(s_name: String)
object ShredQuery6FullSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6FullSpark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L__F = 3
val L__D_1 = tpch.loadLineitem
L__D_1.cache
L__D_1.count
val C__F = 1
val C__D_1 = tpch.loadCustomers
C__D_1.cache
C__D_1.count
val O__F = 2
val O__D_1 = tpch.loadOrders
O__D_1.cache
O__D_1.count
val S__F = 6
val S__D_1 = tpch.loadSupplier
S__D_1.cache
S__D_1.count

   val x603 = O__D_1.map(x599 => { val x600 = x599.o_orderkey 
val x601 = x599.o_custkey 
val x602 = Record663(x600, x601) 
x602 }) 
val x604 = C__D_1 
val x609 = x604.map(x605 => { val x606 = x605.c_name 
val x607 = x605.c_custkey 
val x608 = Record664(x606, x607) 
x608 }) 
val x614 = { val out1 = x603.map{ case x610 => ({val x612 = x610.o_custkey 
x612}, x610) }
  val out2 = x609.map{ case x611 => ({val x613 = x611.c_custkey 
x613}, x611) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x620 = x614.map{ case (x615, x616) => 
   val x617 = x615.o_orderkey 
val x618 = x616.c_name 
val x619 = Record665(x617, x618) 
x619 
} 
val resultInner__D_1 = x620
val x621 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val x626 = S__D_1.map(x622 => { val x623 = x622.s_name 
val x624 = x622.s_suppkey 
val x625 = Record666(x623, x624) 
x625 }) 
val x632 = x626.map{ case x627 => 
   val x628 = x627.s_name 
val x629 = x627.s_suppkey 
val x630 = Record667(x629) 
val x631 = Record668(x628, x630) 
x631 
} 
val M_flat1 = x632
val x633 = M_flat1
//M_flat1.collect.foreach(println(_))
val x638 = L__D_1.map(x634 => { val x635 = x634.l_suppkey 
val x636 = x634.l_orderkey 
val x637 = Record669(x635, x636) 
x637 }) 
val x639 = resultInner__D_1 
val x641 = x639 
val x646 = { val out1 = x638.map{ case x642 => ({val x644 = x642.l_orderkey 
x644}, x642) }
  val out2 = x641.map{ case x643 => ({val x645 = x643.o_orderkey 
x645}, x643) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x654 = x646.flatMap{ case (x647, x648) => val x653 = (x648) 
x653 match {
   case (null) => Nil 
   case x652 => List(({val x649 = (x647) 
x649}, {val x650 = x648.c_name 
val x651 = Record671(x650) 
x651}))
 }
}.groupByLabel() 
val x660 = x654.map{ case (x655, x656) => 
   val x657 = x655.l_suppkey 
val x658 = Record667(x657) 
val x659 = (x658, x656) 
x659 
} 
val M_flat2 = x660
val x661 = M_flat2
//M_flat2.collect.foreach(println(_))
val Query2Full__D_1 = M_flat1
Query2Full__D_1.cache
Query2Full__D_1.count
val Query2Full__D_2customers2_1 = M_flat2
Query2Full__D_2customers2_1.cache
Query2Full__D_2customers2_1.count
def f = { 
 val x681 = C__D_1.map(x678 => { val x679 = x678.c_name 
val x680 = Record710(x679) 
x680 }) 
val x686 = x681.map{ case x682 => 
   val x683 = x682.c_name 
val x684 = Record711(x683) 
val x685 = Record712(x683, x684) 
x685 
} 
val M_flat1 = x686
val x687 = M_flat1
//M_flat1.collect.foreach(println(_))
val x689 = Query2Full__D_1 
val x691 = Query2Full__D_2customers2_1 
val x694 = x691 
val x698 = { val out1 = x689.map{ case x695 => ({val x697 = x695.customers2 
x697}, x695) }
  val out2 = x694.flatMapValues(identity)
  out1.lookup(out2)
} 
val x707 = x698.map{ case (x699, x700) => 
   val x701 = x700.c_name2 
val x702 = Record711(x701) 
val x703 = x699.s_name 
val x704 = Record713(x703) 
val x705 = List(x704) 
val x706 = (x702, x705) 
x706 
}.groupByLabel() 
val M_flat2 = x707
val x708 = M_flat2
M_flat2.collect.foreach(println(_))
x708.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery6FullSpark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
