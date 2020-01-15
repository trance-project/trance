
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record649(ps_partkey: Int, ps_suppkey: Int)
case class Record650(s_name: String, s_nationkey: Int, s_suppkey: Int)
case class Record651(ps_partkey: Int, s_name: String, s_nationkey: Int)
case class Record652(o_orderkey: Int, o_custkey: Int)
case class Record653(c_name: String, c_nationkey: Int, c_custkey: Int)
case class Record654(o_orderkey: Int, c_name: String, c_nationkey: Int)
case class Record655(l_partkey: Int, l_orderkey: Int)
case class Record656(l_partkey: Int, c_name: String, c_nationkey: Int)
case class Record657(p__Fp_partkey: Int)
case class Record658(p_name: String, suppliers: Record657, customers: Record657)
case class Record659(s_name: String, s_nationkey: Int)
case class Record660(c_name: String, c_nationkey: Int)
case class Record717(n__Fn_nationkey: Int)
case class Record718(n_name: String, suppliers: Record717, customers: Record717)
case class Record719(s_name: String)
case class Record720(c_name: String)
object ShredQuery7Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery7Spark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val N__F = 6
val N__D_1 = tpch.loadNation
N__D_1.cache
N__D_1.count
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
val PS__F = 5
val PS__D_1 = tpch.loadPartSupp
PS__D_1.cache
PS__D_1.count
val S__F = 6
val S__D_1 = tpch.loadSupplier
S__D_1.cache
S__D_1.count

   val x557 = PS__D_1.map(x553 => { val x554 = x553.ps_partkey 
val x555 = x553.ps_suppkey 
val x556 = Record649(x554, x555) 
x556 }) 
val x563 = S__D_1.map(x558 => { val x559 = x558.s_name 
val x560 = x558.s_nationkey 
val x561 = x558.s_suppkey 
val x562 = Record650(x559, x560, x561) 
x562 }) 
val x568 = { val out1 = x557.map{ case x564 => ({val x566 = x564.ps_suppkey 
x566}, x564) }
  val out2 = x563.map{ case x565 => ({val x567 = x565.s_suppkey 
x567}, x565) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x575 = x568.map{ case (x569, x570) => 
   val x571 = x569.ps_partkey 
val x572 = x570.s_name 
val x573 = x570.s_nationkey 
val x574 = Record651(x571, x572, x573) 
x574 
} 
val partsuppliers__D_1 = x575
val x576 = partsuppliers__D_1
//partsuppliers__D_1.collect.foreach(println(_))
val x581 = O__D_1.map(x577 => { val x578 = x577.o_orderkey 
val x579 = x577.o_custkey 
val x580 = Record652(x578, x579) 
x580 }) 
val x587 = C__D_1.map(x582 => { val x583 = x582.c_name 
val x584 = x582.c_nationkey 
val x585 = x582.c_custkey 
val x586 = Record653(x583, x584, x585) 
x586 }) 
val x592 = { val out1 = x581.map{ case x588 => ({val x590 = x588.o_custkey 
x590}, x588) }
  val out2 = x587.map{ case x589 => ({val x591 = x589.c_custkey 
x591}, x589) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x599 = x592.map{ case (x593, x594) => 
   val x595 = x593.o_orderkey 
val x596 = x594.c_name 
val x597 = x594.c_nationkey 
val x598 = Record654(x595, x596, x597) 
x598 
} 
val custorders__D_1 = x599
val x600 = custorders__D_1
//custorders__D_1.collect.foreach(println(_))
val x602 = custorders__D_1 
val x607 = L__D_1.map(x603 => { val x604 = x603.l_partkey 
val x605 = x603.l_orderkey 
val x606 = Record655(x604, x605) 
x606 }) 
val x612 = { val out1 = x602.map{ case x608 => ({val x610 = x608.o_orderkey 
x610}, x608) }
  val out2 = x607.map{ case x609 => ({val x611 = x609.l_orderkey 
x611}, x609) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x619 = x612.map{ case (x613, x614) => 
   val x615 = x614.l_partkey 
val x616 = x613.c_name 
val x617 = x613.c_nationkey 
val x618 = Record656(x615, x616, x617) 
x618 
} 
val cparts__D_1 = x619
val x620 = cparts__D_1
//cparts__D_1.collect.foreach(println(_))
val x626 = P__D_1.map{ case x621 => 
   val x622 = x621.p_name 
val x623 = x621.p_partkey 
val x624 = Record657(x623) 
val x625 = Record658(x622, x624, x624) 
x625 
} 
val M_flat1 = x626
val x627 = M_flat1
//M_flat1.collect.foreach(println(_))
val x636 = partsuppliers__D_1.map{ case x628 => 
   val x629 = x628.ps_partkey 
val x630 = Record657(x629) 
val x631 = x628.s_name 
val x632 = x628.s_nationkey 
val x633 = Record659(x631, x632) 
val x634 = List(x633) 
val x635 = (x630, x634) 
x635 
} 
val M_flat2 = x636
val x637 = M_flat2
//M_flat2.collect.foreach(println(_))
val x646 = cparts__D_1.map{ case x638 => 
   val x639 = x638.l_partkey 
val x640 = Record657(x639) 
val x641 = x638.c_name 
val x642 = x638.c_nationkey 
val x643 = Record660(x641, x642) 
val x644 = List(x643) 
val x645 = (x640, x644) 
x645 
} 
val M_flat3 = x646
val x647 = M_flat3
//M_flat3.collect.foreach(println(_))
val Query3__D_1 = M_flat1
Query3__D_1.cache
Query3__D_1.count
val Query3__D_2suppliers_1 = M_flat2
Query3__D_2suppliers_1.cache
Query3__D_2suppliers_1.count
val Query3__D_2customers_1 = M_flat3
Query3__D_2customers_1.cache
Query3__D_2customers_1.count
def f = { 
 val x682 = N__D_1.map{ case x677 => 
   val x678 = x677.n_name 
val x679 = x677.n_nationkey 
val x680 = Record717(x679) 
val x681 = Record718(x678, x680, x680) 
x681 
} 
val M_flat1 = x682
val x683 = M_flat1
//M_flat1.collect.foreach(println(_))
val x685 = Query3__D_1 
val x689 = { val out1 = x685.map{ case x686 => ({val x688 = x686.suppliers 
x688}, x686) }
  val out2 = Query3__D_2suppliers_1.flatMapValues(identity)
  out1.lookup(out2)
} 
val x698 = x689.map{ case (x690, x691) => 
   val x692 = x691.s_nationkey 
val x693 = Record717(x692) 
val x694 = x691.s_name 
val x695 = Record719(x694) 
val x696 = List(x695) 
val x697 = (x693, x696) 
x697 
} 
val M_flat2 = x698
val x699 = M_flat2
//M_flat2.collect.foreach(println(_))
val x701 = Query3__D_1 
val x705 = { val out1 = x701.map{ case x702 => ({val x704 = x702.customers 
x704}, x702) }
  val out2 = Query3__D_2customers_1.flatMapValues(identity)
  out1.lookup(out2)
} 
val x714 = x705.map{ case (x706, x707) => 
   val x708 = x707.c_nationkey 
val x709 = Record717(x708) 
val x710 = x707.c_name 
val x711 = Record720(x710) 
val x712 = List(x711) 
val x713 = (x709, x712) 
x713 
} 
val M_flat3 = x714
val x715 = M_flat3
//M_flat3.collect.foreach(println(_))
x715.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery7Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
