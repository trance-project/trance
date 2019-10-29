
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record680(ps_partkey: Int, ps_suppkey: Int)
case class Record681(s_name: String, s_nationkey: Int, s_suppkey: Int)
case class Record682(ps_partkey: Int, s_name: String, s_nationkey: Int)
case class Record683(o_orderkey: Int, o_custkey: Int)
case class Record684(c_name: String, c_nationkey: Int, c_custkey: Int)
case class Record685(o_orderkey: Int, c_name: String, c_nationkey: Int)
case class Record686(l_partkey: Int, l_orderkey: Int)
case class Record687(l_partkey: Int, c_name: String, c_nationkey: Int)
case class Record688(p_name: String, p_partkey: Int)
case class Record689(p__Fp_partkey: Int)
case class Record690(p_name: String, suppliers: Record689, customers: Record689)
case class Record691(pss__Fps_partkey: Int)
case class Record692(s_name: String, s_nationkey: Int)
case class Record693(_1: Record691, _2: Iterable[Record692])
case class Record694(cp__Fl_partkey: Int)
case class Record695(c_name: String, c_nationkey: Int)
case class Record696(_1: Record694, _2: Iterable[Record695])
object ShredQuery3Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery3Spark"+sf)
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
val PS__F = 5
val PS__D_1 = tpch.loadPartSupp
PS__D_1.cache
PS__D_1.count
val S__F = 6
val S__D_1 = tpch.loadSupplier
S__D_1.cache
S__D_1.count

   def f = { 
 val x576 = PS__D_1.map(x572 => { val x573 = x572.ps_partkey 
val x574 = x572.ps_suppkey 
val x575 = Record680(x573, x574) 
x575 }) 
val x577 = S__D_1 
val x583 = x577.map(x578 => { val x579 = x578.s_name 
val x580 = x578.s_nationkey 
val x581 = x578.s_suppkey 
val x582 = Record681(x579, x580, x581) 
x582 }) 
val x588 = { val out1 = x576.map{ case x584 => ({val x586 = x584.ps_suppkey 
x586}, x584) }
  val out2 = x583.map{ case x585 => ({val x587 = x585.s_suppkey 
x587}, x585) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x595 = x588.map{ case (x589, x590) => 
   val x591 = x589.ps_partkey 
val x592 = x590.s_name 
val x593 = x590.s_nationkey 
val x594 = Record682(x591, x592, x593) 
x594 
} 
val partsuppliers__D_1 = x595
val x596 = partsuppliers__D_1
//partsuppliers__D_1.collect.foreach(println(_))
val x601 = O__D_1.map(x597 => { val x598 = x597.o_orderkey 
val x599 = x597.o_custkey 
val x600 = Record683(x598, x599) 
x600 }) 
val x602 = C__D_1 
val x608 = x602.map(x603 => { val x604 = x603.c_name 
val x605 = x603.c_nationkey 
val x606 = x603.c_custkey 
val x607 = Record684(x604, x605, x606) 
x607 }) 
val x613 = { val out1 = x601.map{ case x609 => ({val x611 = x609.o_custkey 
x611}, x609) }
  val out2 = x608.map{ case x610 => ({val x612 = x610.c_custkey 
x612}, x610) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x620 = x613.map{ case (x614, x615) => 
   val x616 = x614.o_orderkey 
val x617 = x615.c_name 
val x618 = x615.c_nationkey 
val x619 = Record685(x616, x617, x618) 
x619 
} 
val custorders__D_1 = x620
val x621 = custorders__D_1
//custorders__D_1.collect.foreach(println(_))
val x623 = custorders__D_1 
val x624 = L__D_1 
val x629 = x624.map(x625 => { val x626 = x625.l_partkey 
val x627 = x625.l_orderkey 
val x628 = Record686(x626, x627) 
x628 }) 
val x634 = { val out1 = x623.map{ case x630 => ({val x632 = x630.o_orderkey 
x632}, x630) }
  val out2 = x629.map{ case x631 => ({val x633 = x631.l_orderkey 
x633}, x631) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x641 = x634.map{ case (x635, x636) => 
   val x637 = x636.l_partkey 
val x638 = x635.c_name 
val x639 = x635.c_nationkey 
val x640 = Record687(x637, x638, x639) 
x640 
} 
val cparts__D_1 = x641
val x642 = cparts__D_1
//cparts__D_1.collect.foreach(println(_))
val x647 = P__D_1.map(x643 => { val x644 = x643.p_name 
val x645 = x643.p_partkey 
val x646 = Record688(x644, x645) 
x646 }) 
val x653 = x647.map{ case x648 => 
   val x649 = x648.p_name 
val x650 = x648.p_partkey 
val x651 = Record689(x650) 
val x652 = Record690(x649, x651, x651) 
x652 
} 
val M_flat1 = x653
val x654 = M_flat1
//M_flat1.collect.foreach(println(_))
val x656 = partsuppliers__D_1 
val x665 = x656.map{ case x657 => 
   val x658 = x657.ps_partkey 
val x659 = Record691(x658) 
val x660 = x657.s_name 
val x661 = x657.s_nationkey 
val x662 = Record692(x660, x661) 
val x663 = List(x662) 
val x664 = Record693(x659, x663) 
x664 
} 
val M_flat2 = x665
val x666 = M_flat2
//M_flat2.collect.foreach(println(_))
val x668 = cparts__D_1 
val x677 = x668.map{ case x669 => 
   val x670 = x669.l_partkey 
val x671 = Record694(x670) 
val x672 = x669.c_name 
val x673 = x669.c_nationkey 
val x674 = Record695(x672, x673) 
val x675 = List(x674) 
val x676 = Record696(x671, x675) 
x676 
} 
val M_flat3 = x677
val x678 = M_flat3
//M_flat3.collect.foreach(println(_))
x678.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery3Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
