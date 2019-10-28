
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record670(ps_partkey: Int, ps_suppkey: Int)
case class Record671(s_name: String, s_nationkey: Int, s_suppkey: Int)
case class Record672(ps_partkey: Int, s_name: String, s_nationkey: Int)
case class Record673(o_orderkey: Int, o_custkey: Int)
case class Record674(c_name: String, c_nationkey: Int, c_custkey: Int)
case class Record675(o_orderkey: Int, c_name: String, c_nationkey: Int)
case class Record676(l_partkey: Int, l_orderkey: Int)
case class Record677(l_partkey: Int, c_name: String, c_nationkey: Int)
case class Record678(p_name: String, p_partkey: Int)
case class Record679(p__Fp_partkey: Int)
case class Record680(p_name: String, suppliers: Record679, customers: Record679)
case class Record681(s_name: String, s_nationkey: Int)
case class Record682(_1: Int, _2: Iterable[Record681])
case class Record683(c_name: String, c_nationkey: Int)
case class Record684(_1: Int, _2: Iterable[Record683])
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
 val x568 = PS__D_1.map(x564 => { val x565 = x564.ps_partkey 
val x566 = x564.ps_suppkey 
val x567 = Record670(x565, x566) 
x567 }) 
val x569 = S__D_1 
val x575 = x569.map(x570 => { val x571 = x570.s_name 
val x572 = x570.s_nationkey 
val x573 = x570.s_suppkey 
val x574 = Record671(x571, x572, x573) 
x574 }) 
val x580 = { val out1 = x568.map{ case x576 => ({val x578 = x576.ps_suppkey 
x578}, x576) }
  val out2 = x575.map{ case x577 => ({val x579 = x577.s_suppkey 
x579}, x577) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x587 = x580.map{ case (x581, x582) => 
   val x583 = x581.ps_partkey 
val x584 = x582.s_name 
val x585 = x582.s_nationkey 
val x586 = Record672(x583, x584, x585) 
x586 
} 
val partsuppliers__D_1 = x587
val x588 = partsuppliers__D_1
//partsuppliers__D_1.collect.foreach(println(_))
val x593 = O__D_1.map(x589 => { val x590 = x589.o_orderkey 
val x591 = x589.o_custkey 
val x592 = Record673(x590, x591) 
x592 }) 
val x594 = C__D_1 
val x600 = x594.map(x595 => { val x596 = x595.c_name 
val x597 = x595.c_nationkey 
val x598 = x595.c_custkey 
val x599 = Record674(x596, x597, x598) 
x599 }) 
val x605 = { val out1 = x593.map{ case x601 => ({val x603 = x601.o_custkey 
x603}, x601) }
  val out2 = x600.map{ case x602 => ({val x604 = x602.c_custkey 
x604}, x602) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x612 = x605.map{ case (x606, x607) => 
   val x608 = x606.o_orderkey 
val x609 = x607.c_name 
val x610 = x607.c_nationkey 
val x611 = Record675(x608, x609, x610) 
x611 
} 
val custorders__D_1 = x612
val x613 = custorders__D_1
//custorders__D_1.collect.foreach(println(_))
val x615 = custorders__D_1 
val x616 = L__D_1 
val x621 = x616.map(x617 => { val x618 = x617.l_partkey 
val x619 = x617.l_orderkey 
val x620 = Record676(x618, x619) 
x620 }) 
val x626 = { val out1 = x615.map{ case x622 => ({val x624 = x622.o_orderkey 
x624}, x622) }
  val out2 = x621.map{ case x623 => ({val x625 = x623.l_orderkey 
x625}, x623) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x633 = x626.map{ case (x627, x628) => 
   val x629 = x628.l_partkey 
val x630 = x627.c_name 
val x631 = x627.c_nationkey 
val x632 = Record677(x629, x630, x631) 
x632 
} 
val cparts__D_1 = x633
val x634 = cparts__D_1
//cparts__D_1.collect.foreach(println(_))
val x639 = P__D_1.map(x635 => { val x636 = x635.p_name 
val x637 = x635.p_partkey 
val x638 = Record678(x636, x637) 
x638 }) 
val x645 = x639.map{ case x640 => 
   val x641 = x640.p_name 
val x642 = x640.p_partkey 
val x643 = Record679(x642) 
val x644 = Record680(x641, x643, x643) 
x644 
} 
val M_flat1 = x645
val x646 = M_flat1
//M_flat1.collect.foreach(println(_))
val x648 = partsuppliers__D_1 
val x656 = x648.map{ case x649 => 
   val x650 = x649.ps_partkey 
val x651 = x649.s_name 
val x652 = x649.s_nationkey 
val x653 = Record681(x651, x652) 
val x654 = List(x653) 
val x655 = Record682(x650, x654) 
x655 
} 
val M_flat2 = x656
val x657 = M_flat2
//M_flat2.collect.foreach(println(_))
val x659 = cparts__D_1 
val x667 = x659.map{ case x660 => 
   val x661 = x660.l_partkey 
val x662 = x660.c_name 
val x663 = x660.c_nationkey 
val x664 = Record683(x662, x663) 
val x665 = List(x664) 
val x666 = Record684(x661, x665) 
x666 
} 
val M_flat3 = x667
val x668 = M_flat3
M_flat3.collect.foreach(println(_))
x668.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery3Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
