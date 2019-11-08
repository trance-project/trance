
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record804(lbl: Unit)
case class Record805(ps_partkey: Int, ps_suppkey: Int)
case class Record806(s_name: String, s_nationkey: Int, s_suppkey: Int)
case class Record808(ps_partkey: Int, s_name: String, s_nationkey: Int)
case class Record809(_1: Record804, _2: (Iterable[Record808]))
case class Record810(o_orderkey: Int, o_custkey: Int)
case class Record811(c_name: String, c_nationkey: Int, c_custkey: Int)
case class Record813(o_orderkey: Int, c_name: String, c_nationkey: Int)
case class Record814(_1: Record804, _2: (Iterable[Record813]))
case class Record815(l_partkey: Int, l_orderkey: Int)
case class Record817(l_partkey: Int, c_name: String, c_nationkey: Int)
case class Record818(_1: Record804, _2: (Iterable[Record817]))
case class Record819(p_name: String, p_partkey: Int)
case class Record821(p__Fp_partkey: Int)
case class Record822(p_name: String, suppliers: Record821, customers: Record821)
case class Record823(_1: Record804, _2: (Iterable[Record822]))
case class Record824(lbl: Record821)
case class Record826(s_name: String, s_nationkey: Int)
case class Record827(_1: Record824, _2: (Iterable[Record826]))
case class Record829(c_name: String, c_nationkey: Int)
case class Record830(_1: Record824, _2: (Iterable[Record829]))
case class Record955(n_name: String, n_nationkey: Int)
case class Record957(n__Fn_nationkey: Int)
case class Record958(n_name: String, parts: Record957)
case class Record959(_1: Record804, _2: (Iterable[Record958]))
case class Record960(lbl: Record957)
case class Record963(p_name: String)
case class Record964(_1: Record960, _2: (Iterable[Record963]))
object ShredQuery7FullSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery7FullSpark"+sf)
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

   val x569 = () 
val x570 = Record804(x569) 
val partsuppliers__F = x570
val x571 = partsuppliers__F
//partsuppliers__F.collect.foreach(println(_))
val x572 = List(partsuppliers__F) 
val x574 = x572 
val x575 = PS__D_1 
val x580 = x575.map(x576 => { val x577 = x576.ps_partkey 
val x578 = x576.ps_suppkey 
val x579 = Record805(x577, x578) 
x579 }) 
val x583 = x580.map{ case c => (x574.head, c) } 
val x584 = S__D_1 
val x590 = x584.map(x585 => { val x586 = x585.s_name 
val x587 = x585.s_nationkey 
val x588 = x585.s_suppkey 
val x589 = Record806(x586, x587, x588) 
x589 }) 
val x596 = { val out1 = x583.map{ case (x591, x592) => ({val x594 = x592.ps_suppkey 
x594}, (x591, x592)) }
  val out2 = x590.map{ case x593 => ({val x595 = x593.s_suppkey 
x595}, x593) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x607 = x596.flatMap{ case ((x597, x598), x599) => val x606 = (x598,x599) 
x606 match {
   case (_,null) => Nil 
   case x605 => List(({val x600 = (x597) 
x600}, {val x601 = x598.ps_partkey 
val x602 = x599.s_name 
val x603 = x599.s_nationkey 
val x604 = Record808(x601, x602, x603) 
x604}))
 }
}.groupByLabel() 
val x612 = x607.map{ case (x608, x609) => 
   val x610 = (x609) 
val x611 = Record809(x608, x610) 
x611 
} 
val partsuppliers__D_1 = x612
val x613 = partsuppliers__D_1
//partsuppliers__D_1.collect.foreach(println(_))
val custorders__F = x570
val x614 = custorders__F
//custorders__F.collect.foreach(println(_))
val x615 = List(custorders__F) 
val x617 = x615 
val x618 = O__D_1 
val x623 = x618.map(x619 => { val x620 = x619.o_orderkey 
val x621 = x619.o_custkey 
val x622 = Record810(x620, x621) 
x622 }) 
val x626 = x623.map{ case c => (x617.head, c) } 
val x627 = C__D_1 
val x633 = x627.map(x628 => { val x629 = x628.c_name 
val x630 = x628.c_nationkey 
val x631 = x628.c_custkey 
val x632 = Record811(x629, x630, x631) 
x632 }) 
val x639 = { val out1 = x626.map{ case (x634, x635) => ({val x637 = x635.o_custkey 
x637}, (x634, x635)) }
  val out2 = x633.map{ case x636 => ({val x638 = x636.c_custkey 
x638}, x636) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x650 = x639.flatMap{ case ((x640, x641), x642) => val x649 = (x641,x642) 
x649 match {
   case (_,null) => Nil 
   case x648 => List(({val x643 = (x640) 
x643}, {val x644 = x641.o_orderkey 
val x645 = x642.c_name 
val x646 = x642.c_nationkey 
val x647 = Record813(x644, x645, x646) 
x647}))
 }
}.groupByLabel() 
val x655 = x650.map{ case (x651, x652) => 
   val x653 = (x652) 
val x654 = Record814(x651, x653) 
x654 
} 
val custorders__D_1 = x655
val x656 = custorders__D_1
//custorders__D_1.collect.foreach(println(_))
val cparts__F = x570
val x657 = cparts__F
//cparts__F.collect.foreach(println(_))
val x658 = List(cparts__F) 
val x660 = x658 
val x661 = custorders__D_1 
val x663 = x661 
val x666 = x663.map{ case c => (x660.head, c) } 
val x667 = L__D_1 
val x672 = x667.map(x668 => { val x669 = x668.l_partkey 
val x670 = x668.l_orderkey 
val x671 = Record815(x669, x670) 
x671 }) 
val x678 = { val out1 = x666.map{ case (x673, x674) => ({val x676 = x674.o_orderkey 
x676}, (x673, x674)) }
  val out2 = x672.map{ case x675 => ({val x677 = x675.l_orderkey 
x677}, x675) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x689 = x678.flatMap{ case ((x679, x680), x681) => val x688 = (x680,x681) 
x688 match {
   case (_,null) => Nil 
   case x687 => List(({val x682 = (x679) 
x682}, {val x683 = x681.l_partkey 
val x684 = x680.c_name 
val x685 = x680.c_nationkey 
val x686 = Record817(x683, x684, x685) 
x686}))
 }
}.groupByLabel() 
val x694 = x689.map{ case (x690, x691) => 
   val x692 = (x691) 
val x693 = Record818(x690, x692) 
x693 
} 
val cparts__D_1 = x694
val x695 = cparts__D_1
//cparts__D_1.collect.foreach(println(_))
val x696 = List(x570) 
val M_ctx1 = x696
val x697 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x699 = M_ctx1 
val x700 = P__D_1 
val x705 = x700.map(x701 => { val x702 = x701.p_name 
val x703 = x701.p_partkey 
val x704 = Record819(x702, x703) 
x704 }) 
val x708 = x705.map{ case c => (x699.head, c) } 
val x718 = x708.flatMap{ case (x709, x710) => val x717 = (x710) 
x717 match {
   case (null) => Nil 
   case x716 => List(({val x711 = (x709) 
x711}, {val x712 = x710.p_name 
val x713 = x710.p_partkey 
val x714 = Record821(x713) 
val x715 = Record822(x712, x714, x714) 
x715}))
 }
}.groupByLabel() 
val x723 = x718.map{ case (x719, x720) => 
   val x721 = (x720) 
val x722 = Record823(x719, x721) 
x722 
} 
val M_flat1 = x723
val x724 = M_flat1
//M_flat1.collect.foreach(println(_))
val x726 = M_flat1 
val x730 = x726.flatMap{ case x727 => x727 match {
   case null => List((x727, null))
   case _ =>
   val x728 = x727._2 
x728 match {
     case x729 => x729.map{ case v2 => (x727, v2) }
  }
 }} 
val x735 = x730.map{ case (x731, x732) => 
   val x733 = x732.suppliers 
val x734 = Record824(x733) 
x734 
} 
val x736 = x735.distinct 
val M_ctx2 = x736
val x737 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x739 = M_ctx2 
val x740 = partsuppliers__D_1 
val x742 = x740 
val x748 = { val out1 = x739.map{ case x743 => ({val x745 = x743.lbl 
val x746 = x745.p__Fp_partkey 
x746}, x743) }
  val out2 = x742.map{ case x744 => ({val x747 = x744.ps_partkey 
x747}, x744) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x757 = x748.flatMap{ case (x749, x750) => val x756 = (x750) 
x756 match {
   case (null) => Nil 
   case x755 => List(({val x751 = (x749) 
x751}, {val x752 = x750.s_name 
val x753 = x750.s_nationkey 
val x754 = Record826(x752, x753) 
x754}))
 }
}.groupByLabel() 
val x762 = x757.map{ case (x758, x759) => 
   val x760 = (x759) 
val x761 = Record827(x758, x760) 
x761 
} 
val M_flat2 = x762
val x763 = M_flat2
//M_flat2.collect.foreach(println(_))
val x765 = M_flat1 
val x769 = x765.flatMap{ case x766 => x766 match {
   case null => List((x766, null))
   case _ =>
   val x767 = x766._2 
x767 match {
     case x768 => x768.map{ case v2 => (x766, v2) }
  }
 }} 
val x774 = x769.map{ case (x770, x771) => 
   val x772 = x771.customers 
val x773 = Record824(x772) 
x773 
} 
val x775 = x774.distinct 
val M_ctx3 = x775
val x776 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x778 = M_ctx3 
val x779 = cparts__D_1 
val x781 = x779 
val x787 = { val out1 = x778.map{ case x782 => ({val x784 = x782.lbl 
val x785 = x784.p__Fp_partkey 
x785}, x782) }
  val out2 = x781.map{ case x783 => ({val x786 = x783.l_partkey 
x786}, x783) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x796 = x787.flatMap{ case (x788, x789) => val x795 = (x789) 
x795 match {
   case (null) => Nil 
   case x794 => List(({val x790 = (x788) 
x790}, {val x791 = x789.c_name 
val x792 = x789.c_nationkey 
val x793 = Record829(x791, x792) 
x793}))
 }
}.groupByLabel() 
val x801 = x796.map{ case (x797, x798) => 
   val x799 = (x798) 
val x800 = Record830(x797, x799) 
x800 
} 
val M_flat3 = x801
val x802 = M_flat3
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
 val x847 = () 
val x848 = Record804(x847) 
val x849 = List(x848) 
val M_ctx1 = x849
val x850 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x852 = M_ctx1 
val x853 = N__D_1 
val x858 = x853.map(x854 => { val x855 = x854.n_name 
val x856 = x854.n_nationkey 
val x857 = Record955(x855, x856) 
x857 }) 
val x861 = x858.map{ case c => (x852.head, c) } 
val x871 = x861.flatMap{ case (x862, x863) => val x870 = (x863) 
x870 match {
   case (null) => Nil 
   case x869 => List(({val x864 = (x862) 
x864}, {val x865 = x863.n_name 
val x866 = x863.n_nationkey 
val x867 = Record957(x866) 
val x868 = Record958(x865, x867) 
x868}))
 }
}.groupByLabel() 
val x876 = x871.map{ case (x872, x873) => 
   val x874 = (x873) 
val x875 = Record959(x872, x874) 
x875 
} 
val M_flat1 = x876
val x877 = M_flat1
//M_flat1.collect.foreach(println(_))
val x879 = M_flat1 
val x883 = x879.flatMap{ case x880 => x880 match {
   case null => List((x880, null))
   case _ =>
   val x881 = x880._2 
x881 match {
     case x882 => x882.map{ case v2 => (x880, v2) }
  }
 }} 
val x888 = x883.map{ case (x884, x885) => 
   val x886 = x885.parts 
val x887 = Record960(x886) 
x887 
} 
val x889 = x888.distinct 
val M_ctx2 = x889
val x890 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x892 = M_ctx2 
val x893 = Query3__D_1 
val x895 = x893 
val x898 = x895.map{ case c => (x892.head, c) } 
val x900 = Query3__D_2suppliers_1 
val x903 = x900 
val x908 = { val out1 = x898.map{ case (a, null) => (null, (a, null)); case (x904, x905) => ({val x907 = x905.suppliers 
x907}, (x904, x905)) }
  val out2 = x903.flatMapValues(identity)
  out1.outerLookup(out2)
} 
val x909 = Query3__D_2customers_1 
val x912 = x909 
val x918 = { val out1 = x908.map{ case (a, null) => (null, (a, null)); case ((x913, x914), x915) => ({val x917 = x914.customers 
x917}, ((x913, x914), x915)) }
  val out2 = x912.flatMapValues(identity)
  out1.outerLookup(out2)
} 
val x931 = x918.flatMap{ case (((x919, x920), x921), x922) => val x930 = (x922) 
x930 match {
   case (null) => Nil
   case x929 => List(({val x923 = (x919,x920,x921) 
x923}, {val x924 = x922.c_nationkey 
val x925 = x919.lbl 
val x926 = x925.n__Fn_nationkey 
val x927 = x924 == x926 
val x928 = 
 if ({x927})
 {  1}
 else 0  
x928}))
 }
}.reduceByKey(_ + _) 
val x947 = x931.flatMap{ case ((x932, x933, x934), x935) => val x946 = (x933,x934) 
x946 match {
   case x939 if {val x940 = x841.s_nationkey 
val x941 = x932.lbl 
val x942 = x941.n__Fn_nationkey 
val x943 = x940 == x942 
val x944 = x935 == 0 
val x945 = x943 && x944 
x945} => List(({val x936 = (x932) 
x936}, {val x937 = x933.p_name 
val x938 = Record963(x937) 
x938}))
   case x939 => List(({val x936 = (x932) 
x936}, null))
 }    
}.groupByLabel() 
val x952 = x947.map{ case (x948, x949) => 
   val x950 = (x949) 
val x951 = Record964(x948, x950) 
x951 
} 
val M_flat2 = x952
val x953 = M_flat2
//M_flat2.collect.foreach(println(_))
x953.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery7FullSpark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
