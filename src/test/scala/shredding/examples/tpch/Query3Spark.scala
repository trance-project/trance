
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record837(ps_partkey: Int, ps_suppkey: Int)
case class Record838(s_name: String, s_nationkey: Int, s_suppkey: Int)
case class Record839(ps_partkey: Int, s_name: String, s_nationkey: Int)
case class Record840(o_orderkey: Int, o_custkey: Int)
case class Record841(c_name: String, c_nationkey: Int, c_custkey: Int)
case class Record842(o_orderkey: Int, c_name: String, c_nationkey: Int)
case class Record843(l_partkey: Int, l_orderkey: Int)
case class Record844(l_partkey: Int, c_name: String, c_nationkey: Int)
case class Record845(p_name: String, p_partkey: Int)
case class Record847(s_name: String, s_nationkey: Int)
case class Record849(c_name: String, c_nationkey: Int)
case class Record850(p_name: String, suppliers: Iterable[Record847], customers: Iterable[Record849])
object Query3Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query3Spark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L = tpch.loadLineitem
L.cache
L.count
val P = tpch.loadPart
P.cache
P.count
val C = tpch.loadCustomers
C.cache
C.count
val O = tpch.loadOrders
O.cache
O.count
val PS = tpch.loadPartSupp
PS.cache
PS.count
val S = tpch.loadSupplier
S.cache
S.count

   def f = { 
 val x727 = PS.map(x723 => { val x724 = x723.ps_partkey 
val x725 = x723.ps_suppkey 
val x726 = Record837(x724, x725) 
x726 }) 
val x733 = S.map(x728 => { val x729 = x728.s_name 
val x730 = x728.s_nationkey 
val x731 = x728.s_suppkey 
val x732 = Record838(x729, x730, x731) 
x732 }) 
val x738 = { val out1 = x727.map{ case x734 => ({val x736 = x734.ps_suppkey 
x736}, x734) }
  val out2 = x733.map{ case x735 => ({val x737 = x735.s_suppkey 
x737}, x735) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x745 = x738.map{ case (x739, x740) => 
   val x741 = x739.ps_partkey 
val x742 = x740.s_name 
val x743 = x740.s_nationkey 
val x744 = Record839(x741, x742, x743) 
x744 
} 
val partsuppliers = x745
val x746 = partsuppliers
//partsuppliers.collect.foreach(println(_))
val x751 = O.map(x747 => { val x748 = x747.o_orderkey 
val x749 = x747.o_custkey 
val x750 = Record840(x748, x749) 
x750 }) 
val x757 = C.map(x752 => { val x753 = x752.c_name 
val x754 = x752.c_nationkey 
val x755 = x752.c_custkey 
val x756 = Record841(x753, x754, x755) 
x756 }) 
val x762 = { val out1 = x751.map{ case x758 => ({val x760 = x758.o_custkey 
x760}, x758) }
  val out2 = x757.map{ case x759 => ({val x761 = x759.c_custkey 
x761}, x759) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x769 = x762.map{ case (x763, x764) => 
   val x765 = x763.o_orderkey 
val x766 = x764.c_name 
val x767 = x764.c_nationkey 
val x768 = Record842(x765, x766, x767) 
x768 
} 
val custorders = x769
val x770 = custorders
//custorders.collect.foreach(println(_))
val x772 = custorders 
val x777 = L.map(x773 => { val x774 = x773.l_partkey 
val x775 = x773.l_orderkey 
val x776 = Record843(x774, x775) 
x776 }) 
val x782 = { val out1 = x772.map{ case x778 => ({val x780 = x778.o_orderkey 
x780}, x778) }
  val out2 = x777.map{ case x779 => ({val x781 = x779.l_orderkey 
x781}, x779) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x789 = x782.map{ case (x783, x784) => 
   val x785 = x784.l_partkey 
val x786 = x783.c_name 
val x787 = x783.c_nationkey 
val x788 = Record844(x785, x786, x787) 
x788 
} 
val cparts = x789
val x790 = cparts
//cparts.collect.foreach(println(_))
val x795 = P.map(x791 => { val x792 = x791.p_name 
val x793 = x791.p_partkey 
val x794 = Record845(x792, x793) 
x794 }) 
val x797 = partsuppliers 
val x802 = { val out1 = x795.map{ case x798 => ({val x800 = x798.p_partkey 
x800}, x798) }
  val out2 = x797.map{ case x799 => ({val x801 = x799.ps_partkey 
x801}, x799) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x811 = x802.flatMap{ case (x803, x804) => val x810 = (x804) 
x810 match {
   case (null) => Nil 
   case x809 => List(({val x805 = (x803) 
x805}, {val x806 = x804.s_name 
val x807 = x804.s_nationkey 
val x808 = Record847(x806, x807) 
x808}))
 }
}.groupByKey() 
val x813 = cparts 
val x819 = { val out1 = x811.map{ case (x814, x815) => ({val x817 = x814.p_partkey 
x817}, (x814, x815)) }
  val out2 = x813.map{ case x816 => ({val x818 = x816.l_partkey 
x818}, x816) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x829 = x819.flatMap{ case ((x820, x821), x822) => val x828 = (x822) 
x828 match {
   case (null) => Nil 
   case x827 => List(({val x823 = (x820,x821) 
x823}, {val x824 = x822.c_name 
val x825 = x822.c_nationkey 
val x826 = Record849(x824, x825) 
x826}))
 }
}.groupByKey() 
val x835 = x829.map{ case ((x830, x831), x832) => 
   val x833 = x830.p_name 
val x834 = Record850(x833, x831, x832) 
x834 
} 
x835.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("Query3Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
