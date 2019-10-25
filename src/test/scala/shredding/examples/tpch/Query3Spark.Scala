
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record882(ps_partkey: Int, ps_suppkey: Int)
case class Record883(s_name: String, s_nationkey: Int, s_suppkey: Int)
case class Record884(ps_partkey: Int, s_name: String, s_nationkey: Int)
case class Record885(o_orderkey: Int, o_custkey: Int)
case class Record886(c_name: String, c_nationkey: Int, c_custkey: Int)
case class Record887(o_orderkey: Int, c_name: String, c_nationkey: Int)
case class Record888(l_partkey: Int, l_orderkey: Int)
case class Record889(l_partkey: Int, c_name: String, c_nationkey: Int)
case class Record890(p_name: String, p_partkey: Int)
case class Record892(s_name: String, s_nationkey: Int)
case class Record894(c_name: String, c_nationkey: Int)
case class Record895(p_name: String, suppliers: Iterable[Record892], customers: Iterable[Record894])
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
 val x772 = PS.map(x768 => { val x769 = x768.ps_partkey 
val x770 = x768.ps_suppkey 
val x771 = Record882(x769, x770) 
x771 }) 
val x778 = S.map(x773 => { val x774 = x773.s_name 
val x775 = x773.s_nationkey 
val x776 = x773.s_suppkey 
val x777 = Record883(x774, x775, x776) 
x777 }) 
val x783 = { val out1 = x772.map{ case x779 => ({val x781 = x779.ps_suppkey 
x781}, x779) }
  val out2 = x778.map{ case x780 => ({val x782 = x780.s_suppkey 
x782}, x780) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x790 = x783.map{ case (x784, x785) => 
   val x786 = x784.ps_partkey 
val x787 = x785.s_name 
val x788 = x785.s_nationkey 
val x789 = Record884(x786, x787, x788) 
x789 
} 
val partsuppliers = x790
val x791 = partsuppliers
//partsuppliers.collect.foreach(println(_))
val x796 = O.map(x792 => { val x793 = x792.o_orderkey 
val x794 = x792.o_custkey 
val x795 = Record885(x793, x794) 
x795 }) 
val x802 = C.map(x797 => { val x798 = x797.c_name 
val x799 = x797.c_nationkey 
val x800 = x797.c_custkey 
val x801 = Record886(x798, x799, x800) 
x801 }) 
val x807 = { val out1 = x796.map{ case x803 => ({val x805 = x803.o_custkey 
x805}, x803) }
  val out2 = x802.map{ case x804 => ({val x806 = x804.c_custkey 
x806}, x804) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x814 = x807.map{ case (x808, x809) => 
   val x810 = x808.o_orderkey 
val x811 = x809.c_name 
val x812 = x809.c_nationkey 
val x813 = Record887(x810, x811, x812) 
x813 
} 
val custorders = x814
val x815 = custorders
//custorders.collect.foreach(println(_))
val x817 = custorders 
val x822 = L.map(x818 => { val x819 = x818.l_partkey 
val x820 = x818.l_orderkey 
val x821 = Record888(x819, x820) 
x821 }) 
val x827 = { val out1 = x817.map{ case x823 => ({val x825 = x823.o_orderkey 
x825}, x823) }
  val out2 = x822.map{ case x824 => ({val x826 = x824.l_orderkey 
x826}, x824) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x834 = x827.map{ case (x828, x829) => 
   val x830 = x829.l_partkey 
val x831 = x828.c_name 
val x832 = x828.c_nationkey 
val x833 = Record889(x830, x831, x832) 
x833 
} 
val cparts = x834
val x835 = cparts
//cparts.collect.foreach(println(_))
val x840 = P.map(x836 => { val x837 = x836.p_name 
val x838 = x836.p_partkey 
val x839 = Record890(x837, x838) 
x839 }) 
val x842 = partsuppliers 
val x847 = { val out1 = x840.map{ case x843 => ({val x845 = x843.p_partkey 
x845}, x843) }
  val out2 = x842.map{ case x844 => ({val x846 = x844.ps_partkey 
x846}, x844) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x856 = x847.map{ case (x848, x849) => val x855 = (x849) 
x855 match {
   case (null) => ({val x850 = (x848) 
x850}, null) 
   case x854 => ({val x850 = (x848) 
x850}, {val x851 = x849.s_name 
val x852 = x849.s_nationkey 
val x853 = Record892(x851, x852) 
x853})
 }
}.groupByKey() 
val x858 = cparts 
val x864 = { val out1 = x856.map{ case (x859, x860) => ({val x862 = x859.p_partkey 
x862}, (x859, x860)) }
  val out2 = x858.map{ case x861 => ({val x863 = x861.l_partkey 
x863}, x861) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x874 = x864.map{ case ((x865, x866), x867) => val x873 = (x867) 
x873 match {
   case (null) => ({val x868 = (x865,x866) 
x868}, null) 
   case x872 => ({val x868 = (x865,x866) 
x868}, {val x869 = x867.c_name 
val x870 = x867.c_nationkey 
val x871 = Record894(x869, x870) 
x871})
 }
}.groupByKey() 
val x880 = x874.map{ case ((x875, x876), x877) => 
   val x878 = x875.p_name 
val x879 = Record895(x878, x876, x877) 
x879 
} 
x880.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis()
   println("Query3Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
