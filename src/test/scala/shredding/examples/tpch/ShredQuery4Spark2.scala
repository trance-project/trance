
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record894(lbl: Unit)
case class Record895(c_name: String, c_custkey: Int)
case class Record897(c__Fc_custkey: Int)
case class Record898(c_name: String, c_orders: Record897)
case class Record899(_1: Record894, _2: (Iterable[Record898]))
case class Record900(lbl: Record897)
case class Record901(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record903(o__Fo_orderkey: Int)
case class Record904(o_orderdate: String, o_parts: Record903)
case class Record905(_1: Record900, _2: (Iterable[Record904]))
case class Record906(lbl: Record903)
case class Record907(l_quantity: Double, l_partkey: Int, l_orderkey: Int)
case class Record908(p_name: String, p_partkey: Int)
case class Record910(p_name: String, l_qty: Double)
case class Record911(_1: Record906, _2: (Iterable[Record910]))
case class Record977(c_name: String, p_name: String, month: String)
object ShredQuery4Spark2 {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4Spark2"+sf)
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

   
   val x762 = ()
val x763 = Record894(x762)
val x764 = List(x763)
val M_ctx1 = x764
val x767 = M_ctx1
val x768 = C__D_1
val x773 = x768.map(x769 => { val x770 = x769.c_name
val x771 = x769.c_custkey
val x772 = Record895(x770, x771)
x772 })
val x776 = x773.map{ case c => (x767.head, c) }
val x786 = x776.map{ case (x777, x778) => val x785 = (x778)
x785 match {
   case (null) => ({val x779 = (x777)
x779}, null)
   case x784 => ({val x779 = (x777)
x779}, {val x780 = x778.c_name
val x781 = x778.c_custkey
val x782 = Record897(x781)
val x783 = Record898(x780, x782)
x783})
 }
}.groupByLabel()
val x791 = x786.map{ case (x787, x788) =>
   val x789 = (x788)
val x790 = Record899(x787, x789)
x790
}
val M_flat1 = x791
//println("M_flat1")
val x792 = M_flat1
//M_flat1.collect.foreach(println(_))
val x794 = M_flat1
val x798 = x794.flatMap{ case x795 => x795 match {
   case null => List((x795, null))
   case _ =>
   val x796 = x795._2
x796 match {
     case x797 => x797.map{ case v2 => (x795, v2) }
  }
 }}
val x803 = x798.map{ case (x799, x800) =>
   val x801 = x800.c_orders
val x802 = Record900(x801)
x802
}
val x804 = x803.distinct
val M_ctx2 = x804
//println("M_ctx2")
val x805 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x807 = M_ctx2
val x808 = O__D_1
val x814 = x808.map(x809 => { val x810 = x809.o_orderdate
val x811 = x809.o_orderkey
val x812 = x809.o_custkey
val x813 = Record901(x810, x811, x812)
x813 })
val x820 = { val out1 = x807.map{ case x815 => ({val x817 = x815.lbl
val x818 = x817.c__Fc_custkey
x818}, x815) }
  val out2 = x814.map{ case x816 => ({val x819 = x816.o_custkey
x819}, x816) }
  out1.join(out2).map{ case (k,v) => v }
}
val x830 = x820.map{ case (x821, x822) => val x829 = (x822)
x829 match {
   case (null) => ({val x823 = (x821)
x823}, null)
   case x828 => ({val x823 = (x821)
x823}, {val x824 = x822.o_orderdate
val x825 = x822.o_orderkey
val x826 = Record903(x825)
val x827 = Record904(x824, x826)
x827})
 }
}.groupByLabel()
val x835 = x830.map{ case (x831, x832) =>
   val x833 = (x832)
val x834 = Record905(x831, x833)
x834
}
val M_flat2 = x835
//println("M_flat2")
val x836 = M_flat2
//M_flat2.collect.foreach(println(_))
val x838 = M_flat2
val x842 = x838.flatMap{ case x839 => x839 match {
   case null => List((x839, null))
   case _ =>
   val x840 = x839._2
x840 match {
     case x841 => x841.map{ case v2 => (x839, v2) }
  }
 }}
val x847 = x842.map{ case (x843, x844) =>
   val x845 = x844.o_parts
val x846 = Record906(x845)
x846
}
val x848 = x847.distinct
val M_ctx3 = x848
//println("M_ctx3")
val x849 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x851 = M_ctx3
val x852 = L__D_1
val x858 = x852.map(x853 => { val x854 = x853.l_quantity
val x855 = x853.l_partkey
val x856 = x853.l_orderkey
val x857 = Record907(x854, x855, x856)
x857 })
val x864 = { val out1 = x851.map{ case x859 => ({val x861 = x859.lbl
val x862 = x861.o__Fo_orderkey
x862}, x859) }
  val out2 = x858.map{ case x860 => ({val x863 = x860.l_orderkey
x863}, x860) }
  out1.join(out2).map{ case (k,v) => v }
}
val x865 = P__D_1
val x870 = x865.map(x866 => { val x867 = x866.p_name
val x868 = x866.p_partkey
val x869 = Record908(x867, x868)
x869 })
val x876 = { val out1 = x864.map{ case (x871, x872) => ({val x874 = x872.l_partkey
x874}, (x871, x872)) }
  val out2 = x870.map{ case x873 => ({val x875 = x873.p_partkey
x875}, x873) }
  out1.join(out2).map{ case (k,v) => v }
}
val x886 = x876.map{ case ((x877, x878), x879) => val x885 = (x878,x879)
x885 match {
   case (_,null) => ({val x880 = (x877)
x880}, null)
   case x884 => ({val x880 = (x877)
x880}, {val x881 = x879.p_name
val x882 = x878.l_quantity
val x883 = Record910(x881, x882)
x883})
 }
}.groupByLabel()
val x891 = x886.map{ case (x887, x888) =>
   val x889 = (x888)
val x890 = Record911(x887, x889)
x890
}
val M_flat3 = x891
//println("M_flat3")
val x892 = M_flat3
//M_flat3.collect.foreach(println(_))
val res = x892

val Query4__F = M_ctx1
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
val x944 = Query4__D_2c_orders_2o_parts 
val x946 = x944 
val x948 = x946 
val out2 = x948.flatMap(x952 => x952._2.map{case v2 => (x952._1.lbl, v2)})
val x967 = out2.map{ 
  case (a, null) => (a, 0.0) 
  case (x957, x958) =>
    ({val x960 = x958.p_name
      val x961 = x957
      val x963 = (x961,x960)
     x963}, {val x964 = x958.l_qty
     x964})
}.reduceByKey(_+_)
val M_flat1 = x967

val x974 = M_flat1
M_flat1.collect.foreach(println(_))
val res = x974.count
  }
   f
   var end0 = System.currentTimeMillis() - start0
   println("ShredQuery4Spark2"+sf+","+Config.datapath+","+end0)
 }
}
