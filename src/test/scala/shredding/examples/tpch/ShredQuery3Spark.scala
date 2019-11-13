
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record1031(lbl: Unit)
case class Record1032(ps_partkey: Int, ps_suppkey: Int)
case class Record1033(s_name: String, s_nationkey: Int, s_suppkey: Int)
case class Record1034(ps_partkey: Int, s_name: String, s_nationkey: Int)
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
 val x873 = () 
val x874 = Record1031(x873) 
val x875 = List(x874) 
val M_ctx1 = x875
val x876 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x881 = PS__D.map(x877 => { val x878 = x877.ps_partkey 
val x879 = x877.ps_suppkey 
val x880 = Record1032(x878, x879) 
x880 }) 
val x882 = S__D_1 
val x888 = x882.map(x883 => { val x884 = x883.s_name 
val x885 = x883.s_nationkey 
val x886 = x883.s_suppkey 
val x887 = Record1033(x884, x885, x886) 
x887 }) 
val x893 = { val out1 = x881.map{ case x889 => ({val x891 = x889.ps_suppkey 
x891}, x889) }
  val out2 = x888.map{ case x890 => ({val x892 = x852.s_suppkey 
x892}, x890) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x900 = x893.map{ case (x894, x895) => 
   val x896 = x894.ps_partkey 
val x897 = x852.s_name 
val x898 = x852.s_nationkey 
val x899 = Record1034(x896, x897, x898) 
x899 
} 
val M_flat1 = x900
val x901 = M_flat1
//M_flat1.collect.foreach(println(_))
x901.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery3Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
