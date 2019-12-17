
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record164(l_suppkey: Int, l_orderkey: Int)
case class Record165(o_custkey: Int, o_orderkey: Int)
case class Record166(c_name: String, c_custkey: Int)
case class Record167(l_suppkey: Int, c_name: String)
case class Record168(s__Fs_suppkey: Int)
case class Record169(s_name: String, customers2: Record168)
case class Record170(c_name2: String)
object ShredQuery2SparkOpt {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery2SparkOpt"+sf)
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

   def f = { 
var start0 = System.currentTimeMillis()
 val x117 = L__D_1.map(x113 => { val x114 = x113.l_suppkey 
val x115 = x113.l_orderkey 
val x116 = Record164(x114, x115) 
x116 }) 
val x122 = O__D_1.map(x118 => { val x119 = x118.o_custkey 
val x120 = x118.o_orderkey 
val x121 = Record165(x119, x120) 
x121 }) 
val x127 = { val out1 = x117.map{ case x123 => ({val x125 = x123.l_orderkey 
x125}, x123) }
  val out2 = x122.map{ case x124 => ({val x126 = x124.o_orderkey 
x126}, x124) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x132 = C__D_1.map(x128 => { val x129 = x128.c_name 
val x130 = x128.c_custkey 
val x131 = Record166(x129, x130) 
x131 }) 
val x138 = { val out1 = x127.map{ case (x133, x134) => ({val x136 = x134.o_custkey 
x136}, (x133, x134)) }
  val out2 = x132.map{ case x135 => ({val x137 = x135.c_custkey 
x137}, x135) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x145 = x138.map{ case ((x139, x140), x141) => 
   val x142 = x139.l_suppkey 
val x143 = x141.c_name 
val x144 = Record167(x142, x143) 
x144 
} 
val resultInner__D_1 = x145
val x146 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val x152 = S__D_1.map{ case x147 => 
   val x148 = x147.s_name 
val x149 = x147.s_suppkey 
val x150 = Record168(x149) 
val x151 = Record169(x148, x150) 
x151 
} 
val M_flat1 = x152
val x153 = M_flat1
M_flat1.count
//M_flat1.collect.foreach(println(_))
val x161 = resultInner__D_1.map{ case x154 => 
   val x155 = x154.l_suppkey 
val x156 = Record168(x155) 
val x157 = x154.c_name 
val x158 = Record170(x157) 
val x159 = x158//List(x158) 
val x160 = (x156, x159) 
x160 
}.groupByLabel() 
val M_flat2 = x161
val x162 = M_flat2
//M_flat2.collect.foreach(println(_))
//x162.count
M_flat2.count
var end0 = System.currentTimeMillis() - start0 
println("ShredQuery2SparkOpt"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)

var start1 = System.currentTimeMillis()
val unshred = M_flat1.map{m => (m.customers2) -> m.s_name }.join(M_flat2).map{ case (k,v) => v }
unshred.count
var end1 = System.currentTimeMillis() - start1 
println("ShredQuery2SparkOpt"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)

}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start 
   println("ShredQuery2SparkOpt"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
