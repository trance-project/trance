
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record163(o_orderkey: Int, o_custkey: Int)
case class Record164(c_name: String, c_custkey: Int)
case class Record165(o_orderkey: Int, c_name: String)
case class Record166(s__Fs_suppkey: Int)
case class Record167(s_name: String, customers2: Record166)
case class Record168(l_orderkey: Int, l_suppkey: Int)
case class Record169(c_name2: String)
object ShredQuery2SparkOpt2WUS {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery2SparkOpt2WUS"+sf)
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
 val x118 = O__D_1.map(x114 => { val x115 = x114.o_orderkey 
val x116 = x114.o_custkey 
val x117 = Record163(x115, x116) 
x117 }) 
val x123 = C__D_1.map(x119 => { val x120 = x119.c_name 
val x121 = x119.c_custkey 
val x122 = Record164(x120, x121) 
x122 }) 
val x128 = { val out1 = x118.map{ case x124 => ({val x126 = x124.o_custkey 
x126}, x124) }
  val out2 = x123.map{ case x125 => ({val x127 = x125.c_custkey 
x127}, x125) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x134 = x128.map{ case (x129, x130) => 
   val x131 = x129.o_orderkey 
val x132 = x130.c_name 
val x133 = Record165(x131, x132) 
x133 
} 
val resultInner__D_1 = x134
val x135 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val x141 = S__D_1.map{ case x136 => 
   val x137 = x136.s_name 
val x138 = x136.s_suppkey 
val x139 = Record166(x138) 
val x140 = Record167(x137, x139) 
x140 
} 
val M_flat1 = x141
val x142 = M_flat1
M_flat1.count
//M_flat1.collect.foreach(println(_))
val x144 = resultInner__D_1 
val x148 = L__D_1.map(x145 => { val x146 = x145.l_orderkey 
val x147 = Record168(x146, x145.l_suppkey) 
x147 }) 
val x153 = { val out1 = x144.map{ case x149 => ({val x151 = x149.o_orderkey 
x151}, x149) }
  val out2 = x148.map{ case x150 => ({val x152 = x150.l_orderkey 
x152}, x150) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x160 = x153.map{ case (x154, x155) => 
   val x156 = x154.c_name 
val x157 = (Record166(x155.l_suppkey), Record169(x156))
x157 
}.groupByLabel()
val M_flat2 = x160
val x161 = M_flat2
//M_flat2.collect.foreach(println(_))
x161.count
var end0 = System.currentTimeMillis() - start0
println("ShredQuery2SparkOpt2WUS"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
var start1 = System.currentTimeMillis()
val firstlevel = M_flat1.map(m => (m.customers2, m.s_name)).cogroup(M_flat2).flatMap{
  case (_, (sn, custs)) => sn.map(s => (s, custs))
}
firstlevel.count
var end1 = System.currentTimeMillis() - start1
println("ShredQuery2SparkOpt2WUS"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)

}
var start = System.currentTimeMillis()
f 
var end = System.currentTimeMillis() - start
   println("ShredQuery2SparkOpt2WUS"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
