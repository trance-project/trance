
package experiments
/** Generated - manually editted to look at some unshredding values **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record182(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record183(p_name: String, p_partkey: Int)
case class Record184(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record185(c__Fc_custkey: Int)
case class Record186(c_name: String, c_orders: Record185)
case class Record187(o__Fo_orderkey: Int)
case class Record188(o_orderdate: String, o_parts: Record187)
case class Record189(p_name: String, l_qty: Double)
object ShredQuery1SparkOptCart {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1SparkOptCart"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L__F = 3
val L__D_1 = tpch.loadLineitemProj
L__D_1.cache
spark.sparkContext.runJob(L__D_1, (iter: Iterator[_]) => {})
val P__F = 4
val P__D_1 = tpch.loadPartProj
P__D_1.cache
spark.sparkContext.runJob(P__D_1, (iter: Iterator[_]) => {})
val C__F = 1
val C__D_1 = tpch.loadCustomersProj
C__D_1.cache
spark.sparkContext.runJob(C__D_1, (iter: Iterator[_]) => {})
val O__F = 2
val O__D_1 = tpch.loadOrdersProj
O__D_1.cache
spark.sparkContext.runJob(O__D_1, (iter: Iterator[_]) => {})

tpch.triggerGC

   def f = { 
var start0 = System.currentTimeMillis()
 val x134 = L__D_1/**.map(x129 => { val x130 = x129.l_orderkey 
val x131 = x129.l_quantity 
val x132 = x129.l_partkey 
val x133 = Record182(x130, x131, x132) 
x133 }) **/
val x139 = P__D_1/**.map(x135 => { val x136 = x135.p_name 
val x137 = x135.p_partkey 
val x138 = Record183(x136, x137) 
x138 }) **/
val x144_out1 = x134.map{ case x140 => ({val x142 = x140.l_partkey 
x142}, x140) }
val x144_out2 = x139.map{ case x141 => ({val x143 = x141.p_partkey 
x143}, x141) }
val heavykeys = x144_out1.heavyKeys()
val hkeys = x144_out1.sparkContext.broadcast(heavykeys)
val (llight, rlight) = x144_out1.filterLight(x144_out2, hkeys)
val light = llight.joinDropKey(rlight)
val (lheavy, rheavy) = x144_out1.filterHeavy(x144_out2, hkeys)
val heavy = lheavy.cartesian(rheavy).map{
  case ((_, v1), (_, v2)) => (v1, v2)
}
val x144 = heavy union light
//val x144 = x144_out1.joinSkewLeft(x144_out2)

val x151 = x144.map{ case (x145, x146) => 
   val x147 = x145.l_orderkey 
val x148 = x146.p_name 
val x149 = x145.l_quantity 
val x150 = Record184(x147, x148, x149) 
x150 
} 
val ljp__D_1 = x151
val x152 = ljp__D_1
//ljp__D_1.collect.foreach(println(_))
val x158 = C__D_1.map{ case x153 => 
   val x154 = x153.c_name 
val x155 = x153.c_custkey 
val x156 = Record185(x155) 
val x157 = Record186(x154, x156) 
x157 
} 
val M_flat1 = x158
val x159 = M_flat1
spark.sparkContext.runJob(M_flat1, (iter: Iterator[_]) => {})
val x169 = O__D_1.map{ case x160 => 
   val x161 = x160.o_custkey 
val x162 = Record185(x161) 
val x163 = x160.o_orderdate 
val x164 = x160.o_orderkey 
val x165 = Record187(x164) 
val x166 = Record188(x163, x165) 
val x167 = x166//List(x166) 
val x168 = (x162, x167) 
x168 
}.groupBySkew() 
val M_flat2 = x169
val x170 = M_flat2
spark.sparkContext.runJob(M_flat2, (iter: Iterator[_]) => {})
val x179 = ljp__D_1.map{ case x171 => 
   val x172 = x171.l_orderkey 
val x173 = Record187(x172) 
val x174 = x171.p_name 
val x175 = x171.l_qty 
val x176 = Record189(x174, x175) 
val x177 = x176//List(x176) 
val x178 = (x173, x177) 
x178 
}.groupByLabel() 
val M_flat3 = x179
val x180 = M_flat3
//M_flat3.collect.foreach(println(_))
spark.sparkContext.runJob(M_flat3, (iter: Iterator[_]) => {})
var end0 = System.currentTimeMillis() - start0
println("ShredQuery1SparkOptCart"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)

var start1 = System.currentTimeMillis()
/**val top = M_flat1.map{ c => c.c_orders -> c.c_name }
val x207 = M_flat2.flatMap{
  case (lbl, bag) => bag.map( d => d.o_parts -> (d.o_orderdate, lbl) )
}.cogroup(M_flat3).flatMap{
  case (_, (dates, parts)) => dates.map{ case (date, lbl) => lbl -> (date, parts)}
}.cogroup(top).flatMap{
  case (_, (dates, names)) => names.map(n => (n, dates))
}
spark.sparkContext.runJob(x207,(iter: Iterator[_]) => {})
//x207.collect.foreach(println(_))**/
var end1 = System.currentTimeMillis() - start1
println("ShredQuery1SparkOptCart"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)

}
f
 }
}
