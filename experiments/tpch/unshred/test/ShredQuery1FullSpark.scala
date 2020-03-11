
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.PairRDDOperations._
import sprkloader.DictRDDOperations._
import sprkloader.TopRDD._
case class Record223(c__Fc_custkey: Int)
case class Record224(c_name: String, c_orders: Record223)
case class Record225(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record228(o__Fo_orderkey: Int)
case class Record229(o_orderdate: String, o_parts: Record228)
case class Record230(l_partkey: Int, l_quantity: Double, l_orderkey: Int)
case class Record233(l_partkey: Int, l_qty: Double)
case class Record286(o_orderdate: String)
case class Record287(o_orderdate: String, o_parts: Vector[Record233])
case class Record289(c_name: String)
case class Record290(c_name: String, c_orders: Vector[Record287])
object ShredQuery1FullSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1FullSpark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L__D_1 = tpch.loadLineitem()
L__D_1.cache
spark.sparkContext.runJob(L__D_1, (iter: Iterator[_]) => {})
val P__D_1 = tpch.loadPart()
P__D_1.cache
spark.sparkContext.runJob(P__D_1, (iter: Iterator[_]) => {})
val C__D_1 = tpch.loadCustomer()
C__D_1.cache
spark.sparkContext.runJob(C__D_1, (iter: Iterator[_]) => {})
val O__D_1 = tpch.loadOrder()
O__D_1.cache
spark.sparkContext.runJob(O__D_1, (iter: Iterator[_]) => {})

    def f = {
 
var start0 = System.currentTimeMillis()
val x173 = C__D_1.map{ case x168 => 
   val x169 = x168.c_name 
val x170 = x168.c_custkey 
val x171 = Record223(x170) 
val x172 = Record224(x169, x171) 
x172
} 
val M__D_1 = x173
val x174 = M__D_1
//M__D_1.cache
//M__D_1.print
//M__D_1.evaluate
val x178 = M__D_1.createDomain(l => l.c_orders) 
val M_ctx2 = x178
val x179 = M_ctx2

val x181 = M_ctx2 
val x187 = O__D_1.map{ case x182 => 
   val x183 = x182.o_orderdate 
val x184 = x182.o_orderkey 
val x185 = x182.o_custkey 
val x186 = Record225(x183, x184, x185) 
x186
} 
 val x226 = x181
 val x227 = x187.map{ case x189 => (Record223({val x192 = x189.o_custkey 
x192}), {val x193 = x189.o_orderdate 
val x194 = x189.o_orderkey 
val x195 = Record228(x194) 
val x196 = Record229(x193, x195) 
x196}) }
 val x197 = x227.cogroupDomain(x226)
 val M__D_2 = x197
val x198 = M__D_2
//M__D_2.cache
//M__D_2.print
//M__D_2.evaluate
val x202 = M__D_2.createDomain(l => l.o_parts) 
val M_ctx3 = x202
val x203 = M_ctx3

val x205 = M_ctx3 
val x211 = L__D_1.map{ case x206 => 
   val x207 = x206.l_partkey 
val x208 = x206.l_quantity 
val x209 = x206.l_orderkey 
val x210 = Record230(x207, x208, x209) 
x210
} 
 val x231 = x205
 val x232 = x211.map{ case x213 => (Record228({val x216 = x213.l_orderkey 
x216}), {val x217 = x213.l_partkey 
val x218 = x213.l_quantity 
val x219 = Record233(x217, x218) 
x219}) }
 val x220 = x232.cogroupDomain(x231)
 val M__D_3 = x220
val x221 = M__D_3
//M__D_3.cache
//M__D_3.print
M__D_3.evaluate
        
        
var end0 = System.currentTimeMillis() - start0
println("ShredQuery1FullSpark,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    

var start1 = System.currentTimeMillis()
val x255 = M__D_2 
val x259 = x255.flatMap{
 case x256 => {val x257 = x256._2 
x257}.map{case x258 => 
   (x258.o_parts, (x256._1, Record286(x258.o_orderdate)))}
}
val x264 = M__D_3.rightCoGroupDropKey(x259)
val x271 = x264.map{ case ((x265, x266), x267) => 
   val x268 = x266.o_orderdate 
val x269 = Record287(x268, x267) 
val x270 = (x265, x269) 
x270
} 
val newM__D_2 = x271
val x272 = newM__D_2
//newM__D_2.cache
//newM__D_2.print
//newM__D_2.evaluate
val x274 = M__D_1 
val x288 = x274.map{ case x275 => (x275.c_orders, Record289(x275.c_name))}
val x278 = x288.cogroupDropKey(newM__D_2)
val x283 = x278.map{ case (x279, x280) => 
   val x281 = x279.c_name 
val x282 = Record290(x281, x280) 
x282
} 
val newM__D_1 = x283
val x284 = newM__D_1
//newM__D_1.cache
//newM__D_1.print
newM__D_1.evaluate


var end1 = System.currentTimeMillis() - start1
println("ShredQuery1FullSpark,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("ShredQuery1FullSpark"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
