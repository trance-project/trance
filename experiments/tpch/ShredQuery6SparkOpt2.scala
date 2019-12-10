
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record226(o_orderkey: Int, o_custkey: Int)
case class Record227(c_name: String, c_custkey: Int)
case class Record228(o_orderkey: Int, c_name: String)
case class Record229(s__Fs_suppkey: Int)
case class Record230(s_name: String, customers2: Record229)
case class Record231(l_suppkey: Int, l_orderkey: Int)
case class Record233(c_name2: String)
case class Record274(c_name: String, s_name: String)
case class Record275(c__Fc_name: String)
case class Record276(c_name: String, suppliers: Record275)
case class Record277(s_name: String)
object ShredQuery6SparkOpt2 {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6SparkOpt2"+sf)
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

   val x173 = O__D_1.map(x169 => { val x170 = x169.o_orderkey 
val x171 = x169.o_custkey 
val x172 = Record226(x170, x171) 
x172 }) 
val x178 = C__D_1.map(x174 => { val x175 = x174.c_name 
val x176 = x174.c_custkey 
val x177 = Record227(x175, x176) 
x177 }) 
val x183 = { val out1 = x173.map{ case x179 => ({val x181 = x179.o_custkey 
x181}, x179) }
  val out2 = x178.map{ case x180 => ({val x182 = x180.c_custkey 
x182}, x180) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x189 = x183.map{ case (x184, x185) => 
   val x186 = x184.o_orderkey 
val x187 = x185.c_name 
val x188 = Record228(x186, x187) 
x188 
} 
val resultInner__D_1 = x189
val x190 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val x196 = S__D_1.map{ case x191 => 
   val x192 = x191.s_name 
val x193 = x191.s_suppkey 
val x194 = Record229(x193) 
val x195 = Record230(x192, x194) 
x195 
} 
val M_flat1 = x196
val x197 = M_flat1
//M_flat1.collect.foreach(println(_))
val x202 = L__D_1.map(x198 => { val x199 = x198.l_suppkey 
val x200 = x198.l_orderkey 
val x201 = Record231(x199, x200) 
x201 }) 
val x204 = resultInner__D_1 
val x209 = { val out1 = x202.map{ case x205 => ({val x207 = x205.l_orderkey 
x207}, x205) }
  val out2 = x204.map{ case x206 => ({val x208 = x206.o_orderkey 
x208}, x206) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x217 = x209.flatMap{ case (x210, x211) => val x216 = (x211) 
x216 match {
   case (null) => Nil 
   case x215 => List(({val x212 = (x210) 
x212}, {val x213 = x211.c_name 
val x214 = Record233(x213) 
x214}))
 }
}.groupByLabel() 
val x223 = x217.map{ case (x218, x219) => 
   val x220 = x218.l_suppkey 
val x221 = Record229(x220) 
val x222 = (x221, x219) 
x222 
} 
val M_flat2 = x223
val x224 = M_flat2
//M_flat2.collect.foreach(println(_))
val Query2__D_1 = M_flat1
Query2__D_1.cache
Query2__D_1.count
val Query2__D_2customers2_1 = M_flat2
Query2__D_2customers2_1.cache
Query2__D_2customers2_1.count
def f = { 
 val x246 = Query2__D_1 
val x250 = { val out1 = x246.map{ case x247 => ({val x249 = x247.customers2 
x249}, x247.s_name) }
  val out2 = Query2__D_2customers2_1.flatMapValues(identity)
  out2.lookupSkewLeft(out1)
} 
val x256 = x250.map{ case (x252, x251) => 
   val x253 = x252.c_name2 
val x254 = x251//.s_name 
val x255 = Record274(x253, x254) 
x255 
} 
val cflat__D_1 = x256
val x257 = cflat__D_1
cflat__D_1.collect.foreach(println(_))
val x262 = C__D_1.map{ case x258 => 
   val x259 = x258.c_name 
val x260 = Record275(x259) 
val x261 = Record276(x259, x260) 
x261 
} 
val M_flat1 = x262
val x263 = M_flat1
M_flat1.count
//M_flat1.collect.foreach(println(_))
val x271 = cflat__D_1.map{ case x264 => 
   val x265 = x264.c_name 
val x266 = Record275(x265) 
val x267 = x264.s_name 
val x268 = Record277(x267) 
val x269 = List(x268) 
val x270 = (x266, x269) 
x270 
}.groupByLabel() 
val M_flat2 = x271
val x272 = M_flat2
//M_flat2.collect.foreach(println(_))
x272.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery6SparkOpt2"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
