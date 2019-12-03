
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record267(lbl: Unit)
case class Record268(l_suppkey: Int, l_orderkey: Int)
case class Record269(o_custkey: Int, o_orderkey: Int)
case class Record270(c_name: String, c_custkey: Int)
case class Record271(l_suppkey: Int, c_name: String)
case class Record272(s__Fs_suppkey: Int)
case class Record273(s_name: String, customers2: Record272)
case class Record274(lbl: Record272)
case class Record276(c_name2: String)
case class Record358(c_name: String, s_name: String)
case class Record359(c__Fc_name: String)
case class Record360(c_name: String, suppliers: Record359)
case class Record361(lbl: Record359)
case class Record363(s_name: String)
object ShredQuery6SparkDomains {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6SparkDomains"+sf)
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

   val x189 = () 
val x190 = Record267(x189) 
val x191 = List(x190) 
val M_ctx1 = x191
val x192 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x197 = L__D_1.map(x193 => { val x194 = x193.l_suppkey 
val x195 = x193.l_orderkey 
val x196 = Record268(x194, x195) 
x196 }) 
val x202 = O__D_1.map(x198 => { val x199 = x198.o_custkey 
val x200 = x198.o_orderkey 
val x201 = Record269(x199, x200) 
x201 }) 
val x207 = { val out1 = x197.map{ case x203 => ({val x205 = x203.l_orderkey 
x205}, x203) }
  val out2 = x202.map{ case x204 => ({val x206 = x204.o_orderkey 
x206}, x204) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x212 = C__D_1.map(x208 => { val x209 = x208.c_name 
val x210 = x208.c_custkey 
val x211 = Record270(x209, x210) 
x211 }) 
val x218 = { val out1 = x207.map{ case (x213, x214) => ({val x216 = x214.o_custkey 
x216}, (x213, x214)) }
  val out2 = x212.map{ case x215 => ({val x217 = x215.c_custkey 
x217}, x215) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x225 = x218.map{ case ((x219, x220), x221) => 
   val x222 = x219.l_suppkey 
val x223 = x221.c_name 
val x224 = Record271(x222, x223) 
x224 
} 
val resultInner__D_1 = x225
val x226 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val x232 = S__D_1.map{ case x227 => 
   val x228 = x227.s_name 
val x229 = x227.s_suppkey 
val x230 = Record272(x229) 
val x231 = Record273(x228, x230) 
x231 
} 
val M_flat1 = x232
val x233 = M_flat1
//M_flat1.collect.foreach(println(_))
val x235 = M_flat1 
val x239 = x235.map{ case x236 => 
   val x237 = x236.customers2 
val x238 = Record274(x237) 
x238 
} 
val x240 = x239.distinct 
val M_ctx2 = x240
val x241 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x243 = M_ctx2 
val x245 = resultInner__D_1 
val x251 = { val out1 = x243.map{ case x246 => ({val x248 = x246.lbl 
val x249 = x248.s__Fs_suppkey 
x249}, x246) }
  val out2 = x245.map{ case x247 => ({val x250 = x247.l_suppkey 
x250}, x247) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x259 = x251.flatMap{ case (x252, x253) => val x258 = (x253) 
x258 match {
   case (null) => Nil 
   case x257 => List(({val x254 = (x252) 
x254}, {val x255 = x253.c_name 
val x256 = Record276(x255) 
x256}))
 }
}.groupByLabel() 
val x264 = x259.map{ case (x260, x261) => 
   val x262 = x260.lbl 
val x263 = (x262, x261) 
x263 
} 
val M_flat2 = x264
val x265 = M_flat2
//M_flat2.collect.foreach(println(_))
val Query2__D_1 = M_flat1
Query2__D_1.cache
Query2__D_1.count
val Query2__D_2customers2_1 = M_flat2
Query2__D_2customers2_1.cache
Query2__D_2customers2_1.count
def f = { 
 val x302 = () 
val x303 = Record267(x302) 
val x304 = List(x303) 
val M_ctx1 = x304
val x305 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x307 = Query2__D_1 
val x311 = { val out1 = x307.map{ case x308 => ({val x310 = x308.customers2 
x310}, x308) }
  val out2 = Query2__D_2customers2_1.flatMapValues(identity)
  out2.lookupSkewLeft(out1)
} 
val x317 = x311.map{ case (x313, x312) => 
   val x314 = x313.c_name2 
val x315 = x312.s_name 
val x316 = Record358(x314, x315) 
x316 
} 
val cflat__D_1 = x317
val x318 = cflat__D_1
//cflat__D_1.collect.foreach(println(_))
val x323 = C__D_1.map{ case x319 => 
   val x320 = x319.c_name 
val x321 = Record359(x320) 
val x322 = Record360(x320, x321) 
x322 
} 
val M_flat1 = x323
val x324 = M_flat1
//M_flat1.collect.foreach(println(_))
val x326 = M_flat1 
val x330 = x326.map{ case x327 => 
   val x328 = x327.suppliers 
val x329 = Record361(x328) 
x329 
} 
val x331 = x330.distinct 
val M_ctx2 = x331
val x332 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x334 = M_ctx2 
val x336 = cflat__D_1 
val x342 = { val out1 = x334.map{ case x337 => ({val x339 = x337.lbl 
val x340 = x339.c__Fc_name 
x340}, x337) }
  val out2 = x336.map{ case x338 => ({val x341 = x338.c_name 
x341}, x338) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x350 = x342.flatMap{ case (x343, x344) => val x349 = (x344) 
x349 match {
   case (null) => Nil 
   case x348 => List(({val x345 = (x343) 
x345}, {val x346 = x344.s_name 
val x347 = Record363(x346) 
x347}))
 }
}.groupByLabel() 
val x355 = x350.map{ case (x351, x352) => 
   val x353 = x351.lbl 
val x354 = (x353, x352) 
x354 
} 
val M_flat2 = x355
val x356 = M_flat2
//M_flat2.collect.foreach(println(_))
x356.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery6SparkDomains"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
