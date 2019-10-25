
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record323(lbl: Unit)
case class Record324(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record325(p_name: String, p_partkey: Int)
case class Record327(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record328(_1: Record323, _2: (Iterable[Record327]))
case class Record329(c_name: String, c_custkey: Int)
case class Record331(c__Fc_custkey: Int)
case class Record332(c_name: String, c_orders: Record331)
case class Record333(_1: Record323, _2: (Iterable[Record332]))
case class Record334(lbl: Record331)
case class Record335(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record337(o__Fo_orderkey: Int)
case class Record338(o_orderdate: String, o_parts: Record337)
case class Record339(_1: Record334, _2: (Iterable[Record338]))
case class Record340(lbl: Record337)
case class Record342(p_name: String, l_qty: Double)
case class Record343(_1: Record340, _2: (Iterable[Record342]))
object ShredQuery1Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1Spark"+sf)
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

   def f = { 
 val x165 = () 
val x166 = Record323(x165) 
val ljp__F = x166
val x167 = ljp__F
//ljp__F.collect.foreach(println(_))
val x168 = List(ljp__F) 
val x170 = x168 
val x171 = L__D_1 
val x177 = x171.map(x172 => { val x173 = x172.l_orderkey 
val x174 = x172.l_quantity 
val x175 = x172.l_partkey 
val x176 = Record324(x173, x174, x175) 
x176 }) 
val x180 = x177.map{ case c => (x170.head, c) } 
val x181 = P__D_1 
val x186 = x181.map(x182 => { val x183 = x182.p_name 
val x184 = x182.p_partkey 
val x185 = Record325(x183, x184) 
x185 }) 
val x192 = { val out1 = x180.map{ case (x187, x188) => ({val x190 = x188.l_partkey 
x190}, (x187, x188)) }
  val out2 = x186.map{ case x189 => ({val x191 = x189.p_partkey 
x191}, x189) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x203 = x192.map{ case ((x193, x194), x195) => val x202 = (x194,x195) 
x202 match {
   case (_,null) => ({val x196 = (x193) 
x196}, null) 
   case x201 => ({val x196 = (x193) 
x196}, {val x197 = x194.l_orderkey 
val x198 = x195.p_name 
val x199 = x194.l_quantity 
val x200 = Record327(x197, x198, x199) 
x200})
 }
}.groupByLabel() 
val x208 = x203.map{ case (x204, x205) => 
   val x206 = (x205) 
val x207 = Record328(x204, x206) 
x207 
} 
val ljp__D_1 = x208
val x209 = ljp__D_1
//ljp__D_1.collect.foreach(println(_))
val x210 = List(x166) 
val M_ctx1 = x210
val x211 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x213 = M_ctx1 
val x214 = C__D_1 
val x219 = x214.map(x215 => { val x216 = x215.c_name 
val x217 = x215.c_custkey 
val x218 = Record329(x216, x217) 
x218 }) 
val x222 = x219.map{ case c => (x213.head, c) } 
val x232 = x222.map{ case (x223, x224) => val x231 = (x224) 
x231 match {
   case (null) => ({val x225 = (x223) 
x225}, null) 
   case x230 => ({val x225 = (x223) 
x225}, {val x226 = x224.c_name 
val x227 = x224.c_custkey 
val x228 = Record331(x227) 
val x229 = Record332(x226, x228) 
x229})
 }
}.groupByLabel() 
val x237 = x232.map{ case (x233, x234) => 
   val x235 = (x234) 
val x236 = Record333(x233, x235) 
x236 
} 
val M_flat1 = x237
val x238 = M_flat1
//M_flat1.collect.foreach(println(_))
val x240 = M_flat1 
val x244 = x240.flatMap{ case x241 => x241 match {
   case null => List((x241, null))
   case _ =>
   val x242 = x241._2 
x242 match {
     case x243 => x243.map{ case v2 => (x241, v2) }
  }
 }} 
val x249 = x244.map{ case (x245, x246) => 
   val x247 = x246.c_orders 
val x248 = Record334(x247) 
x248 
} 
val x250 = x249.distinct 
val M_ctx2 = x250
val x251 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x253 = M_ctx2 
val x254 = O__D_1 
val x260 = x254.map(x255 => { val x256 = x255.o_orderdate 
val x257 = x255.o_orderkey 
val x258 = x255.o_custkey 
val x259 = Record335(x256, x257, x258) 
x259 }) 
val x266 = { val out1 = x253.map{ case x261 => ({val x263 = x261.lbl 
val x264 = x263.c__Fc_custkey 
x264}, x261) }
  val out2 = x260.map{ case x262 => ({val x265 = x262.o_custkey 
x265}, x262) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x276 = x266.map{ case (x267, x268) => val x275 = (x268) 
x275 match {
   case (null) => ({val x269 = (x267) 
x269}, null) 
   case x274 => ({val x269 = (x267) 
x269}, {val x270 = x268.o_orderdate 
val x271 = x268.o_orderkey 
val x272 = Record337(x271) 
val x273 = Record338(x270, x272) 
x273})
 }
}.groupByLabel() 
val x281 = x276.map{ case (x277, x278) => 
   val x279 = (x278) 
val x280 = Record339(x277, x279) 
x280 
} 
val M_flat2 = x281
val x282 = M_flat2
//M_flat2.collect.foreach(println(_))
val x284 = M_flat2 
val x288 = x284.flatMap{ case x285 => x285 match {
   case null => List((x285, null))
   case _ =>
   val x286 = x285._2 
x286 match {
     case x287 => x287.map{ case v2 => (x285, v2) }
  }
 }} 
val x293 = x288.map{ case (x289, x290) => 
   val x291 = x290.o_parts 
val x292 = Record340(x291) 
x292 
} 
val x294 = x293.distinct 
val M_ctx3 = x294
val x295 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x297 = M_ctx3 
val x298 = ljp__D_1 
val x300 = x298 
val x306 = { val out1 = x297.map{ case x301 => ({val x303 = x301.lbl 
val x304 = x303.o__Fo_orderkey 
x304}, x301) }
  val out2 = x300.map{ case x302 => ({val x305 = x302.l_orderkey 
x305}, x302) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x315 = x306.map{ case (x307, x308) => val x314 = (x308) 
x314 match {
   case (null) => ({val x309 = (x307) 
x309}, null) 
   case x313 => ({val x309 = (x307) 
x309}, {val x310 = x308.p_name 
val x311 = x308.l_qty 
val x312 = Record342(x310, x311) 
x312})
 }
}.groupByLabel() 
val x320 = x315.map{ case (x316, x317) => 
   val x318 = (x317) 
val x319 = Record343(x316, x318) 
x319 
} 
val M_flat3 = x320
val x321 = M_flat3
//M_flat3.collect.foreach(println(_))
x321.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis()
   println("ShredQuery1Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
