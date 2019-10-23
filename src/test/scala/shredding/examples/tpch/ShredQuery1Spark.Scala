
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record311(lbl: Unit)
case class Record312(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record313(p_name: String, p_partkey: Int)
case class Record315(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record316(_1: Record311, _2: (Iterable[Record315]))
case class Record317(c_name: String, c_custkey: Int)
case class Record319(c__Fc_custkey: Int)
case class Record320(c_name: String, c_orders: Record319)
case class Record321(_1: Record311, _2: (Iterable[Record320]))
case class Record322(lbl: Record319)
case class Record323(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record325(o__Fo_orderkey: Int)
case class Record326(o_orderdate: String, o_parts: Record325)
case class Record327(_1: Record322, _2: (Iterable[Record326]))
case class Record328(lbl: Record325)
case class Record330(p_name: String, l_qty: Double)
case class Record331(_1: Record328, _2: (Iterable[Record330]))
object ShredQuery1Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1Spark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val C__F = 1
val C__D_1 = tpch.loadCustomers
C__D_1.cache
C__D_1.count
val O__F = 2
val O__D_1 = tpch.loadOrders
O__D_1.cache
O__D_1.count
val L__F = 3
val L__D_1 = tpch.loadLineitem
L__D_1.cache
L__D_1.count
val P__F = 4
val P__D_1 = tpch.loadPart
P__D_1.cache
P__D_1.count

   def f = { 
 val x153 = () 
val x154 = Record311(x153) 
val ljp__F = x154
val x155 = ljp__F
//ljp__F.collect.foreach(println(_))
val x156 = List(ljp__F) 
val x158 = x156 
val x159 = L__D_1 
val x165 = x159.map(x160 => { val x161 = x160.l_orderkey 
val x162 = x160.l_quantity 
val x163 = x160.l_partkey 
val x164 = Record312(x161, x162, x163) 
x164 }) 
val x168 = x165.map{ case c => (x158.head, c) } 
val x169 = P__D_1 
val x174 = x169.map(x170 => { val x171 = x170.p_name 
val x172 = x170.p_partkey 
val x173 = Record313(x171, x172) 
x173 }) 
val x180 = { val out1 = x168.map{ case (x175, x176) => ({val x178 = x176.l_partkey 
x178}, (x175, x176)) }
  val out2 = x174.map{ case x177 => ({val x179 = x177.p_partkey 
x179}, x177) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x191 = x180.map{ case ((x181, x182), x183) => val x190 = (x182,x183) 
x190 match {
   case (_,null) => ({val x184 = (x181) 
x184}, null) 
   case x189 => ({val x184 = (x181) 
x184}, {val x185 = x182.l_orderkey 
val x186 = x183.p_name 
val x187 = x182.l_quantity 
val x188 = Record315(x185, x186, x187) 
x188})
 }
}.groupByLabel() 
val x196 = x191.map{ case (x192, x193) => 
   val x194 = (x193) 
val x195 = Record316(x192, x194) 
x195 
} 
// there is a bug here, related to the join that happens later
val ljp__D_1 = x196.flatMap(r => r._2)
val x197 = ljp__D_1
//ljp__D_1.collect.foreach(println(_))
val x198 = List(x154) 
val M_ctx1 = x198
val x199 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x201 = M_ctx1 
val x202 = C__D_1 
val x207 = x202.map(x203 => { val x204 = x203.c_name 
val x205 = x203.c_custkey 
val x206 = Record317(x204, x205) 
x206 }) 
val x210 = x207.map{ case c => (x201.head, c) } 
val x220 = x210.map{ case (x211, x212) => val x219 = (x212) 
x219 match {
   case (null) => ({val x213 = (x211) 
x213}, null) 
   case x218 => ({val x213 = (x211) 
x213}, {val x214 = x212.c_name 
val x215 = x212.c_custkey 
val x216 = Record319(x215) 
val x217 = Record320(x214, x216) 
x217})
 }
}.groupByLabel() 
val x225 = x220.map{ case (x221, x222) => 
   val x223 = (x222) 
val x224 = Record321(x221, x223) 
x224 
} 
val M_flat1 = x225
val x226 = M_flat1
//M_flat1.collect.foreach(println(_))
val x228 = M_flat1 
val x232 = x228.flatMap{ case x229 => x229 match {
   case null => List((x229, null))
   case _ =>
   val x230 = x229._2 
x230 match {
     case x231 => x231.map{ case v2 => (x229, v2) }
  }
 }} 
val x237 = x232.map{ case (x233, x234) => 
   val x235 = x234.c_orders 
val x236 = Record322(x235) 
x236 
} 
val x238 = x237.distinct 
val M_ctx2 = x238
val x239 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x241 = M_ctx2 
val x242 = O__D_1 
val x248 = x242.map(x243 => { val x244 = x243.o_orderdate 
val x245 = x243.o_orderkey 
val x246 = x243.o_custkey 
val x247 = Record323(x244, x245, x246) 
x247 }) 
val x254 = { val out1 = x241.map{ case x249 => ({val x251 = x249.lbl 
val x252 = x251.c__Fc_custkey 
x252}, x249) }
  val out2 = x248.map{ case x250 => ({val x253 = x250.o_custkey 
x253}, x250) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x264 = x254.map{ case (x255, x256) => val x263 = (x256) 
x263 match {
   case (null) => ({val x257 = (x255) 
x257}, null) 
   case x262 => ({val x257 = (x255) 
x257}, {val x258 = x256.o_orderdate 
val x259 = x256.o_orderkey 
val x260 = Record325(x259) 
val x261 = Record326(x258, x260) 
x261})
 }
}.groupByLabel() 
val x269 = x264.map{ case (x265, x266) => 
   val x267 = (x266) 
val x268 = Record327(x265, x267) 
x268 
} 
val M_flat2 = x269
val x270 = M_flat2
//M_flat2.collect.foreach(println(_))
val x272 = M_flat2 
val x276 = x272.flatMap{ case x273 => x273 match {
   case null => List((x273, null))
   case _ =>
   val x274 = x273._2 
x274 match {
     case x275 => x275.map{ case v2 => (x273, v2) }
  }
 }} 
val x281 = x276.map{ case (x277, x278) => 
   val x279 = x278.o_parts 
val x280 = Record328(x279) 
x280 
} 
val x282 = x281.distinct 
val M_ctx3 = x282
val x283 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x285 = M_ctx3 
val x286 = ljp__D_1 
val x288 = x286 
val x294 = { val out1 = x285.map{ case x289 => ({val x291 = x289.lbl 
val x292 = x291.o__Fo_orderkey 
x292}, x289) }
  val out2 = x288.map{ case x290 => ({val x293 = x290.l_orderkey 
x293}, x290) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x303 = x294.map{ case (x295, x296) => val x302 = (x296) 
x302 match {
   case (null) => ({val x297 = (x295) 
x297}, null) 
   case x301 => ({val x297 = (x295) 
x297}, {val x298 = x296.p_name 
val x299 = x296.l_qty 
val x300 = Record330(x298, x299) 
x300})
 }
}.groupByLabel() 
val x308 = x303.map{ case (x304, x305) => 
   val x306 = (x305) 
val x307 = Record331(x304, x306) 
x307 
} 
val M_flat3 = x308
val x309 = M_flat3
//M_flat3.collect.foreach(println(_))
val res = x309
res.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis()
   println("ShredQuery1Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
