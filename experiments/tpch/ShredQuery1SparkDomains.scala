
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record272(lbl: Unit)
case class Record273(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record274(p_name: String, p_partkey: Int)
case class Record275(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record276(c__Fc_custkey: Int)
case class Record277(c_name: String, c_orders: Record276)
case class Record278(lbl: Record276)
case class Record279(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record281(o__Fo_orderkey: Int)
case class Record282(o_orderdate: String, o_parts: Record281)
case class Record283(lbl: Record281)
case class Record285(p_name: String, l_qty: Double)
object ShredQuery1SparkDomains {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1SparkDomains"+sf)
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
 val x160 = () 
val x161 = Record272(x160) 
val x162 = List(x161) 
val M_ctx1 = x162
val x163 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x169 = L__D_1.map(x164 => { val x165 = x164.l_orderkey 
val x166 = x164.l_quantity 
val x167 = x164.l_partkey 
val x168 = Record273(x165, x166, x167) 
x168 }) 
val x174 = P__D_1.map(x170 => { val x171 = x170.p_name 
val x172 = x170.p_partkey 
val x173 = Record274(x171, x172) 
x173 }) 
val x179 = { val out1 = x169.map{ case x175 => ({val x177 = x175.l_partkey 
x177}, x175) }
  val out2 = x174.map{ case x176 => ({val x178 = x176.p_partkey 
x178}, x176) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x186 = x179.map{ case (x180, x181) => 
   val x182 = x180.l_orderkey 
val x183 = x181.p_name 
val x184 = x180.l_quantity 
val x185 = Record275(x182, x183, x184) 
x185 
} 
val ljp__D_1 = x186
val x187 = ljp__D_1
//ljp__D_1.collect.foreach(println(_))
val x193 = C__D_1.map{ case x188 => 
   val x189 = x188.c_name 
val x190 = x188.c_custkey 
val x191 = Record276(x190) 
val x192 = Record277(x189, x191) 
x192 
} 
val M_flat1 = x193
val x194 = M_flat1
//M_flat1.collect.foreach(println(_))
val x196 = M_flat1 
val x200 = x196.map{ case x197 => 
   val x198 = x197.c_orders 
val x199 = Record278(x198) 
x199 
} 
val x201 = x200.distinct 
val M_ctx2 = x201
val x202 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x204 = M_ctx2 
val x210 = O__D_1.map(x205 => { val x206 = x205.o_orderdate 
val x207 = x205.o_orderkey 
val x208 = x205.o_custkey 
val x209 = Record279(x206, x207, x208) 
x209 }) 
val x216 = { val out1 = x204.map{ case x211 => ({val x213 = x211.lbl 
val x214 = x213.c__Fc_custkey 
x214}, x211) }
  val out2 = x210.map{ case x212 => ({val x215 = x212.o_custkey 
x215}, x212) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x226 = x216.flatMap{ case (x217, x218) => val x225 = (x218) 
x225 match {
   case (null) => Nil 
   case x224 => List(({val x219 = (x217) 
x219}, {val x220 = x218.o_orderdate 
val x221 = x218.o_orderkey 
val x222 = Record281(x221) 
val x223 = Record282(x220, x222) 
x223}))
 }
}.groupByLabel() 
val x231 = x226.map{ case (x227, x228) => 
   val x229 = x227.lbl 
val x230 = (x229, x228) 
x230 
} 
val M_flat2 = x231
val x232 = M_flat2
//M_flat2.collect.foreach(println(_))
val x234 = M_flat2 
val x238 = x234.flatMap{ case x235 => x235 match {
   case null => List((x235, null))
   case _ =>
   val x236 = x235._2 
x236 match {
     case x237 => x237.map{ case v2 => (x235, v2) }
  }
 }} 
val x243 = x238.map{ case (x239, x240) => 
   val x241 = x240.o_parts 
val x242 = Record283(x241) 
x242 
} 
val x244 = x243.distinct 
val M_ctx3 = x244
val x245 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x247 = M_ctx3 
val x249 = ljp__D_1 
val x255 = { val out1 = x247.map{ case x250 => ({val x252 = x250.lbl 
val x253 = x252.o__Fo_orderkey 
x253}, x250) }
  val out2 = x249.map{ case x251 => ({val x254 = x251.l_orderkey 
x254}, x251) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x264 = x255.flatMap{ case (x256, x257) => val x263 = (x257) 
x263 match {
   case (null) => Nil 
   case x262 => List(({val x258 = (x256) 
x258}, {val x259 = x257.p_name 
val x260 = x257.l_qty 
val x261 = Record285(x259, x260) 
x261}))
 }
}.groupByLabel() 
val x269 = x264.map{ case (x265, x266) => 
   val x267 = x265.lbl 
val x268 = (x267, x266) 
x268 
} 
val M_flat3 = x269
val x270 = M_flat3
//M_flat3.collect.foreach(println(_))
x270.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery1SparkDomains"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
