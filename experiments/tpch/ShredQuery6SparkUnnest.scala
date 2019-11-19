
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record257(lbl: Unit)
case class Record258(o_orderkey: Int, o_custkey: Int)
case class Record259(c_name: String, c_custkey: Int)
case class Record260(o_orderkey: Int, c_name: String)
case class Record261(s__Fs_suppkey: Int)
case class Record262(s_name: String, customers2: Record261)
case class Record263(lbl: Record261)
case class Record264(l_orderkey: Int, l_suppkey: Int)
case class Record266(c_name2: String)
case class Record339(c__Fc_name: String)
case class Record340(c_name: String, suppliers: Record339)
case class Record341(lbl: Record339)
case class Record343(s_name: String)
object ShredQuery6SparkUnnest {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6SparkUnnest"+sf)
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

   val x179 = () 
val x180 = Record257(x179) 
val x181 = List(x180) 
val M_ctx1 = x181
val x182 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x187 = O__D_1.map(x183 => { val x184 = x183.o_orderkey 
val x185 = x183.o_custkey 
val x186 = Record258(x184, x185) 
x186 }) 
val x192 = C__D_1.map(x188 => { val x189 = x188.c_name 
val x190 = x188.c_custkey 
val x191 = Record259(x189, x190) 
x191 }) 
val x197 = { val out1 = x187.map{ case x193 => ({val x195 = x193.o_custkey 
x195}, x193) }
  val out2 = x192.map{ case x194 => ({val x196 = x194.c_custkey 
x196}, x194) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x203 = x197.map{ case (x198, x199) => 
   val x200 = x198.o_orderkey 
val x201 = x199.c_name 
val x202 = Record260(x200, x201) 
x202 
} 
val resultInner__D_1 = x203
val x204 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val x210 = S__D_1.map{ case x205 => 
   val x206 = x205.s_name 
val x207 = x205.s_suppkey 
val x208 = Record261(x207) 
val x209 = Record262(x206, x208) 
x209 
} 
val M_flat1 = x210
val x211 = M_flat1
//M_flat1.collect.foreach(println(_))
val x213 = M_flat1 
val x217 = x213.map{ case x214 => 
   val x215 = x214.customers2 
val x216 = Record263(x215) 
x216 
} 
val x218 = x217.distinct 
val M_ctx2 = x218
val x219 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x221 = M_ctx2 
val x226 = L__D_1.map(x222 => { val x223 = x222.l_orderkey 
val x224 = x222.l_suppkey 
val x225 = Record264(x223, x224) 
x225 }) 
val x232 = { val out1 = x221.map{ case x227 => ({val x229 = x227.lbl 
val x230 = x229.s__Fs_suppkey 
x230}, x227) }
  val out2 = x226.map{ case x228 => ({val x231 = x228.l_suppkey 
x231}, x228) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x234 = resultInner__D_1 
val x240 = { val out1 = x232.map{ case (x235, x236) => ({val x238 = x236.l_orderkey 
x238}, (x235, x236)) }
  val out2 = x234.map{ case x237 => ({val x239 = x237.o_orderkey 
x239}, x237) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x249 = x240.flatMap{ case ((x241, x242), x243) => val x248 = (x242,x243) 
x248 match {
   case (_,null) => Nil 
   case x247 => List(({val x244 = (x241) 
x244}, {val x245 = x243.c_name 
val x246 = Record266(x245) 
x246}))
 }
}.groupByLabel() 
val x254 = x249.map{ case (x250, x251) => 
   val x252 = x250.lbl 
val x253 = (x252, x251) 
x253 
} 
val M_flat2 = x254
val x255 = M_flat2
//M_flat2.collect.foreach(println(_))
val Query2__D_1 = M_flat1
Query2__D_1.cache
Query2__D_1.count
val Query2__D_2customers2_1 = M_flat2
Query2__D_2customers2_1.cache
Query2__D_2customers2_1.count
def f = { 
 val x290 = () 
val x291 = Record257(x290) 
val x292 = List(x291) 
val M_ctx1 = x292
val x293 = M_ctx1
//M_ctx1.collect.foreach(println(_))
/**
M_flat1 :=  REDUCE[ (c_name := x274.c_name,suppliers := (c__Fc_name := x274.c_name)) / true ](C__D_1)
**/
val x298 = C__D_1.map{ case x294 => 
   val x295 = x294.c_name 
val x296 = Record339(x295) 
val x297 = Record340(x295, x296) 
x297 
} 
val M_flat1 = x298
val x299 = M_flat1
//M_flat1.collect.foreach(println(_))
val x301 = M_flat1 
/**
M_ctx2 := DeDup( REDUCE[ (lbl := x276.suppliers) / true ](
 <-- (x275) -- SELECT[ true, x275 ](M_flat1)))
 **/

val x305 = x301.map{ case x302 => 
   val x303 = x302.suppliers 
val x304 = Record341(x303) 
x304 
} 
val x306 = x305.distinct 
val M_ctx2 = x306
val x307 = M_ctx2
//M_ctx2.collect.foreach(println(_))
/**
M_flat2 :=  REDUCE[ (_1 := x288.lbl,_2 := x289) / true ](  NEST[ U / (s_name := x285.s_name) / (x284), true / (x285,x286) ]( <-- (x277,x280,x283) -- ( <-- (x277,x280) -- (
 <-- (x277) -- SELECT[ true, x277 ](M_ctx2)) JOIN[true = true](

   <-- (x278) -- SELECT[ true, x278 ](Query2__D_1))) OUTERLOOKUP[x282.customers2, x283.c_name2 = x281.lbl.c__Fc_name](
   Query2__D_2customers2_1)))
**/
val x309 = M_ctx2 
val x311 = Query2__D_1 
val x314 = x309.cartesian(x311) 
val x322 = { val out1 = x314.map{ case (x315, x316) => (({val x318 = x316.customers2 
x318}, {val x320 = x315.lbl 
val x321 = x320.c__Fc_name 
x321}), (x315, x316)) }
  val out2 = Query2__D_2customers2_1
        .flatMap(v2 => v2._2.map{case x317 => ((v2._1, {val x319 = x317.c_name2 
x319}), x317)})
  out2.lookupSkewLeft(out1)
} 
val x331 = x322.flatMap{ case (x325, (x323, x324)) => val x330 = (x324,x325) 
x330 match {
   case (_,null) => Nil 
   case x329 => List(({val x326 = (x323) 
x326}, {val x327 = x324.s_name 
val x328 = Record343(x327) 
x328}))
 }
}.groupByLabel() 
val x336 = x331.map{ case (x332, x333) => 
   val x334 = x332.lbl 
val x335 = (x334, x333) 
x335 
} 
val M_flat2 = x336
val x337 = M_flat2
M_flat2.collect.foreach(println(_))
x337.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery6SparkUnnest"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
