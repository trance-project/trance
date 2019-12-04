
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record268(o_orderkey: Int, o_custkey: Int)
case class Record269(c_name: String, c_custkey: Int)
case class Record270(o_orderkey: Int, c_name: String)
case class Record271(s_name: String, s_suppkey: Int)
case class Record272(l_orderkey: Int, l_suppkey: Int)
case class Record274(c_name2: String)
case class Record275(s_name: String, customers2: Iterable[Record274])
case class Record333(c_name: String, s_name: String)
case class Record334(c_name: String)
case class Record336(s_name: String)
case class Record337(c_name: String, suppliers: Iterable[Record336])
object Query6SparkUnnest {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query6SparkUnnest"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L = tpch.loadLineitem
L.cache
L.count
val C = tpch.loadCustomers
C.cache
C.count
val O = tpch.loadOrders
O.cache
O.count
val S = tpch.loadSupplier
S.cache
S.count

   val Query2 = {
 val x212 = O.map(x208 => { val x209 = x208.o_orderkey 
val x210 = x208.o_custkey 
val x211 = Record268(x209, x210) 
x211 }) 
val x217 = C.map(x213 => { val x214 = x213.c_name 
val x215 = x213.c_custkey 
val x216 = Record269(x214, x215) 
x216 }) 
val x222 = { val out1 = x212.map{ case x218 => ({val x220 = x218.o_custkey 
x220}, x218) }
  val out2 = x217.map{ case x219 => ({val x221 = x219.c_custkey 
x221}, x219) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x228 = x222.map{ case (x223, x224) => 
   val x225 = x223.o_orderkey 
val x226 = x224.c_name 
val x227 = Record270(x225, x226) 
x227 
} 
val resultInner = x228
val x229 = resultInner
//resultInner.collect.foreach(println(_))
val x234 = S.map(x230 => { val x231 = x230.s_name 
val x232 = x230.s_suppkey 
val x233 = Record271(x231, x232) 
x233 }) 
val x239 = L.map(x235 => { val x236 = x235.l_orderkey 
val x237 = x235.l_suppkey 
val x238 = Record272(x236, x237) 
x238 }) 
val x244 = { val out1 = x234.map{ case x240 => ({val x242 = x240.s_suppkey 
x242}, x240) }
  val out2 = x239.map{ case x241 => ({val x243 = x241.l_suppkey 
x243}, x241) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x246 = resultInner 
val x252 = { val out1 = x244.map{ case (x247, x248) => ({val x250 = x248.l_orderkey 
x250}, (x247, x248)) }
  val out2 = x246.map{ case x249 => ({val x251 = x249.o_orderkey 
x251}, x249) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x261 = x252.flatMap{ case ((x253, x254), x255) => val x260 = (x254,x255) 
x260 match {
   case (_,null) => Nil 
   case x259 => List(({val x256 = (x253) 
x256}, {val x257 = x255.c_name 
val x258 = Record274(x257) 
x258}))
 }
}.groupByKey() 
val x266 = x261.map{ case (x262, x263) => 
   val x264 = x262.s_name 
val x265 = Record275(x264, x263) 
x265 
} 
x266
}
Query2.cache
Query2.count
def f = { 
 val x296 = Query2 
val x300 = x296.flatMap{ case x297 => x297 match {
   case null => List((x297, null))
   case _ =>
   val x298 = x297.customers2 
x298 match {
     case x299 => x299.map{ case v2 => (x297, v2) }
  }
 }} 
val x306 = x300.map{ case (x301, x302) => 
   val x303 = x302.c_name2 
val x304 = x301.s_name 
val x305 = Record333(x303, x304) 
x305 
} 
val cflat = x306
val x307 = cflat
//cflat.collect.foreach(println(_))
val x311 = C.map(x308 => { val x309 = x308.c_name 
val x310 = Record334(x309) 
x310 }) 
val x313 = cflat 
val x318 = { val out1 = x311.map{ case x314 => ({val x316 = x314.c_name 
x316}, x314) }
  val out2 = x313.map{ case x315 => ({val x317 = x315.c_name 
x317}, x315) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x326 = x318.flatMap{ case (x319, x320) => val x325 = (x320) 
x325 match {
   case (null) => Nil 
   case x324 => List(({val x321 = (x319) 
x321}, {val x322 = x320.s_name 
val x323 = Record336(x322) 
x323}))
 }
}.groupByKey() 
val x331 = x326.map{ case (x327, x328) => 
   val x329 = x327.c_name 
val x330 = Record337(x329, x328) 
x330 
} 
x331.collect.foreach(println(_))
x331.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("Query6SparkUnnest"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
