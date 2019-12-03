
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record226(l_suppkey: Int, l_orderkey: Int)
case class Record227(o_custkey: Int, o_orderkey: Int)
case class Record228(c_name: String, c_custkey: Int)
case class Record229(l_suppkey: Int, c_name: String)
case class Record230(s__Fs_suppkey: Int)
case class Record231(s_name: String, customers2: Record230)
case class Record232(c_name2: String)
case class Record273(c_name: String, s_name: String)
case class Record274(c__Fc_name: String)
case class Record275(c_name: String, suppliers: Record274)
case class Record276(s_name: String)
object ShredQuery6SparkOpt {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6SparkOpt"+sf)
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

   val x179 = L__D_1.map(x175 => { val x176 = x175.l_suppkey 
val x177 = x175.l_orderkey 
val x178 = Record226(x176, x177) 
x178 }) 
val x184 = O__D_1.map(x180 => { val x181 = x180.o_custkey 
val x182 = x180.o_orderkey 
val x183 = Record227(x181, x182) 
x183 }) 
val x189 = { val out1 = x179.map{ case x185 => ({val x187 = x185.l_orderkey 
x187}, x185) }
  val out2 = x184.map{ case x186 => ({val x188 = x186.o_orderkey 
x188}, x186) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x194 = C__D_1.map(x190 => { val x191 = x190.c_name 
val x192 = x190.c_custkey 
val x193 = Record228(x191, x192) 
x193 }) 
val x200 = { val out1 = x189.map{ case (x195, x196) => ({val x198 = x196.o_custkey 
x198}, (x195, x196)) }
  val out2 = x194.map{ case x197 => ({val x199 = x197.c_custkey 
x199}, x197) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x207 = x200.map{ case ((x201, x202), x203) => 
   val x204 = x201.l_suppkey 
val x205 = x203.c_name 
val x206 = Record229(x204, x205) 
x206 
} 
val resultInner__D_1 = x207
val x208 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val x214 = S__D_1.map{ case x209 => 
   val x210 = x209.s_name 
val x211 = x209.s_suppkey 
val x212 = Record230(x211) 
val x213 = Record231(x210, x212) 
x213 
} 
val M_flat1 = x214
val x215 = M_flat1
//M_flat1.collect.foreach(println(_))
val x223 = resultInner__D_1.map{ case x216 => 
   val x217 = x216.l_suppkey 
val x218 = Record230(x217) 
val x219 = x216.c_name 
val x220 = Record232(x219) 
val x221 = List(x220) 
val x222 = (x218, x221) 
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
 val x245 = Query2__D_1 
val x249 = { val out1 = x245.map{ case x246 => ({val x248 = x246.customers2 
x248}, x246) }
  val out2 = Query2__D_2customers2_1.flatMapValues(identity)
  out2.lookupSkewLeft(out1)
} 
val x255 = x249.map{ case (x251, x250) => 
   val x252 = x251.c_name2 
val x253 = x250.s_name 
val x254 = Record273(x252, x253) 
x254 
} 
val cflat__D_1 = x255
val x256 = cflat__D_1
//cflat__D_1.collect.foreach(println(_))
val x261 = C__D_1.map{ case x257 => 
   val x258 = x257.c_name 
val x259 = Record274(x258) 
val x260 = Record275(x258, x259) 
x260 
} 
val M_flat1 = x261
val x262 = M_flat1
M_flat1.count
//M_flat1.collect.foreach(println(_))
val x270 = cflat__D_1.map{ case x263 => 
   val x264 = x263.c_name 
val x265 = Record274(x264) 
val x266 = x263.s_name 
val x267 = Record276(x266) 
val x268 = List(x267) 
val x269 = (x265, x268) 
x269 
} 
val M_flat2 = x270
val x271 = M_flat2
//M_flat2.collect.foreach(println(_))
x271.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery6SparkOpt"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
