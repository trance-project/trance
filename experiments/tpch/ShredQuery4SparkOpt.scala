
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record206(c__Fc_custkey: Int)
case class Record207(c_name: String, corders: Record206)
case class Record208(o_orderkey: Int, o_orderdate: String)
case class Record283(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record284(p_name: String, p_partkey: Int)
case class Record285(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record286(customer__Fcorders: Record206)
case class Record287(c_name: String, partqty: Record286)
case class Record289(orderdate: String, pname: String)
object ShredQuery4SparkOpt {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4SparkOpt"+sf)
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

   val x193 = C__D_1.map{ case x188 => 
   val x189 = x188.c_name 
val x190 = x188.c_custkey 
val x191 = Record206(x190) 
val x192 = Record207(x189, x191) 
x192 
} 
val M_flat1 = x193
val x194 = M_flat1
//M_flat1.collect.foreach(println(_))
val x203 = O__D_1.map{ case x195 => 
   val x196 = x195.o_custkey 
val x197 = Record206(x196) 
val x198 = x195.o_orderkey 
val x199 = x195.o_orderdate 
val x200 = Record208(x198, x199) 
val x201 = List(x200) 
val x202 = (x197, x201) 
x202 
} 
val M_flat2 = x203
val x204 = M_flat2
//M_flat2.collect.foreach(println(_))
val CustOrders__D_1 = M_flat1
CustOrders__D_1.cache
CustOrders__D_1.count
val CustOrders__D_2corders_1 = M_flat2
CustOrders__D_2corders_1.cache
CustOrders__D_2corders_1.count
def f = { 
 val x235 = L__D_1.map(x230 => { val x231 = x230.l_orderkey 
val x232 = x230.l_quantity 
val x233 = x230.l_partkey 
val x234 = Record283(x231, x232, x233) 
x234 }) 
val x240 = P__D_1.map(x236 => { val x237 = x236.p_name 
val x238 = x236.p_partkey 
val x239 = Record284(x237, x238) 
x239 }) 
val x245 = { val out1 = x235.map{ case x241 => ({val x243 = x241.l_partkey 
x243}, x241) }
  val out2 = x240.map{ case x242 => ({val x244 = x242.p_partkey 
x244}, x242) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x252 = x245.map{ case (x246, x247) => 
   val x248 = x246.l_orderkey 
val x249 = x247.p_name 
val x250 = x246.l_quantity 
val x251 = Record285(x248, x249, x250) 
x251 
} 
val parts__D_1 = x252
val x253 = parts__D_1
//parts__D_1.collect.foreach(println(_))
val x259 = CustOrders__D_1.map{ case x254 => 
   val x255 = x254.c_name 
val x256 = x254.corders 
val x257 = Record286(x256) 
val x258 = Record287(x255, x257) 
x258 
} 
val M_flat1 = x259
val x260 = M_flat1
//M_flat1.collect.foreach(println(_))
val x261 = CustOrders__D_2corders_1
val x262 = x261 
val x264 = x262 
val x266 = parts__D_1 
val x271 = { val out1 = x264.flatMap{ case x267 => x267._2.map{ v => ({val x269 = v.o_orderkey 
x269}, (x267._1, v))  }}
  val out2 = x266.map{ case x268 => ({val x270 = x268.l_orderkey 
x270}, x268) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x280 = x271.flatMap{ case ((x272,x272a), x273) => val x279 = (x272,x273) 
x279 match {
   case (_,null) => Nil
   case x278 => List(({val x274 = x272a.o_orderdate 
val x275 = x273.p_name 
val x276 = (x272, Record289(x274, x275))
x276}, {val x277 = x273.l_qty 
x277}))
 }
}.reduceByKey(_ + _) 
val M_flat2 = x280
val x281 = M_flat2
//M_flat2.collect.foreach(println(_))
x281.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery4SparkOpt"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
