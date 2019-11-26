
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record205(lbl: Unit)
case class Record206(c__Fc_custkey: Int)
case class Record207(c_name: String, corders: Record206)
case class Record208(lbl: Record206)
case class Record209(o_orderkey: Int, o_orderdate: String, o_custkey: Int)
case class Record211(o_orderkey: Int, o_orderdate: String)
case class Record323(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record324(p_name: String, p_partkey: Int)
case class Record325(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record326(customer__Fcorders: Record206)
case class Record327(c_name: String, partqty: Record326)
case class Record328(lbl: Record326)
case class Record330(orderdate: String, pname: String)
object ShredQuery4SparkDomainsNestedToNested {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4SparkDomainsNestedToNested"+sf)
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

   val x156 = () 
val x157 = Record205(x156) 
val x158 = List(x157) 
val M_ctx1 = x158
val x159 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x165 = C__D_1.map{ case x160 => 
   val x161 = x160.c_name 
val x162 = x160.c_custkey 
val x163 = Record206(x162) 
val x164 = Record207(x161, x163) 
x164 
} 
val M_flat1 = x165
val x166 = M_flat1
//M_flat1.collect.foreach(println(_))
val x168 = M_flat1 
val x172 = x168.map{ case x169 => 
   val x170 = x169.corders 
val x171 = Record208(x170) 
x171 
} 
val x173 = x172.distinct 
val M_ctx2 = x173
val x174 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x176 = M_ctx2 
val x182 = O__D_1.map(x177 => { val x178 = x177.o_orderkey 
val x179 = x177.o_orderdate 
val x180 = x177.o_custkey 
val x181 = Record209(x178, x179, x180) 
x181 }) 
val x188 = { val out1 = x176.map{ case x183 => ({val x185 = x183.lbl 
val x186 = x185.c__Fc_custkey 
x186}, x183) }
  val out2 = x182.map{ case x184 => ({val x187 = x184.o_custkey 
x187}, x184) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x197 = x188.flatMap{ case (x189, x190) => val x196 = (x190) 
x196 match {
   case (null) => Nil 
   case x195 => List(({val x191 = (x189) 
x191}, {val x192 = x190.o_orderkey 
val x193 = x190.o_orderdate 
val x194 = Record211(x192, x193) 
x194}))
 }
}.groupByLabel() 
val x202 = x197.map{ case (x198, x199) => 
   val x200 = x198.lbl 
val x201 = (x200, x199) 
x201 
} 
val M_flat2 = x202
val x203 = M_flat2
//M_flat2.collect.foreach(println(_))
val CustOrders__D_1 = M_flat1
CustOrders__D_1.cache
CustOrders__D_1.count
val CustOrders__D_2corders_1 = M_flat2
CustOrders__D_2corders_1.cache
CustOrders__D_2corders_1.count
def f = { 
 val x246 = () 
val x247 = Record205(x246) 
val x248 = List(x247) 
val M_ctx1 = x248
val x249 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x255 = L__D_1.map(x250 => { val x251 = x250.l_orderkey 
val x252 = x250.l_quantity 
val x253 = x250.l_partkey 
val x254 = Record323(x251, x252, x253) 
x254 }) 
val x260 = P__D_1.map(x256 => { val x257 = x256.p_name 
val x258 = x256.p_partkey 
val x259 = Record324(x257, x258) 
x259 }) 
val x265 = { val out1 = x255.map{ case x261 => ({val x263 = x261.l_partkey 
x263}, x261) }
  val out2 = x260.map{ case x262 => ({val x264 = x262.p_partkey 
x264}, x262) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x272 = x265.map{ case (x266, x267) => 
   val x268 = x266.l_orderkey 
val x269 = x267.p_name 
val x270 = x266.l_quantity 
val x271 = Record325(x268, x269, x270) 
x271 
} 
val parts__D_1 = x272
val x273 = parts__D_1
//parts__D_1.collect.foreach(println(_))
val x279 = CustOrders__D_1.map{ case x274 => 
   val x275 = x274.c_name 
val x276 = x274.corders 
val x277 = Record326(x276) 
val x278 = Record327(x275, x277) 
x278 
} 
val M_flat1 = x279
val x280 = M_flat1
//M_flat1.collect.foreach(println(_))
val x282 = M_flat1 
val x286 = x282.map{ case x283 => 
   val x284 = x283.partqty 
val x285 = Record328(x284) 
x285 
} 
val x287 = x286.distinct 
val M_ctx2 = x287
val x288 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x290 = M_ctx2 
val x295 = { val out1 = x290.map{ case x291 => ({val x293 = x291.lbl 
val x294 = x293.customer__Fcorders 
x294}, x291) }
  val out2 = CustOrders__D_2corders_1.flatMapValues(identity)
  out1.lookup(out2)
} 
val x297 = parts__D_1 
val x303 = { val out1 = x295.map{ case (x298, x299) => ({val x301 = x299.o_orderkey 
x301}, (x298, x299)) }
  val out2 = x297.map{ case x300 => ({val x302 = x300.l_orderkey 
x302}, x300) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x314 = x303.flatMap{ case ((x304, x305), x306) => val x313 = (x304,x305,x306) 
x313 match {
   case (_,null,_) => Nil
case (_,_,null) => Nil
   case x312 => List(({val x307 = x305.o_orderdate 
val x308 = x306.p_name 
val x309 = Record330(x307, x308) 
val x310 = (x304,x309) 
x310}, {val x311 = x306.l_qty 
x311}))
 }
}.reduceByKey(_ + _) 
val x320 = x314.map{ case ((x315, x316), x317) => 
   val x318 = x315.lbl 
val x319 = (x318, x316) 
x319 
} 
val M_flat2 = x320
val x321 = M_flat2
//M_flat2.collect.foreach(println(_))
x321.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery4SparkDomainsNestedToNested"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
