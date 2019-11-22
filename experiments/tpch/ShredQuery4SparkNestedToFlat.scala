
package experiments
/** 
Query 4 Flat to Nested

let parts = For l in L Union
  For p in P Union
    If (l.l_partkey = p.p_partkey)
    Then Sng((l_orderkey := l.l_orderkey, p_name := p.p_name, l_qty := l.l_quantity))

(For customer in CustOrders Union
  For order in customer.corders Union
    For part in parts Union
      If (order.o_orderkey = part.l_orderkey)
      Then Sng((c_name := customer.c_name, orderdate := order.o_orderdate, pname := part.p_name, l_qty := part.l_qty))).groupBy+((c_name := x1.c_name, orderdate := x1.orderdate, pname := x1.pname)), x1.l_qty)

**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record194(lbl: Unit)
case class Record195(c__Fc_custkey: Int)
case class Record196(c_name: String, corders: Record195)
case class Record197(lbl: Record195)
case class Record198(o_orderkey: Int, o_orderdate: String, o_custkey: Int)
case class Record200(o_orderkey: Int, o_orderdate: String)
case class Record280(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record281(p_name: String, p_partkey: Int)
case class Record282(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record284(c_name: String, orderdate: String, pname: String)
object ShredQuery4SparkNestedToFlat {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4SparkNestedToFlat"+sf)
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

   val x145 = () 
val x146 = Record194(x145) 
val x147 = List(x146) 
val M_ctx1 = x147
val x148 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x154 = C__D_1.map{ case x149 => 
   val x150 = x149.c_name 
val x151 = x149.c_custkey 
val x152 = Record195(x151) 
val x153 = Record196(x150, x152) 
x153 
} 
val M_flat1 = x154
val x155 = M_flat1
//M_flat1.collect.foreach(println(_))
val x157 = M_flat1 
val x161 = x157.map{ case x158 => 
   val x159 = x158.corders 
val x160 = Record197(x159) 
x160 
} 
val x162 = x161.distinct 
val M_ctx2 = x162
val x163 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x165 = M_ctx2 
val x171 = O__D_1.map(x166 => { val x167 = x166.o_orderkey 
val x168 = x166.o_orderdate 
val x169 = x166.o_custkey 
val x170 = Record198(x167, x168, x169) 
x170 }) 
val x177 = { val out1 = x165.map{ case x172 => ({val x174 = x172.lbl 
val x175 = x174.c__Fc_custkey 
x175}, x172) }
  val out2 = x171.map{ case x173 => ({val x176 = x173.o_custkey 
x176}, x173) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x186 = x177.flatMap{ case (x178, x179) => val x185 = (x179) 
x185 match {
   case (null) => Nil 
   case x184 => List(({val x180 = (x178) 
x180}, {val x181 = x179.o_orderkey 
val x182 = x179.o_orderdate 
val x183 = Record200(x181, x182) 
x183}))
 }
}.groupByLabel() 
val x191 = x186.map{ case (x187, x188) => 
   val x189 = x187.lbl 
val x190 = (x189, x188) 
x190 
} 
val M_flat2 = x191
val x192 = M_flat2
//M_flat2.collect.foreach(println(_))
val CustOrders__D_1 = M_flat1
CustOrders__D_1.cache
CustOrders__D_1.count
val CustOrders__D_2corders_1 = M_flat2
CustOrders__D_2corders_1.cache
CustOrders__D_2corders_1.count
def f = { 
 val x225 = () 
val x226 = Record194(x225) 
val x227 = List(x226) 
val M_ctx1 = x227
val x228 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x234 = L__D_1.map(x229 => { val x230 = x229.l_orderkey 
val x231 = x229.l_quantity 
val x232 = x229.l_partkey 
val x233 = Record280(x230, x231, x232) 
x233 }) 
val x239 = P__D_1.map(x235 => { val x236 = x235.p_name 
val x237 = x235.p_partkey 
val x238 = Record281(x236, x237) 
x238 }) 
val x244 = { val out1 = x234.map{ case x240 => ({val x242 = x240.l_partkey 
x242}, x240) }
  val out2 = x239.map{ case x241 => ({val x243 = x241.p_partkey 
x243}, x241) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x251 = x244.map{ case (x245, x246) => 
   val x247 = x245.l_orderkey 
val x248 = x246.p_name 
val x249 = x245.l_quantity 
val x250 = Record282(x247, x248, x249) 
x250 
} 
val parts__D_1 = x251
val x252 = parts__D_1
//parts__D_1.collect.foreach(println(_))
val x254 = CustOrders__D_1 
val x258 = { val out1 = x254.map{ case x255 => ({val x257 = x255.corders 
x257}, x255) }
  val out2 = CustOrders__D_2corders_1.flatMapValues(identity)
  out1.lookup(out2)
} 
val x260 = parts__D_1 
val x266 = { val out1 = x258.map{ case (x261, x262) => ({val x264 = x262.o_orderkey 
x264}, (x261, x262)) }
  val out2 = x260.map{ case x263 => ({val x265 = x263.l_orderkey 
x265}, x263) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x277 = x266.flatMap{ case ((x267, x268), x269) => val x276 = (x267,x268,x269) 
x276 match {
   case (_,null,_) => Nil
case (_,_,null) => Nil
   case x275 => List(({val x270 = x267.c_name 
val x271 = x268.o_orderdate 
val x272 = x269.p_name 
val x273 = Record284(x270, x271, x272) 
x273}, {val x274 = x269.l_qty 
x274}))
 }
}.reduceByKey(_ + _) 
val M_flat1 = x277
val x278 = M_flat1
//M_flat1.collect.foreach(println(_))
x278.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery4SparkNestedToFlat"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
