
package experiments
/**

For c in C Union
  Sng((c_name := c.c_name, suppliers := For co in Query2 Union
    For co2 in co.customers2 Union
      If (co2.c_name2 = c.c_name)
      Then Sng((s_name := co.s_name))))

**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record226(o_orderkey: Int, o_custkey: Int)
case class Record227(c_name: String, c_custkey: Int)
case class Record228(o_orderkey: Int, c_name: String)
case class Record229(s__Fs_suppkey: Int)
case class Record230(s_name: String, customers2: Record229)
case class Record231(l_suppkey: Int, l_orderkey: Int)
case class Record233(c_name2: String)
case class Record266(c__Fc_name: String)
case class Record267(c_name: String, suppliers: Record266)
case class Record268(s_name: String)
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

   val x173 = O__D_1.map(x169 => { val x170 = x169.o_orderkey 
val x171 = x169.o_custkey 
val x172 = Record226(x170, x171) 
x172 }) 
val x178 = C__D_1.map(x174 => { val x175 = x174.c_name 
val x176 = x174.c_custkey 
val x177 = Record227(x175, x176) 
x177 }) 
val x183 = { val out1 = x173.map{ case x179 => ({val x181 = x179.o_custkey 
x181}, x179) }
  val out2 = x178.map{ case x180 => ({val x182 = x180.c_custkey 
x182}, x180) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x189 = x183.map{ case (x184, x185) => 
   val x186 = x184.o_orderkey 
val x187 = x185.c_name 
val x188 = Record228(x186, x187) 
x188 
} 
val resultInner__D_1 = x189
val x190 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val x196 = S__D_1.map{ case x191 => 
   val x192 = x191.s_name 
val x193 = x191.s_suppkey 
val x194 = Record229(x193) 
val x195 = Record230(x192, x194) 
x195 
} 
val M_flat1 = x196
val x197 = M_flat1
//M_flat1.collect.foreach(println(_))
val x202 = L__D_1.map(x198 => { val x199 = x198.l_suppkey 
val x200 = x198.l_orderkey 
val x201 = Record231(x199, x200) 
x201 }) 
val x204 = resultInner__D_1 
val x209 = { val out1 = x202.map{ case x205 => ({val x207 = x205.l_orderkey 
x207}, x205) }
  val out2 = x204.map{ case x206 => ({val x208 = x206.o_orderkey 
x208}, x206) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x217 = x209.flatMap{ case (x210, x211) => val x216 = (x211) 
x216 match {
   case (null) => Nil 
   case x215 => List(({val x212 = (x210) 
x212}, {val x213 = x211.c_name 
val x214 = Record233(x213) 
x214}))
 }
}.groupByLabel() 
val x223 = x217.map{ case (x218, x219) => 
   val x220 = x218.l_suppkey 
val x221 = Record229(x220) 
val x222 = (x221, x219) 
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
/**
M_flat1 :=  REDUCE[ (c_name := x237.c_name,suppliers := (c__Fc_name := x237.c_name)) / true ](C__D_1)
**/ 
 
 val x247 = C__D_1.map{ case x243 => 
   val x244 = x243.c_name 
val x245 = Record266(x244) 
val x246 = Record267(x244, x245) 
x246 
} 
val M_flat1 = x247
val x248 = M_flat1
M_flat1.count
//M_flat1.collect.foreach(println(_))
/**
M_flat2 :=  REDUCE[ (key := (c__Fc_name := x242.c_name2),value := { (s_name := x241.s_name) }) / true ]( <-- (x238,x240) -- (
 <-- (x238) -- SELECT[ true, x238 ](Query2__D_1)) LOOKUP[x239.customers2, true = true](
   Query2__D_2customers2_1))
**/

val x250 = Query2__D_1 
val x254 = {
 val out1 = x250.map{case x251 => ({val x253 = x251.customers2 
x253},x251)}
 val out2 = Query2__D_2customers2_1.flatMapValues(identity)
 out1.lookupSkewRight(out2)

} 
val x263 = x254.map{ case (x255, x256) => 
   val x257 = x256.c_name2 
val x258 = Record266(x257) 
val x259 = x255.s_name 
val x260 = Record268(x259) 
val x261 = List(x260) 
val x262 = (x258, x261) 
x262 
}.groupByLabel() 
val M_flat2 = x263
val x264 = M_flat2
//M_flat2.collect.foreach(println(_))
x264.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery6SparkOpt"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
