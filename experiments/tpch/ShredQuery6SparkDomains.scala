
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record111(lbl: Unit)
case class Record112(o_orderkey: Int, o_custkey: Int)
case class Record113(c_name: String, c_custkey: Int)
case class Record114(o_orderkey: Int, c_name: String)
case class Record115(s__Fs_suppkey: Int)
case class Record116(s_name: String, customers2: Record115)
case class Record117(lbl: Record115)
case class Record118(l_orderkey: Int, l_suppkey: Int)
case class Record120(c_name2: String)
case class Record202(c_name: String, s_name: String)
case class Record203(c__Fc_name: String)
case class Record204(c_name: String, suppliers: Record203)
case class Record205(lbl: Record203)
case class Record207(s_name: String)
object ShredQuery6SparkDomains {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6SparkDomains"+sf)
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

   val x33 = () 
val x34 = Record111(x33) 
val x35 = List(x34) 
val M_ctx1 = x35
val x36 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x41 = O__D_1.map(x37 => { val x38 = x37.o_orderkey 
val x39 = x37.o_custkey 
val x40 = Record112(x38, x39) 
x40 }) 
val x46 = C__D_1.map(x42 => { val x43 = x42.c_name 
val x44 = x42.c_custkey 
val x45 = Record113(x43, x44) 
x45 }) 
val x51 = { val out1 = x41.map{ case x47 => ({val x49 = x47.o_custkey 
x49}, x47) }
  val out2 = x46.map{ case x48 => ({val x50 = x48.c_custkey 
x50}, x48) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x57 = x51.map{ case (x52, x53) => 
   val x54 = x52.o_orderkey 
val x55 = x53.c_name 
val x56 = Record114(x54, x55) 
x56 
} 
val resultInner__D_1 = x57
val x58 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val x64 = S__D_1.map{ case x59 => 
   val x60 = x59.s_name 
val x61 = x59.s_suppkey 
val x62 = Record115(x61) 
val x63 = Record116(x60, x62) 
x63 
} 
val M_flat1 = x64
val x65 = M_flat1
//M_flat1.collect.foreach(println(_))
val x67 = M_flat1 
val x71 = x67.map{ case x68 => 
   val x69 = x68.customers2 
val x70 = Record117(x69) 
x70 
} 
val x72 = x71.distinct 
val M_ctx2 = x72
val x73 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x75 = M_ctx2 
val x80 = L__D_1.map(x76 => { val x77 = x76.l_orderkey 
val x78 = x76.l_suppkey 
val x79 = Record118(x77, x78) 
x79 }) 
val x86 = { val out1 = x75.map{ case x81 => ({val x83 = x81.lbl 
val x84 = x83.s__Fs_suppkey 
x84}, x81) }
  val out2 = x80.map{ case x82 => ({val x85 = x82.l_suppkey 
x85}, x82) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x88 = resultInner__D_1 
val x94 = { val out1 = x86.map{ case (x89, x90) => ({val x92 = x90.l_orderkey 
x92}, (x89, x90)) }
  val out2 = x88.map{ case x91 => ({val x93 = x91.o_orderkey 
x93}, x91) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x103 = x94.flatMap{ case ((x95, x96), x97) => val x102 = (x96,x97) 
x102 match {
   case (_,null) => Nil 
   case x101 => List(({val x98 = (x95) 
x98}, {val x99 = x97.c_name 
val x100 = Record120(x99) 
x100}))
 }
}.groupByLabel() 
val x108 = x103.map{ case (x104, x105) => 
   val x106 = x104.lbl 
val x107 = (x106, x105) 
x107 
} 
val M_flat2 = x108
val x109 = M_flat2
//M_flat2.collect.foreach(println(_))
val Query2__D_1 = M_flat1
Query2__D_1.cache
Query2__D_1.count
val Query2__D_2customers2_1 = M_flat2
Query2__D_2customers2_1.cache
Query2__D_2customers2_1.count
def f = { 
 val x146 = () 
val x147 = Record111(x146) 
val x148 = List(x147) 
val M_ctx1 = x148
val x149 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x151 = Query2__D_1 
val x155 = { val out1 = x151.map{ case x152 => ({val x154 = x152.customers2 
x154}, x152) }
  val out2 = Query2__D_2customers2_1.flatMapValues(identity)
  out2.lookupSkewLeft(out1)
} 
val x161 = x155.map{ case (x157, x156) => 
   val x158 = x157.c_name2 
val x159 = x156.s_name 
val x160 = Record202(x158, x159) 
x160 
} 
val cflat__D_1 = x161
val x162 = cflat__D_1
//cflat__D_1.collect.foreach(println(_))
val x167 = C__D_1.map{ case x163 => 
   val x164 = x163.c_name 
val x165 = Record203(x164) 
val x166 = Record204(x164, x165) 
x166 
} 
val M_flat1 = x167
val x168 = M_flat1
//M_flat1.collect.foreach(println(_))
val x170 = M_flat1 
val x174 = x170.map{ case x171 => 
   val x172 = x171.suppliers 
val x173 = Record205(x172) 
x173 
} 
val x175 = x174.distinct 
val M_ctx2 = x175
val x176 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x178 = M_ctx2 
val x180 = cflat__D_1 
val x186 = { val out1 = x178.map{ case x181 => ({val x183 = x181.lbl 
val x184 = x183.c__Fc_name 
x184}, x181) }
  val out2 = x180.map{ case x182 => ({val x185 = x182.c_name 
x185}, x182) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x194 = x186.flatMap{ case (x187, x188) => val x193 = (x188) 
x193 match {
   case (null) => Nil 
   case x192 => List(({val x189 = (x187) 
x189}, {val x190 = x188.s_name 
val x191 = Record207(x190) 
x191}))
 }
}.groupByLabel() 
val x199 = x194.map{ case (x195, x196) => 
   val x197 = x195.lbl 
val x198 = (x197, x196) 
x198 
} 
val M_flat2 = x199
val x200 = M_flat2
//M_flat2.collect.foreach(println(_))
x200.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery6SparkDomains"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
