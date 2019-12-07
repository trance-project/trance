
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record205(lbl: Unit)
case class Record206(o_orderkey: Int, o_custkey: Int)
case class Record207(c_name: String, c_custkey: Int)
case class Record208(o_orderkey: Int, c_name: String)
case class Record209(s__Fs_suppkey: Int)
case class Record210(s_name: String, customers2: Record209)
case class Record211(lbl: Record209)
case class Record212(l_orderkey: Int, l_suppkey: Int)
case class Record214(c_name2: String)
object ShredQuery2SparkCGDomains {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery2SparkCGDomains"+sf)
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

   def f = { 
 val x127 = () 
val x128 = Record205(x127) 
val x129 = List(x128) 
val M_ctx1 = x129
val x130 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x135 = O__D_1.map(x131 => { val x132 = x131.o_orderkey 
val x133 = x131.o_custkey 
val x134 = Record206(x132, x133) 
x134 }) 
val x140 = C__D_1.map(x136 => { val x137 = x136.c_name 
val x138 = x136.c_custkey 
val x139 = Record207(x137, x138) 
x139 }) 
val x145 = { val out1 = x135.map{ case x141 => ({val x143 = x141.o_custkey 
x143}, x141) }
  val out2 = x140.map{ case x142 => ({val x144 = x142.c_custkey 
x144}, x142) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x151 = x145.map{ case (x146, x147) => 
   val x148 = x146.o_orderkey 
val x149 = x147.c_name 
val x150 = Record208(x148, x149) 
x150 
} 
val resultInner__D_1 = x151
val x152 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val x158 = S__D_1.map{ case x153 => 
   val x154 = x153.s_name 
val x155 = x153.s_suppkey 
val x156 = Record209(x155) 
val x157 = Record210(x154, x156) 
x157 
} 
val M_flat1 = x158
val x159 = M_flat1
//M_flat1.collect.foreach(println(_))
val x161 = M_flat1 
val x165 = x161.map{ case x162 => 
   val x163 = x162.customers2 
val x164 = Record211(x163) 
x164 
} 
val x166 = x165.distinct 
val M_ctx2 = x166
val x167 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x169 = M_ctx2 
val x174 = L__D_1.map(x170 => { val x171 = x170.l_orderkey 
val x172 = x170.l_suppkey 
val x173 = Record212(x171, x172) 
x173 }) 
val x180 = { val out1 = x169.map{ case x175 => ({val x177 = x175.lbl 
val x178 = x177.s__Fs_suppkey 
x178}, x175) }
  val out2 = x174.map{ case x176 => ({val x179 = x176.l_suppkey 
x179}, x176) }
  out1.cogroup(out2).flatMap{ case (_, (v1, v2)) => v1.map{ v => (v, v2) } }
}
val x182 = resultInner__D_1 
val x188 = { val out1 = x180.flatMap{ 
  case (x183, x184) => x184.map(v => ({val x186 = v.l_orderkey 
x186}, (x183, v))) }
  val out2 = x182.map{ case x185 => ({val x187 = x185.o_orderkey 
x187}, x185) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x197 = x188.flatMap{ case ((x189, x190), x191) => val x196 = (x190,x191) 
x196 match {
   case (_,null) => Nil 
   case x195 => List(({val x192 = (x189) 
x192}, {val x193 = x191.c_name 
val x194 = Record214(x193) 
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
x203.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery2SparkCGDomains"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
