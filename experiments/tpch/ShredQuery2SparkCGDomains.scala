
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record205(lbl: Unit)
case class Record206(l_suppkey: Int, l_orderkey: Int)
case class Record207(o_custkey: Int, o_orderkey: Int)
case class Record208(c_name: String, c_custkey: Int)
case class Record209(l_suppkey: Int, c_name: String)
case class Record210(s__Fs_suppkey: Int)
case class Record211(s_name: String, customers2: Record210)
case class Record212(lbl: Record210)
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
val x135 = L__D_1.map(x131 => { val x132 = x131.l_suppkey 
val x133 = x131.l_orderkey 
val x134 = Record206(x132, x133) 
x134 }) 
val x140 = O__D_1.map(x136 => { val x137 = x136.o_custkey 
val x138 = x136.o_orderkey 
val x139 = Record207(x137, x138) 
x139 }) 
val x145 = { val out1 = x135.map{ case x141 => ({val x143 = x141.l_orderkey 
x143}, x141) }
  val out2 = x140.map{ case x142 => ({val x144 = x142.o_orderkey 
x144}, x142) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x150 = C__D_1.map(x146 => { val x147 = x146.c_name 
val x148 = x146.c_custkey 
val x149 = Record208(x147, x148) 
x149 }) 
val x156 = { val out1 = x145.map{ case (x151, x152) => ({val x154 = x152.o_custkey 
x154}, (x151, x152)) }
  val out2 = x150.map{ case x153 => ({val x155 = x153.c_custkey 
x155}, x153) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x163 = x156.map{ case ((x157, x158), x159) => 
   val x160 = x157.l_suppkey 
val x161 = x159.c_name 
val x162 = Record209(x160, x161) 
x162 
} 
val resultInner__D_1 = x163
val x164 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val x170 = S__D_1.map{ case x165 => 
   val x166 = x165.s_name 
val x167 = x165.s_suppkey 
val x168 = Record210(x167) 
val x169 = Record211(x166, x168) 
x169 
} 
val M_flat1 = x170
val x171 = M_flat1
//M_flat1.collect.foreach(println(_))
val x173 = M_flat1 
val x177 = x173.map{ case x174 => 
   val x175 = x174.customers2 
val x176 = Record212(x175) 
x176 
} 
val x178 = x177.distinct 
val M_ctx2 = x178
val x179 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x181 = M_ctx2 
val x183 = resultInner__D_1 
val x189 = { val out1 = x181.map{ case x184 => ({val x186 = x184.lbl 
val x187 = x186.s__Fs_suppkey 
x187}, x184) }
  val out2 = x183.map{ case x185 => ({val x188 = x185.l_suppkey 
x188}, x185) }
  //out1.join(out2).map{ case (k,v) => v }
  out1.cogroup(out2)
}
val x197 = x189.flatMap{ case (_, (sk, cs)) => sk.map(s => (s, cs.map(c => c.c_name))) }.groupByLabel() 
val M_flat2 = x197
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
