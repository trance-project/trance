
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record182(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record183(p_name: String, p_partkey: Int)
case class Record184(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record185(c__Fc_custkey: Int)
case class Record186(c_name: String, c_orders: Record185)
case class Record187(o__Fo_orderkey: Int)
case class Record188(o_orderdate: String, o_parts: Record187)
case class Record189(p_name: String, l_qty: Double)
object ShredQuery1SparkOptWUS {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1SparkOptWUS"+sf)
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

   def f = { 
var start0 = System.currentTimeMillis()

 val x134 = L__D_1.map(x129 => { val x130 = x129.l_orderkey 
val x131 = x129.l_quantity 
val x132 = x129.l_partkey 
val x133 = Record182(x130, x131, x132) 
x133 }) 
val x139 = P__D_1.map(x135 => { val x136 = x135.p_name 
val x137 = x135.p_partkey 
val x138 = Record183(x136, x137) 
x138 }) 
val x144 = { val out1 = x134.map{ case x140 => ({val x142 = x140.l_partkey 
x142}, x140) }
  val out2 = x139.map{ case x141 => ({val x143 = x141.p_partkey 
x143}, x141) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x151 = x144.map{ case (x145, x146) => 
   val x147 = x145.l_orderkey 
val x148 = x146.p_name 
val x149 = x145.l_quantity 
val x150 = Record184(x147, x148, x149) 
x150 
} 
val ljp__D_1 = x151
val x152 = ljp__D_1
//ljp__D_1.collect.foreach(println(_))
val x158 = C__D_1.map{ case x153 => 
   val x154 = x153.c_name 
val x155 = x153.c_custkey 
val x156 = Record185(x155) 
val x157 = Record186(x154, x156) 
x157 
} 
val M_flat1 = x158
val x159 = M_flat1
M_flat1.count
//M_flat1.collect.foreach(println(_))
val x169 = O__D_1.map{ case x160 => 
   val x161 = x160.o_custkey 
val x162 = Record185(x161) 
val x163 = x160.o_orderdate 
val x164 = x160.o_orderkey 
val x165 = Record187(x164) 
val x166 = Record188(x163, x165) 
val x167 = x166//List(x166) 
val x168 = (x162, x167) 
x168 
}.groupByLabel() 
val M_flat2 = x169
val x170 = M_flat2
M_flat2.count
//M_flat2.collect.foreach(println(_))
val x179 = ljp__D_1.map{ case x171 => 
   val x172 = x171.l_orderkey 
val x173 = Record187(x172) 
val x174 = x171.p_name 
val x175 = x171.l_qty 
val x176 = Record189(x174, x175) 
val x177 = x176//List(x176) 
val x178 = (x173, x177) 
x178 
}.groupByLabel() 
val M_flat3 = x179
val x180 = M_flat3
//M_flat3.collect.foreach(println(_))
x180.count
var end0 = System.currentTimeMillis() - start0 
println("ShredQuery1SparkOptWUS"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
var start1 = System.currentTimeMillis()

//val firstlevel = M_flat2.flatMap(m => m._2.map(v => (v.o_parts, (v.o_orderdate, m._1)))).lookup(M_flat3.flatMapValues(identity)).groupByLabel().map{ case ((od, lbl), parts) => lbl -> (od, parts) }
val firstlevel = M_flat2.flatMap(m => m._2.map(v => (v.o_parts, (v.o_orderdate, m._1)))).cogroup(M_flat3).flatMap{
  case (_, (odates, parts)) => odates.map{ case (od, lbl) => (lbl, (od, parts)) }
}
val secondlevel = M_flat1.map(m => (m.c_orders, m.c_name)).cogroup(firstlevel).flatMap{
  case (_, (c, o)) => c.map(c2 => (c2, o))
}
secondlevel.count
var end1 = System.currentTimeMillis() - start1
println("ShredQuery1SparkOptWUS"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)

}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
   println("ShredQuery1SparkOptWUS"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
