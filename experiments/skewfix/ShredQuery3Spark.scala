
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record165(lbl: Unit)
case class Record166(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record167(p_name: String, p_partkey: Int)
case class Record168(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record169(c_name: String, c_custkey: Int)
case class Record170(c__Fc_custkey: Int)
case class Record171(c_name: String, c_orders: Record170)
case class Record172(lbl: Record170)
case class Record173(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record175(o__Fo_orderkey: Int)
case class Record176(o_orderdate: String, o_parts: Record175)
case class Record177(lbl: Record175)
case class Record179(p_name: String, l_qty: Double)
case class Record244(p_retailprice: Double, p_name: String)
case class Record246(c_name: String, p_name: String)
object ShredQuery3Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery3Spark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()

   val tpch = TPCHLoader(spark)
val L__F = 3
val L__D_1 = tpch.loadLineitem
L__D_1.cache
spark.sparkContext.runJob(L__D_1, (iter: Iterator[_]) => {})
val P__F = 4
val P__D_1 = tpch.loadPart
P__D_1.cache
spark.sparkContext.runJob(P__D_1, (iter: Iterator[_]) => {})
val C__F = 1
val C__D_1 = tpch.loadCustomers
C__D_1.cache
spark.sparkContext.runJob(C__D_1, (iter: Iterator[_]) => {})
val O__F = 2
val O__D_1 = tpch.loadOrders
O__D_1.cache
spark.sparkContext.runJob(O__D_1, (iter: Iterator[_]) => {})

   val x47 = () 
val x48 = Record165(x47) 
val x49 = List(x48) 
val ljp_ctx1 = x49
val x50 = ljp_ctx1
//ljp_ctx1.collect.foreach(println(_))
val x56 = L__D_1.map(x51 => { val x52 = x51.l_orderkey 
val x53 = x51.l_quantity 
val x54 = x51.l_partkey 
val x55 = Record166(x52, x53, x54) 
x55 }) 
val x61 = P__D_1.map(x57 => { val x58 = x57.p_name 
val x59 = x57.p_partkey 
val x60 = Record167(x58, x59) 
x60 }) 
val x66 = { val out1 = x56.map{ case x62 => ({val x64 = x62.l_partkey 
x64}, x62) }
  val out2 = x61.map{ case x63 => ({val x65 = x63.p_partkey 
x65}, x63) }
  out1.lookupSkewLeft(out2)
} 
val x73 = x66.map{ case (x67, x68) => 
   val x69 = x67.l_orderkey 
val x70 = x68.p_name 
val x71 = x67.l_quantity 
val x72 = Record168(x69, x70, x71) 
x72 
} 
val ljp__D_1 = x73
val x74 = ljp__D_1
//ljp__D_1.collect.foreach(println(_))
val M_ctx1 = x49
val x75 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x80 = C__D_1.map(x76 => { val x77 = x76.c_name 
val x78 = x76.c_custkey 
val x79 = Record169(x77, x78) 
x79 }) 
val x86 = x80.map{ case x81 => 
   val x82 = x81.c_name 
val x83 = x81.c_custkey 
val x84 = Record170(x83) 
val x85 = Record171(x82, x84) 
x85 
} 
val M__D_1 = x86
val x87 = M__D_1
//M__D_1.collect.foreach(println(_))
val x89 = M__D_1 
val x93 = x89.map{ case x90 => 
   val x91 = x90.c_orders 
val x92 = Record172(x91) 
x92 
} 
val x94 = x93.distinct 
val M_ctx2 = x94
val x95 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x97 = M_ctx2 
val x103 = O__D_1.map(x98 => { val x99 = x98.o_orderdate 
val x100 = x98.o_orderkey 
val x101 = x98.o_custkey 
val x102 = Record173(x99, x100, x101) 
x102 }) 
val x109 = { val out1 = x97.map{ case x104 => ({val x106 = x104.lbl 
val x107 = x106.c__Fc_custkey 
x107}, x104) }
  val out2 = x103.map{ case x105 => ({val x108 = x105.o_custkey 
x108}, x105) }
  out2.lookupSkewLeft(out1)
} 
val x119 = x109.flatMap{ case (x111, x110) => val x118 = (x111) 
x118 match {
   case (null) => Nil 
   case x117 => List(({val x112 = (x110) 
x112}, {val x113 = x111.o_orderdate 
val x114 = x111.o_orderkey 
val x115 = Record175(x114) 
val x116 = Record176(x113, x115) 
x116}))
 }
}.groupByLabel() 
val x124 = x119.map{ case (x120, x121) => 
   val x122 = x120.lbl 
val x123 = (x122, x121) 
x123 
} 
val M__D_2 = x124
val x125 = M__D_2
//M__D_2.collect.foreach(println(_))
val x127 = M__D_2 
val x131 = x127.flatMap{ 
 case x128 => {val x129 = x128._2 
x129}.map{ case v2 => (x128._1, v2) }
}
         
val x136 = x131.map{ case (x132, x133) => 
   val x134 = x133.o_parts 
val x135 = Record177(x134) 
x135 
} 
val x137 = x136.distinct 
val M_ctx3 = x137
val x138 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x140 = M_ctx3 
val x142 = ljp__D_1 
val x148 = { val out1 = x140.map{ case x143 => ({val x145 = x143.lbl 
val x146 = x145.o__Fo_orderkey 
x146}, x143) }
  val out2 = x142.map{ case x144 => ({val x147 = x144.l_orderkey 
x147}, x144) }
  out2.lookupSkewLeft(out1)
} 
val x157 = x148.flatMap{ case (x150, x149) => val x156 = (x150) 
x156 match {
   case (null) => Nil 
   case x155 => List(({val x151 = (x149) 
x151}, {val x152 = x150.p_name 
val x153 = x150.l_qty 
val x154 = Record179(x152, x153) 
x154}))
 }
}.groupByLabel() 
val x162 = x157.map{ case (x158, x159) => 
   val x160 = x158.lbl 
val x161 = (x160, x159) 
x161 
} 
val M__D_3 = x162
val x163 = M__D_3
//M__D_3.collect.foreach(println(_))
val Query1__D_1 = M__D_1
Query1__D_1.cache
spark.sparkContext.runJob(Query1__D_1, (iter: Iterator[_]) => {})
val Query1__D_2c_orders_1 = M__D_2
Query1__D_2c_orders_1.cache
spark.sparkContext.runJob(Query1__D_2c_orders_1, (iter: Iterator[_]) => {})
val Query1__D_2c_orders_2o_parts_1 = M__D_3
Query1__D_2c_orders_2o_parts_1.cache
spark.sparkContext.runJob(Query1__D_2c_orders_2o_parts_1, (iter: Iterator[_]) => {})

 def f = {
 
var start0 = System.currentTimeMillis()
val x202 = () 
val x203 = Record165(x202) 
val x204 = List(x203) 
val M_ctx1 = x204
val x205 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x207 = Query1__D_1 
val x211 = { val out1 = x207.map{ case x208 => ({val x210 = x208.c_orders 
x210}, x208) }
Query1__D_2c_orders_1.flatMapValues(identity).lookupSkewLeft(out1)
}
         
val x216 = { val out1 = x211.map{ case (x213, x212) => ({val x215 = x213.o_parts 
x215}, (x212, x213)) }
Query1__D_2c_orders_2o_parts_1.flatMapValues(identity).lookupSkewLeft(out1)
}
         
val x221 = P__D_1.map(x217 => { val x218 = x217.p_retailprice 
val x219 = x217.p_name 
val x220 = Record244(x218, x219) 
x220 }) 
val x228 = { val out1 = x216.map{ case (x224, (x222, x223)) => ({val x226 = x224.p_name 
x226}, ((x222, x223), x224)) }
  val out2 = x221.map{ case x225 => ({val x227 = x225.p_name 
x227}, x225) }
  out1.lookupSkewLeft(out2)
} 
val x241 = x228.flatMap{ case (((x229, x230), x231), x232) => val x240 = (x229,x232) 
x240 match {
   case (_,null) => Nil
   case x239 => List(({val x233 = x229.c_name 
val x234 = x232.p_name 
val x235 = Record246(x233, x234) 
x235}, {val x236 = x231.l_qty 
val x237 = x232.p_retailprice 
val x238 = x236 * x237 
x238}))
 }
}.reduceByKey(_ + _) 
val M__D_1 = x241
val x242 = M__D_1
//M__D_1.collect.foreach(println(_))
spark.sparkContext.runJob(M__D_1, (iter: Iterator[_]) => {})
var end0 = System.currentTimeMillis() - start0
println("ShredQuery3Spark,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
println("ShredQuery3Spark,"+sf+","+Config.datapath+",0,unshredding,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("ShredQuery3Spark"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
