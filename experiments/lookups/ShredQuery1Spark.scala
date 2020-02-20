
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
import sprkloader.DomainRDD._
import sprkloader.SkewDictRDD._
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
case class Record232(o_orderdate: String, o_parts: Iterable[Record179])
case class Record233(c_name: String, c_orders: Iterable[Record232])
object ShredQuery1Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1Spark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L__F = 3
val L__D_1 = tpch.loadLineitemProjBzip
L__D_1.cache
spark.sparkContext.runJob(L__D_1, (iter: Iterator[_]) => {})
val P__F = 4
val P__D_1 = tpch.loadPartProj
P__D_1.cache
spark.sparkContext.runJob(P__D_1, (iter: Iterator[_]) => {})
val C__F = 1
val C__D_1 = tpch.loadCustomersProj
C__D_1.cache
spark.sparkContext.runJob(C__D_1, (iter: Iterator[_]) => {})
val O__F = 2
val O__D_1 = tpch.loadOrdersProjBzip
O__D_1.cache
spark.sparkContext.runJob(O__D_1, (iter: Iterator[_]) => {})

tpch.triggerGC

	def f = {
 
var start0 = System.currentTimeMillis()

val x56 = L__D_1
val x61 = P__D_1

val x63 = x56.map{ case x62 => ({val x64 = x62.l_partkey 
x64}, x62) }
/**val x66_out2 = x61.map{ case x63 => ({val x65 = x63.p_partkey 
x65}, x63) }**/
val x66 =  x63.joinSkew(x61, (l: PartProj) => l.p_partkey)

val x73 = x66.map{ case (x67, x68) => 
   val x69 = x67.l_orderkey 
val x70 = x68.p_name 
val x71 = x67.l_quantity 
val x72 = Record168(x69, x70, x71) 
x72 
} 
val ljp__D_1 = x73
val x74 = ljp__D_1

val x80 = C__D_1
val x86 = x80.map{ case x81 => 
   val x82 = x81.c_name 
val x83 = x81.c_custkey 
val x84 = Record170(x83) 
val x85 = Record171(x82, x84) 
x85 
} 
val M__D_1 = x86
val x87 = M__D_1
val x89 = M__D_1

val M_ctx2 = x89.createDomain(l => Record172(l.c_orders))//.distinct
val x95 = M_ctx2

val x97 = M_ctx2 
val x103 = O__D_1
/**val x109_out1 = x97.map{ case x104 => ({val x106 = x104.lbl 
val x107 = x106.c__Fc_custkey 
x107}, x104) }**/
val x107 = x103.map{ case x105 => ({val x108 = x105.o_custkey 
x108}, x105) }

val x109 = x107.joinDomainSkew(x97, (l: Record172) => l.lbl.c__Fc_custkey)
val x119 = x109.map{ case (x111, x110) =>
  ({val x112 = (x110) 
  x112.lbl}, {val x113 = x111.o_orderdate 
val x114 = x111.o_orderkey 
val x115 = Record175(x114) 
val x116 = Record176(x113, x115) 
x116})
}.groupByLabel() 
val x124 = x119
val M__D_2 = x124
val x125 = M__D_2
val x127 = M__D_2 

val x137 = M__D_2.createDomain(v => Record177(v.o_parts))//.distinct
val M_ctx3 = x137
val x138 = M_ctx3

val x140 = M_ctx3 
val x142 = ljp__D_1 
/**val x148_out1 = x140.map{ case x143 => ({val x145 = x143.lbl 
val x146 = x145.o__Fo_orderkey 
x146}, x143) }**/
val x145 = x142.map{ case x144 => ({val x147 = x144.l_orderkey 
x147}, x144) }
val x148 = x145.joinDomainSkew(x140, (l: Record177) => l.lbl.o__Fo_orderkey)

val x157 = x148.map{ case (x150, x149) =>
  ({val x151 = (x149) 
x151.lbl}, {val x152 = x150.p_name 
val x153 = x150.l_qty 
val x154 = Record179(x152, x153) 
x154})
}.groupByLabel() 
val M__D_3 = x157
val x163 = M__D_3

//spark.sparkContext.runJob(M__D_3, (iter: Iterator[_]) => {})
var end0 = System.currentTimeMillis() - start0
println("ShredQuery1Spark,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    

var start1 = System.currentTimeMillis()
/**val x201 = M__D_2.mapPartitions(
  it => it.flatMap(v => v._2.map(o => (o.o_parts, (v._1, o.o_orderdate)))), false
).cogroup(M__D_3).mapPartitions(
  it => it.flatMap{ case (_, (left, x208)) => left.map{ case (x206, x207) => (x206, (x207, x208.flatten)) }}, false
)
val result = M__D_1.map(c => c.c_orders -> c.c_name).cogroup(x201).mapPartitions(
  it => it.flatMap{ case (_, (left, x208)) => left.map( cname => cname -> x208)}, false
)
result.collect.foreach(println(_))
spark.sparkContext.runJob(result, (iter: Iterator[_]) => {})**/
var end = System.currentTimeMillis() - start0
var end1 = System.currentTimeMillis() - start1
println("ShredQuery1Spark,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
println("ShredQuery1Spark,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)
    
}
f
  }
}
