
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
import sprkloader.UtilPairRDD._
//import sprkloader.DomainRDD._
//import sprkloader.SkewDictRDD._
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

object DictPartitionExplore {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("DictPartitionExplore"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)

val L__F = 3
val L__D_1 = tpch.loadLineitemProj
L__D_1.cache
spark.sparkContext.runJob(L__D_1, (iter: Iterator[_]) => {})
val P__F = 4
val P__D_1 = tpch.loadPartProj
P__D_1.cache
spark.sparkContext.runJob(P__D_1, (iter: Iterator[_]) => {})
/**val C__F = 1
val C__D_1 = tpch.loadCustomersProj.rekeyByPartition()
C__D_1.cache
spark.sparkContext.runJob(C__D_1, (iter: Iterator[_]) => {})
val O__F = 2
val O__D_1 = tpch.loadOrdersProjBzip.rekeyByPartition()
O__D_1.cache
spark.sparkContext.runJob(O__D_1, (iter: Iterator[_]) => {})**/

val L__ID = L__D_1.rekeyByPartition()
L__ID.cache
spark.sparkContext.runJob(L__ID, (iter: Iterator[_]) => {})

val P__ID = P__D_1.rekeyByPartition()
P__ID.cache
spark.sparkContext.runJob(P__ID, (iter: Iterator[_]) => {})

//tpch.triggerGC

	def f = {
 
var start0 = System.currentTimeMillis()

val x56 = L__ID
val x61 = P__ID

val x66_out1 = x56.indexKeyBy(l => l.l_partkey)
val x66 =  x66_out1.joinIndexed(x61, (l: PartProj) => l.p_partkey)
x66.count
var end0 = System.currentTimeMillis() - start0
println("DictPartitionExplore,"+sf+","+Config.datapath+","+end0+",idjoin,"+spark.sparkContext.applicationId)

var start1 = System.currentTimeMillis()
val x67 = L__D_1
val x68 = P__D_1

val x69 = x67.map(l => (l.l_partkey, l))
val x70 = x69.joinSkewLeft(x68, (p: PartProj) => p.p_partkey)
x70.count
var end1 = System.currentTimeMillis() - start1
println("DictPartitionExplore,"+sf+","+Config.datapath+","+end1+",join,"+spark.sparkContext.applicationId)

/**val x73 = x66.map{ case (x67, x68) => 
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

val M_ctx2 = x89.createDomain(l => Record172(l.c_orders)).distinct
val x95 = M_ctx2

val x97 = M_ctx2 
val x103 = O__D_1
val x109_out2 = x103.map{ case x105 => ({val x108 = x105.o_custkey 
x108}, x105) }

val x109 = x109_out2.joinSkewLeft(x97, (l: Record172) => l.lbl.c__Fc_custkey)
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

val x137 = M__D_2.createDomain(v => Record177(v.o_parts)).distinct
val M_ctx3 = x137
val x138 = M_ctx3

val x140 = M_ctx3 
val x142 = ljp__D_1 
val x148_out2 = x142.map{ case x144 => ({val x147 = x144.l_orderkey 
x147}, x144) }
val x148 = x148_out2.joinSkewLeft(x140, (l: Record177) => l.lbl.o__Fo_orderkey)

val x157 = x148.map{ case (x150, x149) =>
  ({val x151 = (x149) 
x151.lbl}, {val x152 = x150.p_name 
val x153 = x150.l_qty 
val x154 = Record179(x152, x153) 
x154})
}.groupByLabel() 
val M__D_3 = x157
val x163 = M__D_3

spark.sparkContext.runJob(M__D_3, (iter: Iterator[_]) => {})
var end0 = System.currentTimeMillis() - start0
println("DictPartitionExplore,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    

var start1 = System.currentTimeMillis()
val x201 = M__D_2 
val x205 = x201.flatMap{ 
 case x202 => {val x203 = x202._2 
x203}.map{ case v2 => (x202._1, v2) }
}
         
val x210 = { val out1 = x205.map{ case (x206, x207) => ({val x209 = x207.o_parts 
x209}, (x206, x207)) }
out1.cogroup(M__D_3).flatMap{
 case (_, (left, x208)) => left.map{ case (x206, x207) => ((x206, x207), x208.flatten) }}
}
         
val x217 = x210.map{ case ((x211, x212), x213) => 
   val x214 = x212.o_orderdate 
val x215 = Record232(x214, x213) 
val x216 = (x211, x215) 
x216 
} 
val newM__D_2 = x217
val x218 = newM__D_2
//newM__D_2.collect.foreach(println(_))
val x220 = M__D_1 
val x224 = { val out1 = x220.map{ case x221 => ({val x223 = x221.c_orders 
x223}, x221) }
out1.cogroup(newM__D_2).flatMap{
 case (_, (left, x222)) => left.map{ case x221 => (x221, x222) }}
}
         
val x229 = x224.map{ case (x225, x226) => 
   val x227 = x225.c_name 
val x228 = Record233(x227, x226) 
x228 
} 
val newM__D_1 = x229
val x230 = newM__D_1
newM__D_1.collect.foreach(println(_))
spark.sparkContext.runJob(x230, (iter: Iterator[_]) => {})
var end = System.currentTimeMillis() - start0
var end1 = System.currentTimeMillis() - start1
println("DictPartitionExplore,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
println("DictPartitionExplore,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)**/
    
}
f
  }
}
