
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record118(lbl: Unit)
case class Record119(o_orderkey: Int, o_custkey: Int)
case class Record120(c_name: String, c_custkey: Int)
case class Record121(o_orderkey: Int, c_name: String)
case class Record122(s_name: String, s_suppkey: Int)
case class Record123(s__Fs_suppkey: Int)
case class Record124(s_name: String, customers2: Record123)
case class Record125(lbl: Record123)
case class Record126(l_orderkey: Int, l_suppkey: Int)
case class Record128(c_name2: String)
case class Record150(s_name: String, customers2: Iterable[Record128])
object ShredQuery5Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery5Spark"+sf)
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
 
var start0 = System.currentTimeMillis()
val x34 = () 
val x35 = Record118(x34) 
val x36 = List(x35) 
val resultInner_ctx1 = x36
val x37 = resultInner_ctx1
//resultInner_ctx1.collect.foreach(println(_))
val x42 = O__D_1.map(x38 => { val x39 = x38.o_orderkey 
val x40 = x38.o_custkey 
val x41 = Record119(x39, x40) 
x41 }) 
val x47 = C__D_1.map(x43 => { val x44 = x43.c_name 
val x45 = x43.c_custkey 
val x46 = Record120(x44, x45) 
x46 }) 
val x52 = { val out1 = x42.map{ case x48 => ({val x50 = x48.o_custkey 
x50}, x48) }
  val out2 = x47.map{ case x49 => ({val x51 = x49.c_custkey 
x51}, x49) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x58 = x52.map{ case (x53, x54) => 
   val x55 = x53.o_orderkey 
val x56 = x54.c_name 
val x57 = Record121(x55, x56) 
x57 
} 
val resultInner__D_1 = x58
val x59 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val M_ctx1 = x36
val x60 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x65 = S__D_1.map(x61 => { val x62 = x61.s_name 
val x63 = x61.s_suppkey 
val x64 = Record122(x62, x63) 
x64 }) 
val x71 = x65.map{ case x66 => 
   val x67 = x66.s_name 
val x68 = x66.s_suppkey 
val x69 = Record123(x68) 
val x70 = Record124(x67, x69) 
x70 
} 
val M__D_1 = x71
val x72 = M__D_1
//M__D_1.collect.foreach(println(_))
/**val x74 = M__D_1 
val x78 = x74.map{ case x75 => 
   val x76 = x75.customers2 
val x77 = Record125(x76) 
x77 
} 
val x79 = x78.distinct 
val M_ctx2 = x79
val x80 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x82 = M_ctx2 **/
val x87 = L__D_1.map(x83 => { val x84 = x83.l_orderkey 
val x85 = x83.l_suppkey 
val x86 = Record126(x84, x85) 
x86 }) 
/**val x93 = { val out1 = x82.map{ case x88 => ({val x90 = x88.lbl 
val x91 = x90.s__Fs_suppkey 
x91}, x88) }
  val out2 = x87.map{ case x89 => ({val x92 = x89.l_suppkey 
x92}, x89) }
  out1.join(out2).map{ case (k,v) => v }
} **/
val x95 = resultInner__D_1 
val x101 = { val out1 = x87.map{ case x97 => ({val x99 = x97.l_orderkey 
x99}, x97) }
  val out2 = x95.map{ case x98 => ({val x100 = x98.o_orderkey 
x100}, x98) }
  out2.join(out1).map{ case (k,v) => v }
} 
val x110 = x101.flatMap{ case (x104, x103) => val x109 = (x103,x104) 
x109 match {
   case (_,null) => Nil 
   case x108 => List(({val x105 = Record125(Record123(x103.l_orderkey))
x105}, {val x106 = x104.c_name 
val x107 = Record128(x106) 
x107}))
 }
}.groupByLabel() 
val x115 = x110.map{ case (x111, x112) => 
   val x113 = x111.lbl 
val x114 = (x113, x112) 
x114 
} 
val M__D_2 = x115
val x116 = M__D_2
//M__D_2.collect.foreach(println(_))
x116.count
var end0 = System.currentTimeMillis() - start0
println("ShredQuery5Spark,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    

var start1 = System.currentTimeMillis()
val x138 = M__D_1 
val x142 = { val out1 = x138.map{ case x139 => ({val x141 = x139.customers2 
x141}, x139) }
out1.cogroup(M__D_2).flatMap{
 case (_, (left, x140)) => left.map{ case x139 => (x139, x140.flatten) }}
}
         
val x147 = x142.map{ case (x143, x144) => 
   val x145 = x143.s_name 
val x146 = Record150(x145, x144) 
x146 
} 
val newM__D_1 = x147
val x148 = newM__D_1
//newM__D_1.collect.foreach(println(_))
x148.count
var end1 = System.currentTimeMillis() - start1
println("ShredQuery5Spark,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("ShredQuery5Spark"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
