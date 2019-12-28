
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record112(lbl: Unit)
case class Record113(o_orderkey: Int, o_custkey: Int)
case class Record114(c_name: String, c_custkey: Int)
case class Record115(o_orderkey: Int, c_name: String)
case class Record116(s__Fs_suppkey: Int)
case class Record117(s_name: String, customers2: Record116)
case class Record118(lbl: Record116)
case class Record119(l_orderkey: Int, l_suppkey: Int)
case class Record121(c_name2: String)
case class Record143(s_name: String, customers2: Iterable[Record121])
object ShredQuery2Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery2Spark"+sf)
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
val x33 = () 
val x34 = Record112(x33) 
val x35 = List(x34) 
val resultInner_ctx1 = x35
val x36 = resultInner_ctx1
//resultInner_ctx1.collect.foreach(println(_))
val x41 = O__D_1.map(x37 => { val x38 = x37.o_orderkey 
val x39 = x37.o_custkey 
val x40 = Record113(x38, x39) 
x40 }) 
val x46 = C__D_1.map(x42 => { val x43 = x42.c_name 
val x44 = x42.c_custkey 
val x45 = Record114(x43, x44) 
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
val x56 = Record115(x54, x55) 
x56 
} 
val resultInner__D_1 = x57
val x58 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val M_ctx1 = x35
val x59 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x65 = S__D_1.map{ case x60 => 
   val x61 = x60.s_name 
val x62 = x60.s_suppkey 
val x63 = Record116(x62) 
val x64 = Record117(x61, x63) 
x64 
} 
val M__D_1 = x65
val x66 = M__D_1
//M__D_1.collect.foreach(println(_))
val x68 = M__D_1 
val x72 = x68.map{ case x69 => 
   val x70 = x69.customers2 
val x71 = Record118(x70) 
x71 
} 
val x73 = x72.distinct 
val customers2_ctx1 = x73
val x74 = customers2_ctx1
//customers2_ctx1.collect.foreach(println(_))
val x76 = customers2_ctx1 
val x81 = L__D_1.map(x77 => { val x78 = x77.l_orderkey 
val x79 = x77.l_suppkey 
val x80 = Record119(x78, x79) 
x80 }) 
val x87 = { val out1 = x76.map{ case x82 => ({val x84 = x82.lbl 
val x85 = x84.s__Fs_suppkey 
x85}, x82) }
  val out2 = x81.map{ case x83 => ({val x86 = x83.l_suppkey 
x86}, x83) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x89 = resultInner__D_1 
val x95 = { val out1 = x87.map{ case (x90, x91) => ({val x93 = x91.l_orderkey 
x93}, (x90, x91)) }
  val out2 = x89.map{ case x92 => ({val x94 = x92.o_orderkey 
x94}, x92) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x104 = x95.flatMap{ case ((x96, x97), x98) => val x103 = (x97,x98) 
x103 match {
   case (_,null) => Nil 
   case x102 => List(({val x99 = (x96) 
x99}, {val x100 = x98.c_name 
val x101 = Record121(x100) 
x101}))
 }
}.groupByLabel() 
val x109 = x104.map{ case (x105, x106) => 
   val x107 = x105.lbl 
val x108 = (x107, x106) 
x108 
} 
val customers2__D_1 = x109
val x110 = customers2__D_1
//customers2__D_1.collect.foreach(println(_))
x110.count
var end0 = System.currentTimeMillis() - start0
println("ShredQuery2Spark,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    

var start1 = System.currentTimeMillis()
val x131 = M__D_1 
val x135 = { val out1 = x131.map{ case x132 => ({val x134 = x132.customers2 
x134}, x132) }
out1.cogroup(customers2__D_1).flatMap{
 case (_, (left, x133)) => left.map{ case x132 => (x132, x133.flatten) }}
}
         
val x140 = x135.map{ case (x136, x137) => 
   val x138 = x136.s_name 
val x139 = Record143(x138, x137) 
x139 
} 
val newM__D_1 = x140
val x141 = newM__D_1
//newM__D_1.collect.foreach(println(_))
x141.count
var end1 = System.currentTimeMillis() - start1
println("ShredQuery2Spark,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("ShredQuery2Spark"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
