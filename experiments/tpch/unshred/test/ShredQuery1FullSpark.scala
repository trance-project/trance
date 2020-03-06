
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
import sprkloader.SkewDictRDD._
import sprkloader.DomainRDD._
case class Record96(lbl: Unit)
case class Record97(c_name: String, c_custkey: Int)
case class Record98(c__Fc_custkey: Int)
case class Record99(c_name: String, c_orders: Record98)
case class Record100(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record103(o__Fo_orderkey: Int)
case class Record104(o_orderdate: String, o_parts: Record103)
case class Record105(l_partkey: Int, l_quantity: Double, l_orderkey: Int)
case class Record108(l_partkey: Int, l_qty: Double)
case class Record144(o_orderdate: String)
case class Record146(c_name: String)
object ShredQuery1FullSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1FullSpark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L__F = 3
val L__D_1 = tpch.loadLineitem()
L__D_1.cache
L__D_1.count
val P__F = 4
val P__D_1 = tpch.loadPart()
P__D_1.cache
P__D_1.count
val C__F = 1
val C__D_1 = tpch.loadCustomers()
C__D_1.cache
C__D_1.count
val O__F = 2
val O__D_1 = tpch.loadOrders()
O__D_1.cache
O__D_1.count

    def f = {
 
var start0 = System.currentTimeMillis()
val x26 = () 
val x27 = Record96(x26) 
val x28 = List(x27) 
val M_ctx1 = x28
val x29 = M_ctx1
//M_ctx1.cache
//spark.sparkContext.runJob(M_ctx1, (iter: Iterator[_]) => {})
val x34 = C__D_1.map(x30 => { val x31 = x30.c_name 
val x32 = x30.c_custkey 
val x33 = Record97(x31, x32) 
x33 }) 
val x40 = x34.map{ case x35 => 
   val x36 = x35.c_name 
val x37 = x35.c_custkey 
val x38 = Record98(x37) 
val x39 = Record99(x36, x38) 
x39
}
       
val M__D_1 = x40
val x41 = M__D_1
//M__D_1.cache
//spark.sparkContext.runJob(M__D_1, (iter: Iterator[_]) => {})
val x43 = M__D_1 
val x47 = x43.createDomain(l => l.c_orders) 
val x48 = x47 
val M_ctx2 = x48
val x49 = M_ctx2
//M_ctx2.cache
//spark.sparkContext.runJob(M_ctx2, (iter: Iterator[_]) => {})
val x51 = M_ctx2 
val x57 = O__D_1.map(x52 => { val x53 = x52.o_orderdate 
val x54 = x52.o_orderkey 
val x55 = x52.o_custkey 
val x56 = Record100(x53, x54, x55) 
x56 }) 
 val x101 = x51
 val x102 = x57.map{ case x59 => (Record98({val x62 = x59.o_custkey 
x62}), {val x63 = x59.o_orderdate 
val x64 = x59.o_orderkey 
val x65 = Record103(x64) 
val x66 = Record104(x63, x65) 
x66})}
 val x67 = x102.cogroupDomain(x101)
 val M__D_2 = x67
val x68 = M__D_2
//M__D_2.cache
//spark.sparkContext.runJob(M__D_2, (iter: Iterator[_]) => {})
val x70 = M__D_2 
val x74 = x70.createDomain(l => l.o_parts) 
val x75 = x74 
val M_ctx3 = x75
val x76 = M_ctx3
//M_ctx3.cache
//spark.sparkContext.runJob(M_ctx3, (iter: Iterator[_]) => {})
val x78 = M_ctx3 
val x84 = L__D_1.map(x79 => { val x80 = x79.l_partkey 
val x81 = x79.l_quantity 
val x82 = x79.l_orderkey 
val x83 = Record105(x80, x81, x82) 
x83 }) 
 val x106 = x78
 val x107 = x84.map{ case x86 => (Record103({val x89 = x86.l_orderkey 
x89}), {val x90 = x86.l_partkey 
val x91 = x86.l_quantity 
val x92 = Record108(x90, x91) 
x92})}
 val x93 = x107.cogroupDomain(x106)
 val M__D_3 = x93
val x94 = M__D_3
//M__D_3.cache
spark.sparkContext.runJob(M__D_3, (iter: Iterator[_]) => {})
        
        
var end0 = System.currentTimeMillis() - start0
println("ShredQuery1FullSpark,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    

var start1 = System.currentTimeMillis()
val x125 = M__D_2 
val x129 = x125.flatMap{
 case x126 => {val x127 = x126._2 
x127}.map{case x128 => 
   (x128.o_parts, (x126._1, Record144(x128.o_orderdate)))}
}
val x134 = x129.cogroup(M__D_3).flatMap{
 case (_, (dict1, dict2)) => dict1.map{ case (lbl, d) => lbl -> (d, dict2.flatten) }
}
val newM__D_2 = x134
val x135 = newM__D_2
//newM__D_2.cache
//spark.sparkContext.runJob(newM__D_2, (iter: Iterator[_]) => {})
val x137 = M__D_1 
val x145 = x137.map{ case x138 => (x138.c_orders, Record146(x138.c_name))}
val x141 = x145.cogroup(newM__D_2).flatMap{
 case (_, (dict1, dict2)) => dict1.map{ case x138 => (x138, dict2) }}
val newM__D_1 = x141
val x142 = newM__D_1
//newM__D_1.cache
spark.sparkContext.runJob(newM__D_1, (iter: Iterator[_]) => {})


var end1 = System.currentTimeMillis() - start1
println("ShredQuery1FullSpark,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("ShredQuery1FullSpark"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
