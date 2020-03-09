
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.PairRDDOperations._
import sprkloader.DictRDDOperations._
import sprkloader.DomainRDD._
case class Record84(c_name: String, c_custkey: Int)
case class Record85(c__Fc_custkey: Int)
case class Record86(c_name: String, c_orders: Record85)
case class Record87(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record90(o__Fo_orderkey: Int)
case class Record91(o_orderdate: String, o_parts: Record90)
case class Record92(l_partkey: Int, l_quantity: Double, l_orderkey: Int)
case class Record95(l_partkey: Int, l_qty: Double)
case class Record148(o_orderdate: String)
case class Record149(o_orderdate: String, o_parts: Iterable[Record95])
case class Record151(c_name: String)
case class Record152(c_name: String, c_orders: Iterable[Record149])
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
val x28 = C__D_1.map{ case x24 => 
   val x25 = x24.c_name 
val x26 = x24.c_custkey 
val x27 = Record84(x25, x26) 
x27
} 
val x34 = x28.map{ case x29 => 
   val x30 = x29.c_name 
val x31 = x29.c_custkey 
val x32 = Record85(x31) 
val x33 = Record86(x30, x32) 
x33
} 
val M__D_1 = x34
val x35 = M__D_1
//M__D_1.cache
//M__D_1.collect.foreach(println(_))
//spark.sparkContext.runJob(M__D_1, (iter: Iterator[_]) => {})
val x39 = M__D_1.createDomain(l => l.c_orders) 
val M_ctx2 = x39
val x40 = M_ctx2

val x42 = M_ctx2 
val x48 = O__D_1.map{ case x43 => 
   val x44 = x43.o_orderdate 
val x45 = x43.o_orderkey 
val x46 = x43.o_custkey 
val x47 = Record87(x44, x45, x46) 
x47
} 
 val x88 = x42
 val x89 = x48.map{ case x50 => (Record85({val x53 = x50.o_custkey 
x53}), {val x54 = x50.o_orderdate 
val x55 = x50.o_orderkey 
val x56 = Record90(x55) 
val x57 = Record91(x54, x56) 
x57}) }
 val x58 = x89.cogroupDomain(x88)
 val M__D_2 = x58
val x59 = M__D_2
//M__D_2.cache
//M__D_2.collect.foreach(println(_))
//spark.sparkContext.runJob(M__D_2, (iter: Iterator[_]) => {})
val x63 = M__D_2.createDomain(l => l.o_parts) 
val M_ctx3 = x63
val x64 = M_ctx3

val x66 = M_ctx3 
val x72 = L__D_1.map{ case x67 => 
   val x68 = x67.l_partkey 
val x69 = x67.l_quantity 
val x70 = x67.l_orderkey 
val x71 = Record92(x68, x69, x70) 
x71
} 
 val x93 = x66
 val x94 = x72.map{ case x74 => (Record90({val x77 = x74.l_orderkey 
x77}), {val x78 = x74.l_partkey 
val x79 = x74.l_quantity 
val x80 = Record95(x78, x79) 
x80}) }
 val x81 = x94.cogroupDomain(x93)
 val M__D_3 = x81
val x82 = M__D_3
//M__D_3.cache
//M__D_3.collect.foreach(println(_))
spark.sparkContext.runJob(M__D_3, (iter: Iterator[_]) => {})
        
        
var end0 = System.currentTimeMillis() - start0
println("ShredQuery1FullSpark,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    

var start1 = System.currentTimeMillis()
val x117 = M__D_2 
val x121 = x117.flatMap{
 case x118 => {val x119 = x118._2 
x119}.map{case x120 => 
   (x120.o_parts, (x118._1, Record148(x120.o_orderdate)))}
}
val x126 = M__D_3.rightCoGroupDropKey(x121)
val x133 = x126.map{ case ((x127, x128), x129) => 
   val x130 = x128.o_orderdate 
val x131 = Record149(x130, x129) 
val x132 = (x127, x131) 
x132
} 
val newM__D_2 = x133
val x134 = newM__D_2
//newM__D_2.cache
//newM__D_2.collect.foreach(println(_))
//spark.sparkContext.runJob(newM__D_2, (iter: Iterator[_]) => {})
val x136 = M__D_1 
val x150 = x136.map{ case x137 => (x137.c_orders, Record151(x137.c_name))}
val x140 = x150.cogroupDropKey(newM__D_2)
val x145 = x140.map{ case (x141, x142) => 
   val x143 = x141.c_name 
val x144 = Record152(x143, x142) 
x144
} 
val newM__D_1 = x145
val x146 = newM__D_1
//newM__D_1.cache
newM__D_1.collect.foreach(println(_))
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
