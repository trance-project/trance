
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record80(o_orderkey: Int, o_custkey: Int)
case class Record81(c_name: String, c_custkey: Int)
case class Record82(o_orderkey: Int, c_name: String)
case class Record83(s__Fs_suppkey: Int)
case class Record84(s_name: String, customers2: Record83)
case class Record85(l_suppkey: Int, l_orderkey: Int)
case class Record87(c_name2: String)
case class Record120(c__Fc_name: String)
case class Record121(c_name: String, suppliers: Record120)
case class Record122(s_name: String)
object ShredQuery6SparkOpt {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6SparkOpt"+sf)
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

   val x27 = O__D_1.map(x23 => { val x24 = x23.o_orderkey 
val x25 = x23.o_custkey 
val x26 = Record80(x24, x25) 
x26 }) 
val x32 = C__D_1.map(x28 => { val x29 = x28.c_name 
val x30 = x28.c_custkey 
val x31 = Record81(x29, x30) 
x31 }) 
val x37 = { val out1 = x27.map{ case x33 => ({val x35 = x33.o_custkey 
x35}, x33) }
  val out2 = x32.map{ case x34 => ({val x36 = x34.c_custkey 
x36}, x34) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x43 = x37.map{ case (x38, x39) => 
   val x40 = x38.o_orderkey 
val x41 = x39.c_name 
val x42 = Record82(x40, x41) 
x42 
} 
val resultInner__D_1 = x43
val x44 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val x50 = S__D_1.map{ case x45 => 
   val x46 = x45.s_name 
val x47 = x45.s_suppkey 
val x48 = Record83(x47) 
val x49 = Record84(x46, x48) 
x49 
} 
val M_flat1 = x50
val x51 = M_flat1
//M_flat1.collect.foreach(println(_))
val x56 = L__D_1.map(x52 => { val x53 = x52.l_suppkey 
val x54 = x52.l_orderkey 
val x55 = Record85(x53, x54) 
x55 }) 
val x58 = resultInner__D_1 
val x63 = { val out1 = x56.map{ case x59 => ({val x61 = x59.l_orderkey 
x61}, x59) }
  val out2 = x58.map{ case x60 => ({val x62 = x60.o_orderkey 
x62}, x60) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x71 = x63.flatMap{ case (x64, x65) => val x70 = (x65) 
x70 match {
   case (null) => Nil 
   case x69 => List(({val x66 = (x64) 
x66}, {val x67 = x65.c_name 
val x68 = Record87(x67) 
x68}))
 }
}.groupByLabel() 
val x77 = x71.map{ case (x72, x73) => 
   val x74 = x72.l_suppkey 
val x75 = Record83(x74) 
val x76 = (x75, x73) 
x76 
} 
val M_flat2 = x77
val x78 = M_flat2
//M_flat2.collect.foreach(println(_))
val Query2__D_1 = M_flat1
Query2__D_1.cache
Query2__D_1.count
val Query2__D_2customers2_1 = M_flat2
Query2__D_2customers2_1.cache
Query2__D_2customers2_1.count
def f = { 
 val x101 = C__D_1.map{ case x97 => 
   val x98 = x97.c_name 
val x99 = Record120(x98) 
val x100 = Record121(x98, x99) 
x100 
} 
val M_flat1 = x101
val x102 = M_flat1
M_flat1.count
//M_flat1.collect.foreach(println(_))
val x104 = Query2__D_1 
val x108 = { val out1 = x104.map{ case x105 => ({val x107 = x105.customers2 
x107}, x105) }
  val out2 = Query2__D_2customers2_1.flatMapValues(identity)
  out2.lookupSkewLeft(out1)
} 
val x117 = x108.map{ case (x110, x109) => 
   val x111 = x110.c_name2 
val x112 = Record120(x111) 
val x113 = x109.s_name 
val x114 = Record122(x113) 
val x115 = List(x114) 
val x116 = (x112, x115) 
x116 
}.groupByLabel()
val M_flat2 = x117
val x118 = M_flat2
//M_flat2.collect.foreach(println(_))
x118.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery6SparkOpt"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
