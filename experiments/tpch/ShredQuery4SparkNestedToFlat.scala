
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record23(c__Fc_custkey: Int)
case class Record24(c_name: String, corders: Record23)
case class Record25(o_orderkey: Int, o_orderdate: String)
case class Record100(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record101(p_name: String, p_partkey: Int)
case class Record102(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record104(orderdate: String, pname: String)
object ShredQuery4SparkNestedToFlat {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4SparkNestedToFlat"+sf)
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

   val x10 = C__D_1.map{ case x5 => 
   val x6 = x5.c_name 
val x7 = x5.c_custkey 
val x8 = Record23(x7) 
val x9 = Record24(x6, x8) 
x9 
} 
val M_flat1 = x10
val x11 = M_flat1
//M_flat1.collect.foreach(println(_))
val x20 = O__D_1.map{ case x12 => 
   val x13 = x12.o_custkey 
val x14 = Record23(x13) 
val x15 = x12.o_orderkey 
val x16 = x12.o_orderdate 
val x17 = Record25(x15, x16) 
val x18 = List(x17) 
val x19 = (x14, x18) 
x19 
} 
val M_flat2 = x20
val x21 = M_flat2
//M_flat2.collect.foreach(println(_))
val CustOrders__D_1 = M_flat1
CustOrders__D_1.cache
CustOrders__D_1.count
val CustOrders__D_2corders_1 = M_flat2
CustOrders__D_2corders_1.cache
CustOrders__D_2corders_1.count
def f = { 
 val x55 = L__D_1.map(x50 => { val x51 = x50.l_orderkey 
val x52 = x50.l_quantity 
val x53 = x50.l_partkey 
val x54 = Record100(x51, x52, x53) 
x54 }) 
val x60 = P__D_1.map(x56 => { val x57 = x56.p_name 
val x58 = x56.p_partkey 
val x59 = Record101(x57, x58) 
x59 }) 
val x65 = { val out1 = x55.map{ case x61 => ({val x63 = x61.l_partkey 
x63}, x61) }
  val out2 = x60.map{ case x62 => ({val x64 = x62.p_partkey 
x64}, x62) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x72 = x65.map{ case (x66, x67) => 
   val x68 = x66.l_orderkey 
val x69 = x67.p_name 
val x70 = x66.l_quantity 
val x71 = Record102(x68, x69, x70) 
x71 
} 
val parts__D_1 = x72
val x73 = parts__D_1
//parts__D_1.collect.foreach(println(_))
val x79 = CustOrders__D_2corders_1 
/**val x79 = { val out1 = x75.map{ case x76 => ({val x78 = x76.corders 
x78}, x76) }
  val out2 = CustOrders__D_2corders_1.flatMapValues(identity)
  out1.lookup(out2)
} **/

val x81 = parts__D_1 
val x87 = { val out1 = x79.flatMap{ case (x82, x83) => x83.map{
  v => ({val x85 = v.o_orderkey 
x85}, (x82, v)) }}
  val out2 = x81.map{ case x84 => ({val x86 = x84.l_orderkey 
x86}, x84) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x97 = x87.flatMap{ case ((x88, x89), x90) => val x96 = (x89,x90) 
x96 match {
   case (_,null) => Nil
   case x95 => List(({val x91 = x89.o_orderdate 
val x92 = x90.p_name 
val x93 = Record104(x91, x92) 
x93}, {val x94 = x90.l_qty 
x94}))
 }
}.reduceByKey(_ + _) 
val M_flat1 = x97
val x98 = M_flat1
//M_flat1.collect.foreach(println(_))
x98.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery4SparkNestedToFlat"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
