
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
case class Record103(customer__Fcorders: Record23)
case class Record104(c_name: String, partqty: Record103)
case class Record106(orderdate: String, pname: String)
object ShredQuery4SparkNestedToNested {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4SparkNestedToNested"+sf)
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
 val x52 = L__D_1.map(x47 => { val x48 = x47.l_orderkey 
val x49 = x47.l_quantity 
val x50 = x47.l_partkey 
val x51 = Record100(x48, x49, x50) 
x51 }) 
val x57 = P__D_1.map(x53 => { val x54 = x53.p_name 
val x55 = x53.p_partkey 
val x56 = Record101(x54, x55) 
x56 }) 
val x62 = { val out1 = x52.map{ case x58 => ({val x60 = x58.l_partkey 
x60}, x58) }
  val out2 = x57.map{ case x59 => ({val x61 = x59.p_partkey 
x61}, x59) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x69 = x62.map{ case (x63, x64) => 
   val x65 = x63.l_orderkey 
val x66 = x64.p_name 
val x67 = x63.l_quantity 
val x68 = Record102(x65, x66, x67) 
x68 
} 
val parts__D_1 = x69
val x70 = parts__D_1
//parts__D_1.collect.foreach(println(_))
val x76 = CustOrders__D_1.map{ case x71 => 
   val x72 = x71.c_name 
val x73 = x71.corders 
val x74 = Record103(x73) 
val x75 = Record104(x72, x74) 
x75 
} 
val M_flat1 = x76
val x77 = M_flat1
//M_flat1.collect.foreach(println(_))
val x78 = CustOrders__D_2corders_1 
val x79 = x78 
val x81 = x79 
val x83 = parts__D_1 
val x88 = { val out1 = x81.flatMap{ case (x82, v) => v.map{ case x84 => ({val x86 = x84.o_orderkey 
x86}, x84) }}
  val out2 = x83.map{ case x85 => ({val x87 = x85.l_orderkey 
x87}, x85) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x97 = x88.flatMap{ case (x89, x90) => val x96 = (x89,x90) 
x96 match {
   case (_,null) => Nil
   case x95 => List(({val x91 = x89.o_orderdate 
val x92 = x90.p_name 
val x93 = Record106(x91, x92) 
x93}, {val x94 = x90.l_qty 
x94}))
 }
}.reduceByKey(_ + _) 
val M_flat2 = x97
val x98 = M_flat2
//M_flat2.collect.foreach(println(_))
x98.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery4SparkNestedToNested"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
