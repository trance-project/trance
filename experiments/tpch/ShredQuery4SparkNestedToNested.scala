
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record23(c__Fc_custkey: Int)
case class Record24(c_name: String, corders: Record23)
case class Record25(o_orderkey: Int, o_orderdate: String)
case class Record80(customer__Fcorders: Record23)
case class Record81(c_name: String, partqty: Record80)
case class Record83(orderdate: String, pname: String)
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
 val x49 = CustOrders__D_1.map{ case x44 => 
   val x45 = x44.c_name 
val x46 = x44.corders 
val x47 = Record80(x46) 
val x48 = Record81(x45, x47) 
x48 
} 
val M_flat1 = x49
val x50 = M_flat1
//M_flat1.collect.foreach(println(_))
val x52 = L__D_1 
val x54 = P__D_1 
val x59 = { val out1 = x52.map{ case x55 => ({val x57 = x55.l_partkey 
x57}, x55) }
  val out2 = x54.map{ case x56 => ({val x58 = x56.p_partkey 
x58}, x56) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x60 = CustOrders__D_2corders_1 
val x61 = x60
val x67 = { val out1 = x59.map{ case (x62, x63) => (({val x66 = x62.l_orderkey 
x66}), (x62, x63)) }
  val out2 = x61
        /** WHEN DOES THIS CASE HAPPEN **/
        .flatMap(v2 => v2._2.map{case x64 => ({val x65 = x64.o_orderkey 
x65}, x64)})
  out1.lookup(out2)
} 
val x77 = x67.flatMap{ case ((x68, x69), x70) => val x76 = (x70,x69) 
x76 match {
   case (_,null) => Nil
   case x75 => List(({val x71 = x70.o_orderdate 
val x72 = x69.p_name 
val x73 = Record83(x71, x72) 
x73}, {val x74 = x68.l_quantity 
x74}))
 }
}.reduceByKey(_ + _) 
val M_flat2 = x77
val x78 = M_flat2
//M_flat2.collect.foreach(println(_))
x78.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery4SparkNestedToNested"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
