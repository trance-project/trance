
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record23(c__Fc_custkey: Int)
case class Record24(c_name: String, corders: Record23)
case class Record25(o_orderkey: Int, o_orderdate: String)
case class Record87(customer__Fcorders: Record23)
case class Record88(c_name: String, partqty: Record87)
case class Record89(l_quantity: Double, l_orderkey: Int, l_partkey: Int)
case class Record90(p_name: String, p_partkey: Int)
case class Record92(orderdate: String, pname: String)
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
val x47 = Record87(x46) 
val x48 = Record88(x45, x47) 
x48 
} 
val M_flat1 = x49
val x50 = M_flat1
M_flat1.count
//M_flat1.collect.foreach(println(_))
val x56 = L__D_1.map(x51 => { val x52 = x51.l_quantity 
val x53 = x51.l_orderkey 
val x54 = x51.l_partkey 
val x55 = Record89(x52, x53, x54) 
x55 }) 
val x61 = P__D_1.map(x57 => { val x58 = x57.p_name 
val x59 = x57.p_partkey 
val x60 = Record90(x58, x59) 
x60 }) 
val x66 = { val out1 = x56.map{ case x62 => ({val x64 = x62.l_partkey 
x64}, x62) }
  val out2 = x61.map{ case x63 => ({val x65 = x63.p_partkey 
x65}, x63) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x67 = CustOrders__D_2corders_1
val x68 = x67
val x74 = { val out1 = x66.map{ case (x69, x70) => ({val x73 = x69.l_orderkey 
x73}, (x69, x70)) }
  val out2 = x68
        /** WHEN DOES THIS CASE HAPPEN **/
        .flatMap(v2 => v2._2.map{case x71 => ({val x72 = x71.o_orderkey 
x72}, x71)})
  out1.lookup(out2)
} 
val x84 = x74.flatMap{ case ((x75, x76), x77) => val x83 = (x77,x76) 
x83 match {
   case (_,null) => Nil
   case x82 => List(({val x78 = x77.o_orderdate 
val x79 = x76.p_name 
val x80 = Record92(x78, x79) 
x80}, {val x81 = x75.l_quantity 
x81}))
 }
}.reduceByKey(_ + _) 
val M_flat2 = x84
val x85 = M_flat2
//M_flat2.collect.foreach(println(_))
x85.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery4SparkNestedToNested"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
