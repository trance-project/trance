
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record47(l_quantity: Double, l_orderkey: Int, l_partkey: Int)
case class Record48(p_name: String, p_partkey: Int)
case class Record49(l__Fl_orderkey: Int)
case class Record50(p_name: String, l_qty: Double, orders: Record49)
case class Record51(o_custkey: Int, orderdate: String)
case class Record97(c_name: String, c_custkey: Int)
case class Record99(c_name: String, p_name: String)
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

   val x16 = L__D_1.map(x11 => { val x12 = x11.l_quantity 
val x13 = x11.l_orderkey 
val x14 = x11.l_partkey 
val x15 = Record47(x12, x13, x14) 
x15 }) 
val x21 = P__D_1.map(x17 => { val x18 = x17.p_name 
val x19 = x17.p_partkey 
val x20 = Record48(x18, x19) 
x20 }) 
val x26 = { val out1 = x16.map{ case x22 => ({val x24 = x22.l_partkey 
x24}, x22) }
  val out2 = x21.map{ case x23 => ({val x25 = x23.p_partkey 
x25}, x23) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x34 = x26.map{ case (x27, x28) => 
   val x29 = x28.p_name 
val x30 = x27.l_quantity 
val x31 = x27.l_orderkey 
val x32 = Record49(x31) 
val x33 = Record50(x29, x30, x32) 
x33 
} 
val M_flat1 = x34
val x35 = M_flat1
//M_flat1.collect.foreach(println(_))
val x44 = O__D_1.map{ case x36 => 
   val x37 = x36.o_orderkey 
val x38 = Record49(x37) 
val x39 = x36.o_custkey 
val x40 = x36.o_orderdate 
val x41 = Record51(x39, x40) 
val x42 = List(x41) 
val x43 = (x38, x42) 
x43 
} 
val M_flat2 = x44
val x45 = M_flat2
//M_flat2.collect.foreach(println(_))
L__D_1.unpersist(_)
O__D_1.unpersist(_)
P__D_1.unpersist(_)
val OrderParts__D_1 = M_flat1
OrderParts__D_1.cache
OrderParts__D_1.count
val OrderParts__D_2orders_1 = M_flat2
OrderParts__D_2orders_1.cache
OrderParts__D_2orders_1.count
def f = { 
 val x69 = OrderParts__D_1 
val x73 = { val out1 = x69.map{ case x70 => ({val x72 = x70.orders 
x72}, x70) }
  val out2 = OrderParts__D_2orders_1.flatMapValues(identity)
  out1.lookup(out2)
} 
val x78 = C__D_1.map(x74 => { val x75 = x74.c_name 
val x76 = x74.c_custkey 
val x77 = Record97(x75, x76) 
x77 }) 
val x84 = { val out1 = x73.map{ case (x79, x80) => ({val x82 = x80.o_custkey 
x82}, (x79, x80)) }
  val out2 = x78.map{ case x81 => ({val x83 = x81.c_custkey 
x83}, x81) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x94 = x84.flatMap{ case ((x85, x86), x87) => val x93 = (x87,x85) 
x93 match {
   case (_,null) => Nil
   case x92 => List(({val x88 = x87.c_name 
val x89 = x85.p_name 
val x90 = Record99(x88, x89) 
x90}, {val x91 = x85.l_qty 
x91}))
 }
}.reduceByKey(_ + _) 
val M_flat1 = x94
val x95 = M_flat1
//M_flat1.collect.foreach(println(_))
x95.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery4SparkNestedToFlat"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
