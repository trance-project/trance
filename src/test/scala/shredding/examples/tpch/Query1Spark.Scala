
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record81(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record82(p_name: String, p_partkey: Int)
case class Record83(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record84(c_name: String, c_custkey: Int)
case class Record85(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record87(p_name: String, l_qty: Double)
case class Record89(o_orderdate: String, o_parts: Iterable[Record87])
case class Record90(c_name: String, c_orders: Iterable[Record89])
object Query1Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query1Spark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L = tpch.loadLineitem
L.cache
L.count
val P = tpch.loadPart
P.cache
P.count
val C = tpch.loadCustomers
C.cache
C.count
val O = tpch.loadOrders
O.cache
O.count

   def f = { 
 val x13 = L.map(x8 => { val x9 = x8.l_orderkey 
val x10 = x8.l_quantity 
val x11 = x8.l_partkey 
val x12 = Record81(x9, x10, x11) 
x12 }) 
val x18 = P.map(x14 => { val x15 = x14.p_name 
val x16 = x14.p_partkey 
val x17 = Record82(x15, x16) 
x17 }) 
val x23 = { val out1 = x13.map{ case x19 => ({val x21 = x19.l_partkey 
x21}, x19) }
  val out2 = x18.map{ case x20 => ({val x22 = x20.p_partkey 
x22}, x20) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x30 = x23.map{ case (x24, x25) => 
   val x26 = x24.l_orderkey 
val x27 = x25.p_name 
val x28 = x24.l_quantity 
val x29 = Record83(x26, x27, x28) 
x29 
} 
val ljp = x30
val x31 = ljp
//ljp.collect.foreach(println(_))
val x36 = C.map(x32 => { val x33 = x32.c_name 
val x34 = x32.c_custkey 
val x35 = Record84(x33, x34) 
x35 }) 
val x42 = O.map(x37 => { val x38 = x37.o_orderdate 
val x39 = x37.o_orderkey 
val x40 = x37.o_custkey 
val x41 = Record85(x38, x39, x40) 
x41 }) 
val x47 = { val out1 = x36.map{ case x43 => ({val x45 = x43.c_custkey 
x45}, x43) }
  val out2 = x42.map{ case x44 => ({val x46 = x44.o_custkey 
x46}, x44) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x49 = ljp 
val x55 = { val out1 = x47.map{ case (x50, x51) => ({val x53 = x51.o_orderkey 
x53}, (x50, x51)) }
  val out2 = x49.map{ case x52 => ({val x54 = x52.l_orderkey 
x54}, x52) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x65 = x55.flatMap{ case ((x56, x57), x58) => val x64 = (x58) 
x64 match {
   case (null) => Nil 
   case x63 => List(({val x59 = (x56,x57) 
x59}, {val x60 = x58.p_name 
val x61 = x58.l_qty 
val x62 = Record87(x60, x61) 
x62}))
 }
}.groupByKey() 
val x74 = x65.flatMap{ case ((x66, x67), x68) => val x73 = (x67,x68) 
x73 match {
   case (_,null) => Nil 
   case x72 => List(({val x69 = (x66) 
x69}, {val x70 = x67.o_orderdate 
val x71 = Record89(x70, x68) 
x71}))
 }
}.groupByKey() 
val x79 = x74.map{ case (x75, x76) => 
   val x77 = x75.c_name 
val x78 = Record90(x77, x76) 
x78 
} 
x79.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis()
   println("Query1Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
