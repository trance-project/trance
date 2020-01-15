
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record109(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record110(p_name: String, p_partkey: Int)
case class Record111(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record112(c_name: String, c_custkey: Int)
case class Record113(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record115(p_name: String, l_qty: Double)
case class Record117(o_orderdate: String, o_parts: Iterable[Record115])
case class Record118(c_name: String, c_orders: Iterable[Record117])
object Query1FilterSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query1FilterSpark"+sf)
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
 val x37 = L.map(x32 => { val x33 = x32.l_orderkey 
val x34 = x32.l_quantity 
val x35 = x32.l_partkey 
val x36 = Record109(x33, x34, x35) 
x36 }) 
val x42 = P.map(x38 => { val x39 = x38.p_name 
val x40 = x38.p_partkey 
val x41 = Record110(x39, x40) 
x41 }) 
val x47 = { val out1 = x37.map{ case x43 => ({val x45 = x43.l_partkey 
x45}, x43) }
  val out2 = x42.map{ case x44 => ({val x46 = x44.p_partkey 
x46}, x44) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x54 = x47.map{ case (x48, x49) => 
   val x50 = x48.l_orderkey 
val x51 = x49.p_name 
val x52 = x48.l_quantity 
val x53 = Record111(x50, x51, x52) 
x53 
} 
val ljp = x54
val x55 = ljp
//ljp.collect.foreach(println(_))
val x62 = C.filter(x56 => { val x57 = x56.c_custkey 
val x58 = x57 <= 1500000 
x58 }).map(x56 => { val x59 = x56.c_name 
val x60 = x56.c_custkey 
val x61 = Record112(x59, x60) 
x61 }) 
val x70 = O.filter(x63 => { val x64 = x63.o_orderkey 
val x65 = x64 <= 150000000 
x65 }).map(x63 => { val x66 = x63.o_orderdate 
val x67 = x63.o_orderkey 
val x68 = x63.o_custkey 
val x69 = Record113(x66, x67, x68) 
x69 }) 
val x75 = { val out1 = x62.map{ case x71 => ({val x73 = x71.c_custkey 
x73}, x71) }
  val out2 = x70.map{ case x72 => ({val x74 = x72.o_custkey 
x74}, x72) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x77 = ljp 
val x83 = { val out1 = x75.map{ case (x78, x79) => ({val x81 = x79.o_orderkey 
x81}, (x78, x79)) }
  val out2 = x77.map{ case x80 => ({val x82 = x80.l_orderkey 
x82}, x80) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x93 = x83.flatMap{ case ((x84, x85), x86) => val x92 = (x86) 
x92 match {
   case (null) => Nil 
   case x91 => List(({val x87 = (x84,x85) 
x87}, {val x88 = x86.p_name 
val x89 = x86.l_qty 
val x90 = Record115(x88, x89) 
x90}))
 }
}.groupByKey() 
val x102 = x93.flatMap{ case ((x94, x95), x96) => val x101 = (x95,x96) 
x101 match {
   case (_,null) => Nil 
   case x100 => List(({val x97 = (x94) 
x97}, {val x98 = x95.o_orderdate 
val x99 = Record117(x98, x96) 
x99}))
 }
}.groupByKey() 
val x107 = x102.map{ case (x103, x104) => 
   val x105 = x103.c_name 
val x106 = Record118(x105, x104) 
x106 
} 
x107.count
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start 
   println("Query1FilterSpark"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
