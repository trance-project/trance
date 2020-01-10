
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record105(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record106(p_name: String, p_partkey: Int)
case class Record107(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record108(c_name: String, c_custkey: Int)
case class Record109(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record111(p_name: String, l_qty: Double)
case class Record113(o_orderdate: String, o_parts: Iterable[Record111])
case class Record114(c_name: String, c_orders: Iterable[Record113])
case class Record173(p_retailprice: Double, p_name: String)
case class Record175(c_name: String, p_name: String)
object TPCHNested2UnoptSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("TPCHNested2UnoptSpark"+sf)
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

   val Query1 = {
 val x37 = L.map(x32 => { val x33 = x32.l_orderkey 
val x34 = x32.l_quantity 
val x35 = x32.l_partkey 
val x36 = Record105(x33, x34, x35) 
x36 }) 
val x42 = P.map(x38 => { val x39 = x38.p_name 
val x40 = x38.p_partkey 
val x41 = Record106(x39, x40) 
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
val x53 = Record107(x50, x51, x52) 
x53 
} 
val ljp = x54
val x55 = ljp
//ljp.collect.foreach(println(_))
val x60 = C.map(x56 => { val x57 = x56.c_name 
val x58 = x56.c_custkey 
val x59 = Record108(x57, x58) 
x59 }) 
val x66 = O.map(x61 => { val x62 = x61.o_orderdate 
val x63 = x61.o_orderkey 
val x64 = x61.o_custkey 
val x65 = Record109(x62, x63, x64) 
x65 }) 
val x71 = { val out1 = x60.map{ case x67 => ({val x69 = x67.c_custkey 
x69}, x67) }
  val out2 = x66.map{ case x68 => ({val x70 = x68.o_custkey 
x70}, x68) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x73 = ljp 
val x79 = { val out1 = x71.map{ case (x74, x75) => ({val x77 = x75.o_orderkey 
x77}, (x74, x75)) }
  val out2 = x73.map{ case x76 => ({val x78 = x76.l_orderkey 
x78}, x76) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x89 = x79.flatMap{ case ((x80, x81), x82) => val x88 = (x82) 
x88 match {
   case (null) => Nil 
   case x87 => List(({val x83 = (x80,x81) 
x83}, {val x84 = x82.p_name 
val x85 = x82.l_qty 
val x86 = Record111(x84, x85) 
x86}))
 }
}.groupByKey() 
val x98 = x89.flatMap{ case ((x90, x91), x92) => val x97 = (x91,x92) 
x97 match {
   case (_,null) => Nil 
   case x96 => List(({val x93 = (x90) 
x93}, {val x94 = x91.o_orderdate 
val x95 = Record113(x94, x92) 
x95}))
 }
}.groupByKey() 
val x103 = x98.map{ case (x99, x100) => 
   val x101 = x99.c_name 
val x102 = Record114(x101, x100) 
x102 
} 
x103
}
Query1.cache
Query1.count
def f = { 
 val x138 = Query1 
val x142 = x138.flatMap{ case x139 => x139 match {
   case null => List((x139, null))
   case _ =>
   val x140 = x139.c_orders 
x140 match {
     case x141 => x141.map{ case v2 => (x139, v2) }
  }
 }} 
val x147 = x142.flatMap{ case (x143, x144) => (x143, x144) match {
   case (_, null) => List(((x143, x144), null))
   case _ =>
   val x145 = x144.o_parts 
x145 match {
     case x146 => x146.map{ case v2 => ((x143, x144), v2) }
  }
 }} 
val x152 = P.map(x148 => { val x149 = x148.p_retailprice 
val x150 = x148.p_name 
val x151 = Record173(x149, x150) 
x151 }) 
val x159 = { val out1 = x147.map{ case ((x153, x154), x155) => ({val x157 = x155.p_name 
x157}, ((x153, x154), x155)) }
  val out2 = x152.map{ case x156 => ({val x158 = x156.p_name 
x158}, x156) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x172 = x159.flatMap{ case (((x160, x161), x162), x163) => val x171 = (x160,x163) 
x171 match {
   case (_,null) => Nil
   case x170 => List(({val x164 = x160.c_name 
val x165 = x163.p_name 
val x166 = Record175(x164, x165) 
x166}, {val x167 = x162.l_qty 
val x168 = x163.p_retailprice 
val x169 = x167 * x168 
x169}))
 }
}.reduceByKey(_ + _) 
x172.count
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start 
   println("TPCHNested2UnoptSpark"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
