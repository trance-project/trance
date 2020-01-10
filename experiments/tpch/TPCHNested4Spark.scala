
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
case class Record197(p_retailprice: Double, p_name: String)
case class Record199(p_name: String)
case class Record201(o_orderdate: String, o_parts: (Record199, Double))
case class Record202(c_name: String, c_orders: Iterable[Record201])
object TPCHNested4Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("TPCHNested4Spark"+sf)
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
  out1.join(out2).map{ case (k,v) => v }
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
 val x147 = Query1 
val x151 = x147.flatMap{ case x148 => x148 match {
   case null => List((x148, null))
   case _ => 
   {val x149 = x148.c_orders 
x149} match {
     case Nil => List((x148, null))
     case lst => lst.map{ case x150 => (x148, x150) }
  }
 }} 
val x156 = x151.flatMap{ case (x152, x153) => (x152, x153) match {
   case (_, null) => List(((x152, x153), null))
   case _ => 
   {val x154 = x153.o_parts 
x154} match {
     case Nil => List(((x152, x153), null))
     case lst => lst.map{ case x155 => ((x152, x153), x155) }
  }
 }} 
val x161 = P.map(x157 => { val x158 = x157.p_retailprice 
val x159 = x157.p_name 
val x160 = Record197(x158, x159) 
x160 }) 
val x168 = { val out1 = x156.map{ case ((x162, x163), x164) => ({val x166 = x164.p_name 
x166}, ((x162, x163), x164)) }
  val out2 = x161.map{ case x165 => ({val x167 = x165.p_name 
x167}, x165) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x181 = x168.flatMap{ case (((x169, x170), x171), x172) => val x180 = (x169,x170,x172) 
x180 match {
   case (_,null,_) => Nil
case (_,_,null) => Nil
   case x179 => List(({val x173 = x172.p_name 
val x174 = Record199(x173) 
val x175 = (x169,x170,x174) 
x175}, {val x176 = x171.l_qty 
val x177 = x172.p_retailprice 
val x178 = x176 * x177 
x178}))
 }
}.reduceByKey(_ + _) 
val x191 = x181.flatMap{ case ((x182, x183, x184), x185) => val x190 = (x183,x184) 
x190 match {
   case (_,null) => Nil 
   case x189 => List(({val x186 = (x182) 
x186}, {val x187 = x183.o_orderdate 
val x188 = Record201(x187, (x184, x185))
x188}))
 }
}.groupByKey() 
val x196 = x191.map{ case (x192, x193) => 
   val x194 = x192.c_name 
val x195 = Record202(x194, x193)
x195 
} 
x196.count
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start 
   println("TPCHNested4Spark"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
