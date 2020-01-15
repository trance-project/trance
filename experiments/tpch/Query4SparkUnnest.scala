
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record43(c_name: String, c_custkey: Int)
case class Record44(o_orderkey: Int, o_orderdate: String, o_custkey: Int)
case class Record46(o_orderkey: Int, o_orderdate: String)
case class Record47(c_name: String, corders: Iterable[Record46])
case class Record122(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record123(p_name: String, p_partkey: Int)
case class Record124(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record126(c_name: String, orderdate: String, pname: String)
object Query4SparkUnnest {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query4SparkUnnest"+sf)
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

   val CustOrders = {
 val x17 = C.map(x13 => { val x14 = x13.c_name 
val x15 = x13.c_custkey 
val x16 = Record43(x14, x15) 
x16 }) 
val x23 = O.map(x18 => { val x19 = x18.o_orderkey 
val x20 = x18.o_orderdate 
val x21 = x18.o_custkey 
val x22 = Record44(x19, x20, x21) 
x22 }) 
val x28 = { val out1 = x17.map{ case x24 => ({val x26 = x24.c_custkey 
x26}, x24) }
  val out2 = x23.map{ case x25 => ({val x27 = x25.o_custkey 
x27}, x25) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x37 = x28.flatMap{ case (x29, x30) => val x36 = (x30) 
x36 match {
   case (null) => Nil 
   case x35 => List(({val x31 = (x29) 
x31}, {val x32 = x30.o_orderkey 
val x33 = x30.o_orderdate 
val x34 = Record46(x32, x33) 
x34}))
 }
}.groupByKey() 
val x42 = x37.map{ case (x38, x39) => 
   val x40 = x38.c_name 
val x41 = Record47(x40, x39) 
x41 
} 
x42
}
CustOrders.cache
CustOrders.count
def f = { 
 val x77 = L.map(x72 => { val x73 = x72.l_orderkey 
val x74 = x72.l_quantity 
val x75 = x72.l_partkey 
val x76 = Record122(x73, x74, x75) 
x76 }) 
val x82 = P.map(x78 => { val x79 = x78.p_name 
val x80 = x78.p_partkey 
val x81 = Record123(x79, x80) 
x81 }) 
val x87 = { val out1 = x77.map{ case x83 => ({val x85 = x83.l_partkey 
x85}, x83) }
  val out2 = x82.map{ case x84 => ({val x86 = x84.p_partkey 
x86}, x84) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x94 = x87.map{ case (x88, x89) => 
   val x90 = x88.l_orderkey 
val x91 = x89.p_name 
val x92 = x88.l_quantity 
val x93 = Record124(x90, x91, x92) 
x93 
} 
val parts = x94
val x95 = parts
//parts.collect.foreach(println(_))
val x97 = CustOrders 
val x101 = x97.flatMap{ case x98 => x98 match {
   case null => List((x98, null))
   case _ =>
   val x99 = x98.corders 
x99 match {
     case x100 => x100.map{ case v2 => (x98, v2) }
  }
 }} 
val x103 = parts 
val x109 = { val out1 = x101.map{ case (x104, x105) => ({val x107 = x105.o_orderkey 
x107}, (x104, x105)) }
  val out2 = x103.map{ case x106 => ({val x108 = x106.l_orderkey 
x108}, x106) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x120 = x109.flatMap{ case ((x110, x111), x112) => val x119 = (x110,x111,x112) 
x119 match {
   case (_,null,_) => Nil
case (_,_,null) => Nil
   case x118 => List(({val x113 = x110.c_name 
val x114 = x111.o_orderdate 
val x115 = x112.p_name 
val x116 = Record126(x113, x114, x115) 
x116}, {val x117 = x112.l_qty 
x117}))
 }
}.reduceByKey(_ + _) 
//x120.collect.foreach(println(_))
x120.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("Query4SparkUnnest"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
