
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
case class Record132(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record133(p_name: String, p_partkey: Int)
case class Record134(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record136(orderdate: String, pname: String)
case class Record137(c_name: String, partqty: Record136)
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
 val x81 = L.map(x76 => { val x77 = x76.l_orderkey 
val x78 = x76.l_quantity 
val x79 = x76.l_partkey 
val x80 = Record132(x77, x78, x79) 
x80 }) 
val x86 = P.map(x82 => { val x83 = x82.p_name 
val x84 = x82.p_partkey 
val x85 = Record133(x83, x84) 
x85 }) 
val x91 = { val out1 = x81.map{ case x87 => ({val x89 = x87.l_partkey 
x89}, x87) }
  val out2 = x86.map{ case x88 => ({val x90 = x88.p_partkey 
x90}, x88) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x98 = x91.map{ case (x92, x93) => 
   val x94 = x92.l_orderkey 
val x95 = x93.p_name 
val x96 = x92.l_quantity 
val x97 = Record134(x94, x95, x96) 
x97 
} 
val parts = x98
val x99 = parts
//parts.collect.foreach(println(_))
val x101 = CustOrders 
val x105 = x101.flatMap{ case x102 => x102 match {
   case null => List((x102, null))
   case _ => 
   {val x103 = x102.corders 
x103} match {
     case Nil => List((x102, null))
     case lst => lst.map{ case x104 => (x102, x104) }
  }
 }} 
val x107 = parts 
val x113 = { val out1 = x105.map{ case (x108, x109) => ({val x111 = x109.o_orderkey 
x111}, (x108, x109)) }
  val out2 = x107.map{ case x110 => ({val x112 = x110.l_orderkey 
x112}, x110) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x124 = x113.flatMap{ case ((x114, x115), x116) => val x123 = (x114,x115,x116) 
x123 match {
   case (_,null,_) => Nil
case (_,_,null) => Nil
   case x122 => List(({val x117 = x115.o_orderdate 
val x118 = x116.p_name 
val x119 = Record136(x117, x118) 
val x120 = (x114,x119) 
x120}, {val x121 = x116.l_qty 
x121}))
 }
}.reduceByKey(_ + _) 
val x130 = x124.map{ case ((x125, x126), x127) => 
   val x128 = x125.c_name 
val x129 = Record137(x128, x126) 
x129 
} 
x130.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("Query4SparkUnnest"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
