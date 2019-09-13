
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record73(c_name: String, c_custkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record74(o_orderdate: String, o_orderkey: Int, o_custkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record75(l_quantity: Double, l_partkey: Int, l_orderkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record76(p_name: String, p_partkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record78(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record80(o_orderdate: String, o_parts: Iterable[Record78], uniqueId: Long) extends CaseClassRecord
case class Record129(c_name: String, p_name: String, month: String, t_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Query1Out(c_name: String, c_orders: Iterable[Record80], uniqueId: Long) extends CaseClassRecord
object Query4Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query4Spark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   
val tpch = TPCHLoader(spark)
val C = tpch.loadCustomers
C.cache
C.count
val O = tpch.loadOrders
O.cache
O.count
val L = tpch.loadLineitem
L.cache
L.count
val P = tpch.loadPart
P.cache
P.count
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
   val Query4 = { val x12 = C.map(x8 => { val x9 = x8.c_name 
val x10 = x8.c_custkey 
val x11 = Record73(x9, x10, newId) 
x11 }) 
val x18 = O.map(x13 => { val x14 = x13.o_orderdate 
val x15 = x13.o_orderkey 
val x16 = x13.o_custkey 
val x17 = Record74(x14, x15, x16, newId) 
x17 }) 
val x23 = { val out1 = x12.map{ case x19 => ({val x21 = x19.c_custkey 
x21}, x19) }
  val out2 = x18.map{ case x20 => ({val x22 = x20.o_custkey 
x22}, x20) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x29 = L.map(x24 => { val x25 = x24.l_quantity 
val x26 = x24.l_partkey 
val x27 = x24.l_orderkey 
val x28 = Record75(x25, x26, x27, newId) 
x28 }) 
val x35 = { val out1 = x23.map{ case (x30, x31) => ({val x33 = x31.o_orderkey 
x33}, (x30, x31)) }
  val out2 = x29.map{ case x32 => ({val x34 = x32.l_orderkey 
x34}, x32) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x40 = P.map(x36 => { val x37 = x36.p_name 
val x38 = x36.p_partkey 
val x39 = Record76(x37, x38, newId) 
x39 }) 
val x47 = { val out1 = x35.map{ case ((x41, x42), x43) => ({val x45 = x43.l_partkey 
x45}, ((x41, x42), x43)) }
  val out2 = x40.map{ case x44 => ({val x46 = x44.p_partkey 
x46}, x44) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x58 = x47.map{ case (((x48, x49), x50), x51) => val x57 = (x50,x51) 
x57 match {
   case (_,null) => ({val x52 = (x48,x49) 
x52}, null) 
   case x56 => ({val x52 = (x48,x49) 
x52}, {val x53 = x51.p_name 
val x54 = x50.l_quantity 
val x55 = Record78(x53, x54, newId) 
x55})
 }
}.groupByKey() 
val x67 = x58.map{ case ((x59, x60), x61) => val x66 = (x60,x61) 
x66 match {
   case (_,null) => ({val x62 = (x59) 
x62}, null) 
   case x65 => ({val x62 = (x59) 
x62}, {val x63 = x60.o_orderdate 
val x64 = Record80(x63, x61, newId) 
x64})
 }
}.groupByKey() 
val x72 = x67.map{ case (x68, x69) => 
   val x70 = x68.c_name 
val x71 = Query1Out(x70, x69, newId) 
x71 
} 
x72 } 
 Query4.cache 
 Query4.count
   var start0 = System.currentTimeMillis()
   def f() {
     val x87 = Query4 
val x91 = x87.flatMap{ case x88 => x88 match {
   case null => List((x88, null))
   case _ =>
   val x89 = x88.c_orders 
x89 match {
     case x90 => x90.map{ case v2 => (x88, v2) }
  }
 }} 
val x96 = x91.flatMap{ case (x92, x93) => (x92, x93) match {
   case (_, null) => List(((x92, x93), null))
   case _ =>
   val x94 = x93.o_parts 
x94 match {
     case x95 => x95.map{ case v2 => ((x92, x93), v2) }
  }
 }} 
val x102 = x96.flatMap{ case ((x97, x98), x99) => ((x97, x98), x99) match {
   case (_, null) => List((((x97, x98), x99), null))
   case _ => 
   {val x100 = x98.o_parts 
x100} match {
     case Nil => List((((x97, x98), x99), null))
     case lst => lst.map{ case x101 => (((x97, x98), x99), x101) }
  }
 }} 
val x118 = x102.map{ case (((x103, x104), x105), x106) => val x117 = (x106) 
x117 match {
   case (null) => ({val x107 = (x103.c_name,x104.o_orderdate,x105.p_name) 
x107}, 0.0)
   case x116 => ({val x107 = (x103.c_name,x104.o_orderdate,x105.p_name) 
x107}, {val x108 = x106.p_name 
val x109 = x105.p_name 
val x110 = x108 == x109 
val x111 = x106.l_qty 
val x112 = x105.l_qty 
val x113 = x111 == x112 
val x114 = x110 && x113 
val x115 = 
 if ({x114})
 {  x111}
 else 0.0  
x115})
 }
}.reduceByKey(_ + _) 
/**val x127 = x118.map{ case ((x119, x120, x121), x122) => 
   val x123 = x119.c_name 
val x124 = x121.p_name 
val x125 = x120.o_orderdate 
val x126 = Record129(x123, x124, x125, x122, newId) 
x126 
}**/ 
val x127 = x118
x127.collect.foreach(println(_))
x127.count
   }
   f
   var end0 = System.currentTimeMillis() - start0
   println("Query4Spark"+sf+","+Config.datapath+","+end0)
 }
}
