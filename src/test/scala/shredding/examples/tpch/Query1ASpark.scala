
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record71(c_name: String, c_custkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record72(o_orderdate: String, o_custkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record73(l_quantity: Double, l_partkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record74(p_name: String, p_partkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record76(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record78(o_orderdate: String, o_parts: Iterable[Record76], uniqueId: Long) extends CaseClassRecord
case class Query1AOut(c_name: String, c_orders: Iterable[Record78], uniqueId: Long) extends CaseClassRecord
object Query1ASpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query1ASpark"+sf)
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
   var start0 = System.currentTimeMillis()
   val x11 = C.map(x7 => { val x8 = x7.c_name 
val x9 = x7.c_custkey 
val x10 = Record71(x8, x9, newId) 
x10 }) 
val x16 = O.map(x12 => { val x13 = x12.o_orderdate 
val x14 = x12.o_custkey 
val x15 = Record72(x13, x14, newId) 
x15 }) 
val x21 = { val out1 = x11.map{ case x17 => ({val x19 = x17.c_custkey 
x19}, x17) }
  val out2 = x16.map{ case x18 => ({val x20 = x18.o_custkey 
x20}, x18) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x26 = L.map(x22 => { val x23 = x22.l_quantity 
val x24 = x22.l_partkey 
val x25 = Record73(x23, x24, newId) 
x25 }) 
val x30 = { val out1 = x21.map{ case (x27, x28) => ({true}, (x27, x28)) }
  val out2 = x26.map{ case x29 => ({true}, x29) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x35 = P.map(x31 => { val x32 = x31.p_name 
val x33 = x31.p_partkey 
val x34 = Record74(x32, x33, newId) 
x34 }) 
val x42 = { val out1 = x30.map{ case ((x36, x37), x38) => ({val x40 = x38.l_partkey 
x40}, ((x36, x37), x38)) }
  val out2 = x35.map{ case x39 => ({val x41 = x39.p_partkey 
x41}, x39) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x56 = x42.map{ case (((x43, x44), x45), x46) => val x55 = (x45,x46) 
x55 match {
   case x51 if {val x52 = x45.l_orderkey 
val x53 = x44.o_orderkey 
val x54 = x52 == x53 
x54} => ({val x47 = (x43,x44) 
x47}, {val x48 = x46.p_name 
val x49 = x45.l_quantity 
val x50 = Record76(x48, x49, newId) 
x50})
   case x51 => ({val x47 = (x43,x44) 
x47}, shredding.generator.SparkNamedGenerator$$Lambda$6398/32593371@6be1348)
 }    
}.groupByKey() 
val x66 = x56.map{ case ((x57, x58), x59) => val x65 = (x58,x59) 
x65 match {
   case (_,null) => ({val x60 = x57.c_name 
val x61 = (x60) 
x61}, null) 
   case x64 => ({val x60 = x57.c_name 
val x61 = (x60) 
x61}, {val x62 = x58.o_orderdate 
val x63 = Record78(x62, x59, newId) 
x63})
 }
}.groupByKey() 
val x70 = x66.map{ case (x67, x68) => 
   val x69 = Query1AOut(x67, x68, newId) 
x69 
} 
x70.count
   var end0 = System.currentTimeMillis() - start0
   println("Query1ASpark"+sf+","+Config.datapath+","+end0)
 }
}
