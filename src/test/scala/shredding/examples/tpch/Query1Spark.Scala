
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record707(c_name: String, c_custkey: Int)
case class Record708(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record709(l_quantity: Double, l_partkey: Int, l_orderkey: Int)
case class Record710(p_name: String, p_partkey: Int)
case class Record712(p_name: String, l_qty: Double)
case class Record714(o_orderdate: String, o_parts: Iterable[Record712])
case class Query1Out(c_name: String, c_orders: Iterable[Record714])
object Query1Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query1Spark"+sf)
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
   val x646 = C.map(x642 => { val x643 = x642.c_name 
val x644 = x642.c_custkey 
val x645 = Record707(x643, x644) 
x645 }) 
val x652 = O.map(x647 => { val x648 = x647.o_orderdate 
val x649 = x647.o_orderkey 
val x650 = x647.o_custkey 
val x651 = Record708(x648, x649, x650) 
x651 }) 
val x657 = { val out1 = x646.map{ case x653 => ({val x655 = x653.c_custkey 
x655}, x653) }
  val out2 = x652.map{ case x654 => ({val x656 = x654.o_custkey 
x656}, x654) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x663 = L.map(x658 => { val x659 = x658.l_quantity 
val x660 = x658.l_partkey 
val x661 = x658.l_orderkey 
val x662 = Record709(x659, x660, x661) 
x662 }) 
val x669 = { val out1 = x657.map{ case (x664, x665) => ({val x667 = x665.o_orderkey 
x667}, (x664, x665)) }
  val out2 = x663.map{ case x666 => ({val x668 = x666.l_orderkey 
x668}, x666) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x674 = P.map(x670 => { val x671 = x670.p_name 
val x672 = x670.p_partkey 
val x673 = Record710(x671, x672) 
x673 }) 
val x681 = { val out1 = x669.map{ case ((x675, x676), x677) => ({val x679 = x677.l_partkey 
x679}, ((x675, x676), x677)) }
  val out2 = x674.map{ case x678 => ({val x680 = x678.p_partkey 
x680}, x678) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x692 = x681.map{ case (((x682, x683), x684), x685) => val x691 = (x684,x685) 
x691 match {
   case (_,null) => ({val x686 = (x682,x683) 
x686}, null) 
   case x690 => ({val x686 = (x682,x683) 
x686}, {val x687 = x685.p_name 
val x688 = x684.l_quantity 
val x689 = Record712(x687, x688) 
x689})
 }
}.groupByKey() 
val x701 = x692.map{ case ((x693, x694), x695) => val x700 = (x694,x695) 
x700 match {
   case (_,null) => ({val x696 = (x693) 
x696}, null) 
   case x699 => ({val x696 = (x693) 
x696}, {val x697 = x694.o_orderdate 
val x698 = Record714(x697, x695) 
x698})
 }
}.groupByKey() 
val x706 = x701.map{ case (x702, x703) => 
   val x704 = x702.c_name 
val x705 = Query1Out(x704, x703) 
x705 
} 
x706.count
   var end0 = System.currentTimeMillis() - start0
   println("Query1Spark"+sf+","+Config.datapath+","+end0)
 }
}
