
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record579(c_name: String, c_custkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record581(P__F: Int, L__F: Int, O__F: Int, c__F: Record579, uniqueId: Long) extends CaseClassRecord
case class Record582(c_name: String, c_orders: Record581, uniqueId: Long) extends CaseClassRecord
case class Record584(o_orderdate: String, o_custkey: Int, o_orderkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record586(o__F: Record584, P__F: Int, L__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record587(o_orderdate: String, o_parts: Record586, uniqueId: Long) extends CaseClassRecord
case class Record589(l_quantity: Double, l_partkey: Int, l_orderkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record591(p_name: String, p_partkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record593(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class RecM_flat2(_1: Record581, _2: Iterable[Record587], uniqueId: Long) extends CaseClassRecord
case class RecM_flat3(_1: Record586, _2: Iterable[Record593], uniqueId: Long) extends CaseClassRecord
case class RecM_flat1(_1: Q1Flat, _2: Iterable[Record582], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx3(lbl: Record586, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx2(lbl: Record581, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx1(lbl: Q1Flat, uniqueId: Long) extends CaseClassRecord
object ShredQuery1SparkMinimalDomain3 {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1SparkMinimalDomain3"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   
val tpch = TPCHLoader(spark)
val C__F = 1
val C__D_1 = tpch.loadCustomers()
C__D_1.cache
C__D_1.count
val O__F = 2
val O__D_1 = tpch.loadOrders()
O__D_1.cache
O__D_1.count
val L__F = 3
val L__D_1 = tpch.loadLineitem()
L__D_1.cache
L__D_1.count
val P__F = 4
val P__D_1 = tpch.loadPart()
P__D_1.cache
P__D_1.count
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
   var start0 = System.currentTimeMillis()

   val x439 = Q1Flat(P__F, C__F, L__F, O__F, newId) 
val x440 = RecM_ctx1(x439, newId) 
val x441 = List(x440) 
val M_ctx1 = x441.head
val x444 = M_ctx1 

val x445 = C__D_1 
val x450 = x445.map(x446 => { val x447 = x446.c_name 
val x448 = x446.c_custkey 
val x449 = Record579(x447, x448, newId) 
x449 }) 
val x453 = x450.map{ case c => (x444, c) } 
val x466 = x453.map{ case (x454, x455) => val x465 = (x455) 
x465 match {
   case (null) => ({val x456 = (x454) 
x456}, null) 
   case x464 => ({val x456 = (x454) 
x456}, {val x457 = x455.c_name 
val x458 = x454.lbl 
val x459 = x458.P__F 
val x460 = x458.L__F 
val x461 = x458.O__F 
val x462 = Record581(x459, x460, x461, x455, newId) 
val x463 = Record582(x457, x462, newId) 
x463})
 }
}.groupByLabel() 
val x471 = x466.map{ case (x467, x468) => 
   val x469 = x467.lbl 
val x470 = RecM_flat1(x469, x468, newId) 
x470 
} 
val M_flat1 = x471
val x472 = M_flat1
M_flat1.count

val x488 = O__D_1 
val x494 = x488.map(x489 => { val x490 = x489.o_orderdate 
val x491 = x489.o_custkey 
val x492 = x489.o_orderkey 
val x493 = Record584(x490, x491, x492, newId) 
x493 }).map{ case x496 => ({val x500 = x496.o_custkey
x500}, x496) }.groupByLabel() 

val M_flat2 = x494
val x519 = M_flat2
M_flat2.count

val x535 = L__D_1 
val x541 = x535.map(x536 => { val x537 = x536.l_quantity 
val x538 = x536.l_partkey 
val x539 = x536.l_orderkey 
val x540 = Record589(x537, x538, x539, newId) 
x540 }) 

val x549 = P__D_1 
val x554 = x549.map(x550 => { val x551 = x550.p_name 
val x552 = x550.p_partkey 
val x553 = Record591(x551, x552, newId) 
x553 }) 
val x560 = { val out1 = x541.map{ case x556 => ({val x558 = x556.l_partkey 
x558}, x556) }
  val out2 = x554.map{ case x557 => ({val x559 = x557.p_partkey 
x559}, x557) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
}

val x570 = x560.map{ case (x562, x563) => val x569 = (x562,x563) 
x569 match {
   case (_,null) => ({val x564 = (x562.l_orderkey) 
x564}, null) 
   case x568 => ({val x564 = (x562.l_orderkey) 
x564}, {val x565 = x563.p_name 
val x566 = x562.l_quantity 
val x567 = Record593(x565, x566, newId) 
x567})
 }
}.groupByLabel() 

val M_flat3 = x570
val x576 = M_flat3
M_flat3.count

   var end = System.currentTimeMillis() - start0
   println("ShredQuery1Spark"+sf+","+Config.datapath+",total,"+end)
 }
}
