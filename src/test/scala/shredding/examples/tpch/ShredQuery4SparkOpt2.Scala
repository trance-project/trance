
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record576(c_name: String, c_custkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record578(P__F: Int, L__F: Int, O__F: Int, c__F: Record576, uniqueId: Long) extends CaseClassRecord
case class Record579(c_name: String, c_orders: Record578, uniqueId: Long) extends CaseClassRecord
case class Record581(o_orderdate: String, o_custkey: Int, o_orderkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record583(o__F: Record581, P__F: Int, L__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record584(o_orderdate: String, o_parts: Record583, uniqueId: Long) extends CaseClassRecord
case class Record586(l_quantity: Double, l_partkey: Int, l_orderkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record588(p_name: String, p_partkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record590(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record676(lbl: Q1Flat2, uniqueId: Long) extends CaseClassRecord
case class Input_Query4__D(c_name: String, c_orders: Int, uniqueId: Long) extends CaseClassRecord
case class Record680(c_name: String, p_name: String, month: String, t_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record681(_1: Q1Flat2, _2: Iterable[Record680], uniqueId: Long) extends CaseClassRecord
case class RecM_flat2(_1: Record578, _2: Iterable[Record584], uniqueId: Long) extends CaseClassRecord
case class RecM_flat3(_1: Record583, _2: Iterable[Record590], uniqueId: Long) extends CaseClassRecord
case class RecM_flat1(_1: Q1Flat, _2: Iterable[Record579], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx3(lbl: Record583, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx2(lbl: Record578, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx1(lbl: Q1Flat, uniqueId: Long) extends CaseClassRecord
object ShredQuery4SparkOpt2 {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4SparkOpt2"+sf)
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

   val x439 = Q1Flat(P__F, C__F, L__F, O__F, newId)
val x440 = RecM_ctx1(x439, newId)
val x441 = List(x440)
val M_ctx1 = x441.head
val x444 = M_ctx1

val x445 = C__D_1
val x450 = x445.map(x446 => { val x447 = x446.c_name
val x448 = x446.c_custkey
val x449 = Record576(x447, x448, newId)
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
val x462 = Record578(x459, x460, x461, x455, newId)
val x463 = Record579(x457, x462, newId)
x463})
 }
}.groupByLabel()
val x471 = x466.map{ case (x467, x468) =>
   val x469 = x467.lbl
val x470 = RecM_flat1(x469, x468, newId)
x470
}
val M_flat1 = x471
//println("M_flat1")
val x472 = M_flat1
//M_flat1.collect.foreach(println(_))

val x474 = M_flat1
val x478 = x474.flatMap{ case x475 => x475 match {
   case null => List((x475, null))
   case _ =>
   val x476 = x475._2
x476 match {
     case x477 => x477.map{ case v2 => (x475, v2) }
  }
 }}
val x483 = x478.map{ case (x479, x480) =>
   val x481 = x480.c_orders
val x482 = RecM_ctx2(x481, newId)
x482
}
val x484 = x483.distinct
val M_ctx2 = x484
//println("M_ctx2")
val x485 = M_ctx2

//M_ctx2.collect.foreach(println(_))
val x487 = M_ctx2
val x488 = O__D_1
val x494 = x488.map(x489 => { val x490 = x489.o_orderdate
val x491 = x489.o_custkey
val x492 = x489.o_orderkey
val x493 = Record581(x490, x491, x492, newId)
x493 })
val x501 = { val out1 = x487.map{ case x495 => ({val x497 = x495.lbl
val x498 = x497.c__F
val x499 = x498.c_custkey
x499}, x495) }
  val out2 = x494.map{ case x496 => ({val x500 = x496.o_custkey
x500}, x496) }
  out1.join(out2).map{ case (k,v) => v }
}
val x513 = x501.map{ case (x502, x503) => val x512 = (x503)
x512 match {
   case (null) => ({val x504 = (x502)
x504}, null)
   case x511 => ({val x504 = (x502)
x504}, {val x505 = x503.o_orderdate
val x506 = x502.lbl
val x507 = x506.P__F
val x508 = x506.L__F
val x509 = Record583(x503, x507, x508, newId)
val x510 = Record584(x505, x509, newId)
x510})
 }
}.groupByLabel()
val x518 = x513.map{ case (x514, x515) =>
   val x516 = x514.lbl
val x517 = RecM_flat2(x516, x515, newId)
x517
}
val M_flat2 = x518
//println("M_flat2")
val x519 = M_flat2
//M_flat2.collect.foreach(println(_))

val x521 = M_flat2
val x525 = x521.flatMap{ case x522 => x522 match {
   case null => List((x522, null))
   case _ =>
   val x523 = x522._2
x523 match {
     case x524 => x524.map{ case v2 => (x522, v2) }
  }
 }}
val x530 = x525.map{ case (x526, x527) =>
   val x528 = x527.o_parts
val x529 = RecM_ctx3(x528, newId)
x529
}
val x531 = x530.distinct
val M_ctx3 = x531
//println("M_ctx3")
val x532 = M_ctx3
//M_ctx3.collect.foreach(println(_))

val x534 = M_ctx3
val x535 = L__D_1
val x541 = x535.map(x536 => { val x537 = x536.l_quantity
val x538 = x536.l_partkey
val x539 = x536.l_orderkey
val x540 = Record586(x537, x538, x539, newId)
x540 })

val x549 = P__D_1
val x554 = x549.map(x550 => { val x551 = x550.p_name
val x552 = x550.p_partkey
val x553 = Record588(x551, x552, newId)
x553 })
val x560 = { val out1 = x541.map{ case x556 => ({val x558 = x556.l_partkey
x558}, x556) }
  val out2 = x554.map{ case x557 => ({val x559 = x557.p_partkey
x559}, x557) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
}

val x548 = { val out1 = x534.map{ case x542 => ({val x544 = x542.lbl
val x545 = x544.o__F
val x546 = x545.o_orderkey
x546}, x542) }
  val out2 = x560.map{ case x543 => ({val x547 = x543._1.l_orderkey
x547}, x543) }
  out1.join(out2).map{ case (k,v) => v }
}

val x570 = x548.map{ case (x561, (x562, x563)) => val x569 = (x562,x563)
x569 match {
   case (_,null) => ({val x564 = (x561)
x564}, null)
   case x568 => ({val x564 = (x561)
x564}, {val x565 = x563.p_name
val x566 = x562.l_quantity
val x567 = Record590(x565, x566, newId)
x567})
 }
}.groupByLabel()
val x575 = x570.map{ case (x571, x572) =>
   val x573 = x571.lbl
val x574 = RecM_flat3(x573, x572, newId)
x574
}
val M_flat3 = x575
//println("M_flat3")
val x576 = M_flat3


val Query4__F = M_ctx1.lbl
val Query4__D_1 = M_flat1.flatMap{ r => r._2 }
Query4__D_1.cache
Query4__D_1.count
val Query4__D_2c_orders_1 = M_flat2
Query4__D_2c_orders_1.cache
Query4__D_2c_orders_1.count
val Query4__D_2c_orders_2o_parts = M_flat3
Query4__D_2c_orders_2o_parts.cache
Query4__D_2c_orders_2o_parts.count
    
   var start0 = System.currentTimeMillis()
   def f() {
     val x602 = Q1Flat2(Query4__F, newId)
val x603 = Record676(x602, newId)
val x604 = List(x603)
val M_ctx1 = x604.head
val x607 = M_ctx1
val x608 = Query4__D_1
val x610 = x608
val x613 = x610.map{ case c => (x607, c) }
val x615 = Query4__D_2c_orders_1
val x618 = x615

val x624 = Query4__D_2c_orders_2o_parts
val x626 = x624
val x628 = x626

val x634 = {
  val out1 = x618.flatMap(x621 => x621._2.map{case v2 => (v2.o_parts, (x621._1, v2))})
  val out2 = x628.flatMap(x632 => x632._2.map{case v2 => (x632._1, v2)})
  out1.outerLookup(out2)
}

val x655 = x634.map{
  case (a, null) => (a, 0.0)
  case ((x637, x638), x639) => ((x637, x637.c__F.c_name, x638.o_orderdate, x639.p_name), x639.l_qty)
}.reduceByKey(_+_)

val x623 = {
  val out1 = x613.map{ case (a, null) => (null, (a, null)); case (x619, x620) => ({val x622 = x620.c_orders
x622}, (x619, x620)) }
  out1.outerLookupSkewLeft(x655.map{ 
    case ((a,b,c,d), e) =>  a -> (b, c, d, e)
    case (_,_) => sys.error("failed") 
  }.asInstanceOf[RDD[(experiments.Record578, Any)]])
}.groupByLabel()
  
  x623.collect.foreach(println(_))
  x623.count
   }
   f
   var end0 = System.currentTimeMillis() - start0
   println("ShredQuery4SparkOpt2"+sf+","+Config.datapath+","+end0)
 }
}
