
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record540(ps_partkey: Int, ps_suppkey: Int)
case class Record541(s_name: String, s_nationkey: Int, s_suppkey: Int)
case class Record542(ps_partkey: Int, s_name: String, s_nationkey: Int)
case class Record543(o_orderkey: Int, o_custkey: Int)
case class Record544(c_name: String, c_nationkey: Int, c_custkey: Int)
case class Record545(o_orderkey: Int, c_name: String, c_nationkey: Int)
case class Record546(l_partkey: Int, l_orderkey: Int)
case class Record547(l_partkey: Int, c_name: String, c_nationkey: Int)
case class Record548(p_name: String, p_partkey: Int)
case class Record550(s_name: String, s_nationkey: Int)
case class Record552(c_name: String, c_nationkey: Int)
case class Record553(p_name: String, suppliers: Iterable[Record550], customers: Iterable[Record552])
object Query3Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query3Spark"+sf)
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
val PS = tpch.loadPartSupp
PS.cache
PS.count
val S = tpch.loadSupplier
S.cache
S.count

   def f = { 
 val x430 = PS.map(x426 => { val x427 = x426.ps_partkey 
val x428 = x426.ps_suppkey 
val x429 = Record540(x427, x428) 
x429 }) 
val x436 = S.map(x431 => { val x432 = x431.s_name 
val x433 = x431.s_nationkey 
val x434 = x431.s_suppkey 
val x435 = Record541(x432, x433, x434) 
x435 }) 
val x441 = { val out1 = x430.map{ case x437 => ({val x439 = x437.ps_suppkey 
x439}, x437) }
  val out2 = x436.map{ case x438 => ({val x440 = x438.s_suppkey 
x440}, x438) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x448 = x441.map{ case (x442, x443) => 
   val x444 = x442.ps_partkey 
val x445 = x443.s_name 
val x446 = x443.s_nationkey 
val x447 = Record542(x444, x445, x446) 
x447 
} 
val partsuppliers = x448
val x449 = partsuppliers
//partsuppliers.collect.foreach(println(_))
val x454 = O.map(x450 => { val x451 = x450.o_orderkey 
val x452 = x450.o_custkey 
val x453 = Record543(x451, x452) 
x453 }) 
val x460 = C.map(x455 => { val x456 = x455.c_name 
val x457 = x455.c_nationkey 
val x458 = x455.c_custkey 
val x459 = Record544(x456, x457, x458) 
x459 }) 
val x465 = { val out1 = x454.map{ case x461 => ({val x463 = x461.o_custkey 
x463}, x461) }
  val out2 = x460.map{ case x462 => ({val x464 = x462.c_custkey 
x464}, x462) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x472 = x465.map{ case (x466, x467) => 
   val x468 = x466.o_orderkey 
val x469 = x467.c_name 
val x470 = x467.c_nationkey 
val x471 = Record545(x468, x469, x470) 
x471 
} 
val custorders = x472
val x473 = custorders
//custorders.collect.foreach(println(_))
val x475 = custorders 
val x480 = L.map(x476 => { val x477 = x476.l_partkey 
val x478 = x476.l_orderkey 
val x479 = Record546(x477, x478) 
x479 }) 
val x485 = { val out1 = x475.map{ case x481 => ({val x483 = x481.o_orderkey 
x483}, x481) }
  val out2 = x480.map{ case x482 => ({val x484 = x482.l_orderkey 
x484}, x482) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x492 = x485.map{ case (x486, x487) => 
   val x488 = x487.l_partkey 
val x489 = x486.c_name 
val x490 = x486.c_nationkey 
val x491 = Record547(x488, x489, x490) 
x491 
} 
val cparts = x492
val x493 = cparts
//cparts.collect.foreach(println(_))
val x498 = P.map(x494 => { val x495 = x494.p_name 
val x496 = x494.p_partkey 
val x497 = Record548(x495, x496) 
x497 }) 
val x500 = partsuppliers 
val x505 = { val out1 = x498.map{ case x501 => ({val x503 = x501.p_partkey 
x503}, x501) }
  val out2 = x500.map{ case x502 => ({val x504 = x502.ps_partkey 
x504}, x502) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x514 = x505.flatMap{ case (x506, x507) => val x513 = (x507) 
x513 match {
   case (null) => Nil 
   case x512 => List(({val x508 = (x506) 
x508}, {val x509 = x507.s_name 
val x510 = x507.s_nationkey 
val x511 = Record550(x509, x510) 
x511}))
 }
}.groupByKey() 
val x516 = cparts 
val x522 = { val out1 = x514.map{ case (x517, x518) => ({val x520 = x517.p_partkey 
x520}, (x517, x518)) }
  val out2 = x516.map{ case x519 => ({val x521 = x519.l_partkey 
x521}, x519) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x532 = x522.flatMap{ case ((x523, x524), x525) => val x531 = (x525) 
x531 match {
   case (null) => Nil 
   case x530 => List(({val x526 = (x523,x524) 
x526}, {val x527 = x525.c_name 
val x528 = x525.c_nationkey 
val x529 = Record552(x527, x528) 
x529}))
 }
}.groupByKey() 
val x538 = x532.map{ case ((x533, x534), x535) => 
   val x536 = x533.p_name 
val x537 = Record553(x536, x534, x535) 
x537 
} 
x538.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("Query3Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
