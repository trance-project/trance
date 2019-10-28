
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record532(ps_partkey: Int, ps_suppkey: Int)
case class Record533(s_name: String, s_nationkey: Int, s_suppkey: Int)
case class Record534(ps_partkey: Int, s_name: String, s_nationkey: Int)
case class Record535(o_orderkey: Int, o_custkey: Int)
case class Record536(c_name: String, c_nationkey: Int, c_custkey: Int)
case class Record537(o_orderkey: Int, c_name: String, c_nationkey: Int)
case class Record538(l_partkey: Int, l_orderkey: Int)
case class Record539(l_partkey: Int, c_name: String, c_nationkey: Int)
case class Record540(p_name: String, p_partkey: Int)
case class Record542(s_name: String, s_nationkey: Int)
case class Record544(c_name: String, c_nationkey: Int)
case class Record545(p_name: String, suppliers: Iterable[Record542], customers: Iterable[Record544])
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
 val x422 = PS.map(x418 => { val x419 = x418.ps_partkey 
val x420 = x418.ps_suppkey 
val x421 = Record532(x419, x420) 
x421 }) 
val x428 = S.map(x423 => { val x424 = x423.s_name 
val x425 = x423.s_nationkey 
val x426 = x423.s_suppkey 
val x427 = Record533(x424, x425, x426) 
x427 }) 
val x433 = { val out1 = x422.map{ case x429 => ({val x431 = x429.ps_suppkey 
x431}, x429) }
  val out2 = x428.map{ case x430 => ({val x432 = x430.s_suppkey 
x432}, x430) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x440 = x433.map{ case (x434, x435) => 
   val x436 = x434.ps_partkey 
val x437 = x435.s_name 
val x438 = x435.s_nationkey 
val x439 = Record534(x436, x437, x438) 
x439 
} 
val partsuppliers = x440
val x441 = partsuppliers
//partsuppliers.collect.foreach(println(_))
val x446 = O.map(x442 => { val x443 = x442.o_orderkey 
val x444 = x442.o_custkey 
val x445 = Record535(x443, x444) 
x445 }) 
val x452 = C.map(x447 => { val x448 = x447.c_name 
val x449 = x447.c_nationkey 
val x450 = x447.c_custkey 
val x451 = Record536(x448, x449, x450) 
x451 }) 
val x457 = { val out1 = x446.map{ case x453 => ({val x455 = x453.o_custkey 
x455}, x453) }
  val out2 = x452.map{ case x454 => ({val x456 = x454.c_custkey 
x456}, x454) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x464 = x457.map{ case (x458, x459) => 
   val x460 = x458.o_orderkey 
val x461 = x459.c_name 
val x462 = x459.c_nationkey 
val x463 = Record537(x460, x461, x462) 
x463 
} 
val custorders = x464
val x465 = custorders
//custorders.collect.foreach(println(_))
val x467 = custorders 
val x472 = L.map(x468 => { val x469 = x468.l_partkey 
val x470 = x468.l_orderkey 
val x471 = Record538(x469, x470) 
x471 }) 
val x477 = { val out1 = x467.map{ case x473 => ({val x475 = x473.o_orderkey 
x475}, x473) }
  val out2 = x472.map{ case x474 => ({val x476 = x474.l_orderkey 
x476}, x474) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x484 = x477.map{ case (x478, x479) => 
   val x480 = x479.l_partkey 
val x481 = x478.c_name 
val x482 = x478.c_nationkey 
val x483 = Record539(x480, x481, x482) 
x483 
} 
val cparts = x484
val x485 = cparts
//cparts.collect.foreach(println(_))
val x490 = P.map(x486 => { val x487 = x486.p_name 
val x488 = x486.p_partkey 
val x489 = Record540(x487, x488) 
x489 }) 
val x492 = partsuppliers 
val x497 = { val out1 = x490.map{ case x493 => ({val x495 = x493.p_partkey 
x495}, x493) }
  val out2 = x492.map{ case x494 => ({val x496 = x494.ps_partkey 
x496}, x494) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x506 = x497.flatMap{ case (x498, x499) => val x505 = (x499) 
x505 match {
   case (null) => Nil 
   case x504 => List(({val x500 = (x498) 
x500}, {val x501 = x499.s_name 
val x502 = x499.s_nationkey 
val x503 = Record542(x501, x502) 
x503}))
 }
}.groupByKey() 
val x508 = cparts 
val x514 = { val out1 = x506.map{ case (x509, x510) => ({val x512 = x509.p_partkey 
x512}, (x509, x510)) }
  val out2 = x508.map{ case x511 => ({val x513 = x511.l_partkey 
x513}, x511) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x524 = x514.flatMap{ case ((x515, x516), x517) => val x523 = (x517) 
x523 match {
   case (null) => Nil 
   case x522 => List(({val x518 = (x515,x516) 
x518}, {val x519 = x517.c_name 
val x520 = x517.c_nationkey 
val x521 = Record544(x519, x520) 
x521}))
 }
}.groupByKey() 
val x530 = x524.map{ case ((x525, x526), x527) => 
   val x528 = x525.p_name 
val x529 = Record545(x528, x526, x527) 
x529 
} 
x530.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("Query3Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
