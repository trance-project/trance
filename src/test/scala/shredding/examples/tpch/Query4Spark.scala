
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record478(c_name: String, c_custkey: Int)
case class Record479(o_orderkey: Int, o_orderdate: String, o_custkey: Int)
case class Record481(o_orderkey: Int, o_orderdate: String)
case class Record482(c_name: String, c_orders: Iterable[Record481])
case class Record544(l_quantity: Double, l_orderkey: Int, l_partkey: Int)
case class Record545(p_name: String, p_partkey: Int)
case class Record547(l_orderkey: Int, p_name: String)
case class Record549(c_name: String, o_orderdate: String, p_name: String)
object Query4Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query4Spark"+sf)
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
 val x452 = C.map(x448 => { val x449 = x448.c_name 
val x450 = x448.c_custkey 
val x451 = Record478(x449, x450) 
x451 }) 
val x458 = O.map(x453 => { val x454 = x453.o_orderkey 
val x455 = x453.o_orderdate 
val x456 = x453.o_custkey 
val x457 = Record479(x454, x455, x456) 
x457 }) 
val x463 = { val out1 = x452.map{ case x459 => ({val x461 = x459.c_custkey 
x461}, x459) }
  val out2 = x458.map{ case x460 => ({val x462 = x460.o_custkey 
x462}, x460) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x472 = x463.flatMap{ case (x464, x465) => val x471 = (x465) 
x471 match {
   case (null) => Nil 
   case x470 => List(({val x466 = (x464) 
x466}, {val x467 = x465.o_orderkey 
val x468 = x465.o_orderdate 
val x469 = Record481(x467, x468) 
x469}))
 }
}.groupByKey() 
val x477 = x472.map{ case (x473, x474) => 
   val x475 = x473.c_name 
val x476 = Record482(x475, x474) 
x476 
} 
x477
}
CustOrders.cache
CustOrders.count
def f = { 
 val x497 = L.map(x492 => { val x493 = x492.l_quantity 
val x494 = x492.l_orderkey 
val x495 = x492.l_partkey 
val x496 = Record544(x493, x494, x495) 
x496 }) 
val x502 = P.map(x498 => { val x499 = x498.p_name 
val x500 = x498.p_partkey 
val x501 = Record545(x499, x500) 
x501 }) 
val x507 = { val out1 = x497.map{ case x503 => ({val x505 = x503.l_partkey 
x505}, x503) }
  val out2 = x502.map{ case x504 => ({val x506 = x504.p_partkey 
x506}, x504) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x516 = x507.flatMap{ case (x508, x509) => val x515 = (x508,x509) 
x515 match {
   case (_,null) => Nil
   case x514 => List(({val x510 = x508.l_orderkey 
val x511 = x509.p_name 
val x512 = Record547(x510, x511) 
x512}, {val x513 = x508.l_quantity 
x513}))
 }
}.reduceByKey(_ + _) 
val partcnts = x516
val x517 = partcnts
//partcnts.collect.foreach(println(_))
val x519 = CustOrders 
val x523 = x519.flatMap{ case x520 => x520 match {
   case null => List((x520, null))
   case _ =>
   val x521 = x520.c_orders 
x521 match {
     case x522 => x522.map{ case v2 => (x520, v2) }
  }
 }} 
val x525 = partcnts 
val x531 = { val out1 = x523.map{ case (x526, x527) => ({val x529 = x527.o_orderkey 
x529}, (x526, x527)) }
  val out2 = x525.map{ case x528 => ({val x530 = x528.l_orderkey 
x530}, x528) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x542 = x531.flatMap{ case ((x532, x533), x534) => val x541 = (x532,x533,x534) 
x541 match {
   case (_,null,_) => Nil
case (_,_,null) => Nil
   case x540 => List(({val x535 = x532.c_name 
val x536 = x533.o_orderdate 
val x537 = x534.p_name 
val x538 = Record549(x535, x536, x537) 
x538}, {val x539 = x534._2 
x539}))
 }
}.reduceByKey(_ + _) 
x542.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("Query4Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
