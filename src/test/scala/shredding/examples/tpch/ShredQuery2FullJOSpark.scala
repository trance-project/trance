
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record519(s_name: String, s_suppkey: Int)
case class Record520(s__Fs_suppkey: Int)
case class Record521(s_name: String, customers2: Record520)
case class Record522(l_suppkey: Int, l_orderkey: Int)
case class Record523(o_custkey: Int, o_orderkey: Int)
case class Record524(c_name: String, c_custkey: Int)
case class Record526(c_name2: String)
case class Record527(key: Int, value: Iterable[Record526])
object ShredQuery2FullJOSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery2FullJOSpark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L__F = 3
val L__D_1 = tpch.loadLineitem
L__D_1.cache
L__D_1.count
val C__F = 1
val C__D_1 = tpch.loadCustomers
C__D_1.cache
C__D_1.count
val O__F = 2
val O__D_1 = tpch.loadOrders
O__D_1.cache
O__D_1.count
val S__F = 6
val S__D_1 = tpch.loadSupplier
S__D_1.cache
S__D_1.count

   def f = { 
 val x467 = S__D_1.map(x463 => { val x464 = x463.s_name 
val x465 = x463.s_suppkey 
val x466 = Record519(x464, x465) 
x466 }) 
val x473 = x467.map{ case x468 => 
   val x469 = x468.s_name 
val x470 = x468.s_suppkey 
val x471 = Record520(x470) 
val x472 = Record521(x469, x471) 
x472 
} 
val M_flat1 = x473
val x474 = M_flat1
//M_flat1.collect.foreach(println(_))

val x491 = C__D_1 
val x496 = x491.map(x492 => { val x493 = x492.c_name 
val x494 = x492.c_custkey 
val x495 = Record524(x493, x494) 
x495 }) 

val x480 = O__D_1 
val x485 = x480.map(x481 => { val x482 = x481.o_custkey 
val x483 = x481.o_orderkey 
val x484 = Record523(x482, x483) 
x484 }) 

val x502 = { 
  val out1 = x496.map{ 
    case x498 => ({val x500 = x498.c_custkey; x500}, x498) }
  val out2 = x485.map{ 
    case x499 => ({val x501 = x499.o_custkey; x501}, x499) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 

val x479 = L__D_1.map(x475 => { val x476 = x475.l_suppkey 
val x477 = x475.l_orderkey 
val x478 = Record522(x476, x477) 
x478 }) 

val x490 = { 
  val out1 = x479.map{ case x486 => 
    ({val x488 = x486.l_orderkey; x488}, x486) }
  val out2 = x502.map{ 
    case (x486, x487) => ({val x489 = x487.o_orderkey; x489}, (x486, x487)) }
  out1.join(out2).map{ case (k,v) => v }
} 

// (l, (c, o))
val x511 = x490.map{ case (x505, (x503, x504)) => val x510 = (x503,x504) 
x510 match {
   case (_,null) => (x505, null) 
   case x509 => (x505, {val x507 = x503.c_name 
val x508 = Record526(x507) 
x508})
 }
}.groupByKey() 
val x516 = x511.map{ case (x512, x513) => 
   val x514 = x512.l_suppkey 
val x515 = Record527(x514, x513) 
x515 
} 
val M_flat2 = x516
val x517 = M_flat2
//M_flat2.collect.foreach(println(_))
x517.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0
   println("ShredQuery2FullJOSpark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
