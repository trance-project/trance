
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record417(o_orderkey: Int, o_custkey: Int)
case class Record418(c_name: String, c_custkey: Int)
case class Record419(o_orderkey: Int, c_name: String)
case class Record420(s__Fs_suppkey: Int)
case class Record421(s_name: String, customers2: Record420)
case class Record422(l_suppkey: Int, l_orderkey: Int)
case class Record424(c_name2: String)
case class Record465(c_name: String, s_name: String)
case class Record466(c__Fc_name: String)
case class Record467(c_name: String, suppliers: Record466)
case class Record468(s_name: String)
object ShredQuery6SparkOpt {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6SparkOpt"+sf)
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

   val x364 = O__D_1.map(x360 => { val x361 = x360.o_orderkey 
val x362 = x360.o_custkey 
val x363 = Record417(x361, x362) 
x363 }) 
val x369 = C__D_1.map(x365 => { val x366 = x365.c_name 
val x367 = x365.c_custkey 
val x368 = Record418(x366, x367) 
x368 }) 
val x374 = { val out1 = x364.map{ case x370 => ({val x372 = x370.o_custkey 
x372}, x370) }
  val out2 = x369.map{ case x371 => ({val x373 = x371.c_custkey 
x373}, x371) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x380 = x374.map{ case (x375, x376) => 
   val x377 = x375.o_orderkey 
val x378 = x376.c_name 
val x379 = Record419(x377, x378) 
x379 
} 
val resultInner__D_1 = x380
val x381 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val x387 = S__D_1.map{ case x382 => 
   val x383 = x382.s_name 
val x384 = x382.s_suppkey 
val x385 = Record420(x384) 
val x386 = Record421(x383, x385) 
x386 
} 
val M_flat1 = x387
val x388 = M_flat1
//M_flat1.collect.foreach(println(_))
val x393 = L__D_1.map(x389 => { val x390 = x389.l_suppkey 
val x391 = x389.l_orderkey 
val x392 = Record422(x390, x391) 
x392 }) 
val x395 = resultInner__D_1 
val x400 = { val out1 = x393.map{ case x396 => ({val x398 = x396.l_orderkey 
x398}, x396) }
  val out2 = x395.map{ case x397 => ({val x399 = x397.o_orderkey 
x399}, x397) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x408 = x400.flatMap{ case (x401, x402) => val x407 = (x402) 
x407 match {
   case (null) => Nil 
   case x406 => List(({val x403 = (x401) 
x403}, {val x404 = x402.c_name 
val x405 = Record424(x404) 
x405}))
 }
}.groupByLabel() 
val x414 = x408.map{ case (x409, x410) => 
   val x411 = x409.l_suppkey 
val x412 = Record420(x411) 
val x413 = (x412, x410) 
x413 
} 
val M_flat2 = x414
val x415 = M_flat2
//M_flat2.collect.foreach(println(_))
val Query2__D_1 = M_flat1
Query2__D_1.cache
Query2__D_1.count
val Query2__D_2customers2_1 = M_flat2
Query2__D_2customers2_1.cache
Query2__D_2customers2_1.count
def f = { 
 val x437 = Query2__D_1 
val x441 = { val out1 = x437.map{ case x438 => ({val x440 = x438.customers2 
x440}, x438) }
  val out2 = Query2__D_2customers2_1.flatMapValues(identity)
  out2.lookupSkewLeft(out1)
} 
val x447 = x441.map{ case (x443, x442) => 
   val x444 = x443.c_name2 
val x445 = x442.s_name 
val x446 = Record465(x444, x445) 
x446 
} 
val cflat__D_1 = x447
val x448 = cflat__D_1
//cflat__D_1.collect.foreach(println(_))
val x453 = C__D_1.map{ case x449 => 
   val x450 = x449.c_name 
val x451 = Record466(x450) 
val x452 = Record467(x450, x451) 
x452 
} 
val M_flat1 = x453
val x454 = M_flat1
M_flat1.count
//M_flat1.collect.foreach(println(_))
val x462 = cflat__D_1.map{ case x455 => 
   val x456 = x455.c_name 
val x457 = Record466(x456) 
val x458 = x455.s_name 
val x459 = Record468(x458) 
val x460 = List(x459) 
val x461 = (x457, x460) 
x461 
}.groupByLabel() 
val M_flat2 = x462
val x463 = M_flat2
x463.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery6SparkOpt"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
