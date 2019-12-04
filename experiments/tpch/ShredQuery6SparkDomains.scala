
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record481(lbl: Unit)
case class Record482(o_orderkey: Int, o_custkey: Int)
case class Record483(c_name: String, c_custkey: Int)
case class Record484(o_orderkey: Int, c_name: String)
case class Record485(s__Fs_suppkey: Int)
case class Record486(s_name: String, customers2: Record485)
case class Record487(lbl: Record485)
case class Record488(l_orderkey: Int, l_suppkey: Int)
case class Record490(c_name2: String)
case class Record572(c_name: String, s_name: String)
case class Record573(c__Fc_name: String)
case class Record574(c_name: String, suppliers: Record573)
case class Record575(lbl: Record573)
case class Record577(s_name: String)
object ShredQuery6SparkDomains {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6SparkDomains"+sf)
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

   val x403 = () 
val x404 = Record481(x403) 
val x405 = List(x404) 
val M_ctx1 = x405
val x406 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x411 = O__D_1.map(x407 => { val x408 = x407.o_orderkey 
val x409 = x407.o_custkey 
val x410 = Record482(x408, x409) 
x410 }) 
val x416 = C__D_1.map(x412 => { val x413 = x412.c_name 
val x414 = x412.c_custkey 
val x415 = Record483(x413, x414) 
x415 }) 
val x421 = { val out1 = x411.map{ case x417 => ({val x419 = x417.o_custkey 
x419}, x417) }
  val out2 = x416.map{ case x418 => ({val x420 = x418.c_custkey 
x420}, x418) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x427 = x421.map{ case (x422, x423) => 
   val x424 = x422.o_orderkey 
val x425 = x423.c_name 
val x426 = Record484(x424, x425) 
x426 
} 
val resultInner__D_1 = x427
val x428 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val x434 = S__D_1.map{ case x429 => 
   val x430 = x429.s_name 
val x431 = x429.s_suppkey 
val x432 = Record485(x431) 
val x433 = Record486(x430, x432) 
x433 
} 
val M_flat1 = x434
val x435 = M_flat1
//M_flat1.collect.foreach(println(_))
val x437 = M_flat1 
val x441 = x437.map{ case x438 => 
   val x439 = x438.customers2 
val x440 = Record487(x439) 
x440 
} 
val x442 = x441.distinct 
val M_ctx2 = x442
val x443 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x445 = M_ctx2 
val x450 = L__D_1.map(x446 => { val x447 = x446.l_orderkey 
val x448 = x446.l_suppkey 
val x449 = Record488(x447, x448) 
x449 }) 
val x456 = { val out1 = x445.map{ case x451 => ({val x453 = x451.lbl 
val x454 = x453.s__Fs_suppkey 
x454}, x451) }
  val out2 = x450.map{ case x452 => ({val x455 = x452.l_suppkey 
x455}, x452) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x458 = resultInner__D_1 
val x464 = { val out1 = x456.map{ case (x459, x460) => ({val x462 = x460.l_orderkey 
x462}, (x459, x460)) }
  val out2 = x458.map{ case x461 => ({val x463 = x461.o_orderkey 
x463}, x461) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x473 = x464.flatMap{ case ((x465, x466), x467) => val x472 = (x466,x467) 
x472 match {
   case (_,null) => Nil 
   case x471 => List(({val x468 = (x465) 
x468}, {val x469 = x467.c_name 
val x470 = Record490(x469) 
x470}))
 }
}.groupByLabel() 
val x478 = x473.map{ case (x474, x475) => 
   val x476 = x474.lbl 
val x477 = (x476, x475) 
x477 
} 
val M_flat2 = x478
val x479 = M_flat2
//M_flat2.collect.foreach(println(_))
val Query2__D_1 = M_flat1
Query2__D_1.cache
Query2__D_1.count
val Query2__D_2customers2_1 = M_flat2
Query2__D_2customers2_1.cache
Query2__D_2customers2_1.count
def f = { 
 val x516 = () 
val x517 = Record481(x516) 
val x518 = List(x517) 
val M_ctx1 = x518
val x519 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x521 = Query2__D_1 
val x525 = { val out1 = x521.map{ case x522 => ({val x524 = x522.customers2 
x524}, x522) }
  val out2 = Query2__D_2customers2_1.flatMapValues(identity)
  out2.lookupSkewLeft(out1)
} 
val x531 = x525.map{ case (x527, x526) => 
   val x528 = x527.c_name2 
val x529 = x526.s_name 
val x530 = Record572(x528, x529) 
x530 
} 
val cflat__D_1 = x531
val x532 = cflat__D_1
//cflat__D_1.collect.foreach(println(_))
val x537 = C__D_1.map{ case x533 => 
   val x534 = x533.c_name 
val x535 = Record573(x534) 
val x536 = Record574(x534, x535) 
x536 
} 
val M_flat1 = x537
val x538 = M_flat1
//M_flat1.collect.foreach(println(_))
val x540 = M_flat1 
val x544 = x540.map{ case x541 => 
   val x542 = x541.suppliers 
val x543 = Record575(x542) 
x543 
} 
val x545 = x544.distinct 
val M_ctx2 = x545
val x546 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x548 = M_ctx2 
val x550 = cflat__D_1 
val x556 = { val out1 = x548.map{ case x551 => ({val x553 = x551.lbl 
val x554 = x553.c__Fc_name 
x554}, x551) }
  val out2 = x550.map{ case x552 => ({val x555 = x552.c_name 
x555}, x552) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x564 = x556.flatMap{ case (x557, x558) => val x563 = (x558) 
x563 match {
   case (null) => Nil 
   case x562 => List(({val x559 = (x557) 
x559}, {val x560 = x558.s_name 
val x561 = Record577(x560) 
x561}))
 }
}.groupByLabel() 
val x569 = x564.map{ case (x565, x566) => 
   val x567 = x565.lbl 
val x568 = (x567, x566) 
x568 
} 
val M_flat2 = x569
val x570 = M_flat2
//M_flat2.collect.foreach(println(_))
x570.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery6SparkDomains"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
