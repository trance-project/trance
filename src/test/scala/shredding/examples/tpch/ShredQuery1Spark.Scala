
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record526(lbl: Unit)
case class Record527(c_name: String, c_custkey: Int)
case class Record529(c__Fc_custkey: Int)
case class Record530(c_name: String, c_orders: Record529)
case class Record531(_1: Unit, _2: Iterable[Record530])
case class Record532(lbl: Record529)
case class Record533(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record535(o__Fo_orderkey: Int)
case class Record536(o_orderdate: String, o_parts: Record535)
case class Record537(_1: Record529, _2: Iterable[Record536])
case class Record538(lbl: Record535)
case class Record539(l_quantity: Double, l_partkey: Int, l_orderkey: Int)
case class Record540(p_name: String, p_partkey: Int)
case class Record542(p_name: String, l_qty: Double)
case class Record543(_1: Record535, _2: Iterable[Record542])
object ShredQuery1Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1Spark"+sf)
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
   val x394 = () 
val x395 = Record526(x394) 
val x396 = List(x395) 
val M_ctx1 = x396
val x399 = M_ctx1 
val x400 = C__D_1 
val x405 = x400.map(x401 => { val x402 = x401.c_name 
val x403 = x401.c_custkey 
val x404 = Record527(x402, x403) 
x404 }) 
val x408 = x405.map{ case c => (x399.head, c) } 
val x418 = x408.map{ case (x409, x410) => val x417 = (x410) 
x417 match {
   case (null) => ({val x411 = (x409) 
x411}, null) 
   case x416 => ({val x411 = (x409) 
x411}, {val x412 = x410.c_name 
val x413 = x410.c_custkey 
val x414 = Record529(x413) 
val x415 = Record530(x412, x414) 
x415})
 }
}.groupByLabel() 
val x423 = x418.map{ case (x419, x420) => 
   val x421 = x419.lbl 
val x422 = Record531(x421, x420) 
x422 
} 
val M_flat1 = x423
//println("M_flat1")
val x424 = M_flat1
//M_flat1.collect.foreach(println(_))
val x426 = M_flat1 
val x430 = x426.flatMap{ case x427 => x427 match {
   case null => List((x427, null))
   case _ =>
   val x428 = x427._2 
x428 match {
     case x429 => x429.map{ case v2 => (x427, v2) }
  }
 }} 
val x435 = x430.map{ case (x431, x432) => 
   val x433 = x432.c_orders 
val x434 = Record532(x433) 
x434 
} 
val x436 = x435.distinct 
val M_ctx2 = x436
//println("M_ctx2")
val x437 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x439 = M_ctx2 
val x440 = O__D_1 
val x446 = x440.map(x441 => { val x442 = x441.o_orderdate 
val x443 = x441.o_orderkey 
val x444 = x441.o_custkey 
val x445 = Record533(x442, x443, x444) 
x445 }) 
val x452 = { val out1 = x439.map{ case x447 => ({val x449 = x447.lbl 
val x450 = x449.c__Fc_custkey 
x450}, x447) }
  val out2 = x446.map{ case x448 => ({val x451 = x448.o_custkey 
x451}, x448) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x462 = x452.map{ case (x453, x454) => val x461 = (x454) 
x461 match {
   case (null) => ({val x455 = (x453) 
x455}, null) 
   case x460 => ({val x455 = (x453) 
x455}, {val x456 = x454.o_orderdate 
val x457 = x454.o_orderkey 
val x458 = Record535(x457) 
val x459 = Record536(x456, x458) 
x459})
 }
}.groupByLabel() 
val x467 = x462.map{ case (x463, x464) => 
   val x465 = x463.lbl 
val x466 = Record537(x465, x464) 
x466 
} 
val M_flat2 = x467
//println("M_flat2")
val x468 = M_flat2
//M_flat2.collect.foreach(println(_))
val x470 = M_flat2 
val x474 = x470.flatMap{ case x471 => x471 match {
   case null => List((x471, null))
   case _ =>
   val x472 = x471._2 
x472 match {
     case x473 => x473.map{ case v2 => (x471, v2) }
  }
 }} 
val x479 = x474.map{ case (x475, x476) => 
   val x477 = x476.o_parts 
val x478 = Record538(x477) 
x478 
} 
val x480 = x479.distinct 
val M_ctx3 = x480
//println("M_ctx3")
val x481 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x483 = M_ctx3 
val x484 = L__D_1 
val x490 = x484.map(x485 => { val x486 = x485.l_quantity 
val x487 = x485.l_partkey 
val x488 = x485.l_orderkey 
val x489 = Record539(x486, x487, x488) 
x489 }) 
val x496 = { val out1 = x483.map{ case x491 => ({val x493 = x491.lbl 
val x494 = x493.o__Fo_orderkey 
x494}, x491) }
  val out2 = x490.map{ case x492 => ({val x495 = x492.l_orderkey 
x495}, x492) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x497 = P__D_1 
val x502 = x497.map(x498 => { val x499 = x498.p_name 
val x500 = x498.p_partkey 
val x501 = Record540(x499, x500) 
x501 }) 
val x508 = { val out1 = x496.map{ case (x503, x504) => ({val x506 = x504.l_partkey 
x506}, (x503, x504)) }
  val out2 = x502.map{ case x505 => ({val x507 = x505.p_partkey 
x507}, x505) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x518 = x508.map{ case ((x509, x510), x511) => val x517 = (x510,x511) 
x517 match {
   case (_,null) => ({val x512 = (x509) 
x512}, null) 
   case x516 => ({val x512 = (x509) 
x512}, {val x513 = x511.p_name 
val x514 = x510.l_quantity 
val x515 = Record542(x513, x514) 
x515})
 }
}.groupByLabel() 
val x523 = x518.map{ case (x519, x520) => 
   val x521 = x519.lbl 
val x522 = Record543(x521, x520) 
x522 
} 
val M_flat3 = x523
//println("M_flat3")
val x524 = M_flat3
//M_flat3.collect.foreach(println(_))
val res = x524.count
   var end0 = System.currentTimeMillis() - start0
   println("ShredQuery1Spark"+sf+","+Config.datapath+","+end0)
 }
}
