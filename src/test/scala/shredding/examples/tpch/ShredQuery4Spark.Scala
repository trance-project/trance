
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record518(lbl: Unit)
case class Record519(c_custkey: Int, c_name: String)
case class Record521(c__Fc_custkey: Int)
case class Record522(c_id: Int, c_name: String, c_orders: Record521)
case class Record523(_1: Record518, _2: (Iterable[Record522]))
case class Record524(lbl: Record521)
case class Record525(o_orderkey: Int, o_orderdate: String, o_custkey: Int)
case class Record527(o__Fo_orderkey: Int)
case class Record528(o_id: Int, o_orderdate: String, o_parts: Record527)
case class Record529(_1: Record524, _2: (Iterable[Record528]))
case class Record530(lbl: Record527)
case class Record531(l_quantity: Double, l_partkey: Int, l_orderkey: Int)
case class Record532(p_name: String, p_partkey: Int)
case class Record534(p_name: String, l_qty: Double)
case class Record535(_1: Record530, _2: (Iterable[Record534]))
case class Record605(c_name: String, p_name: String, month: String)
object ShredQuery4Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4Spark"+sf)
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
   val x386 = () 
val x387 = Record518(x386) 
val x388 = List(x387) 
val M_ctx1 = x388
val x391 = M_ctx1 
val x392 = C__D_1 
val x397 = x392.map(x393 => { val x394 = x393.c_custkey 
val x395 = x393.c_name 
val x396 = Record519(x394, x395) 
x396 }) 
val x400 = x397.map{ case c => (x391.head, c) } 
val x410 = x400.flatMap{ case (x401, x402) => val x409 = (x402) 
x409 match {
   case (null) => Nil 
   case x408 => List(({val x403 = (x401) 
x403}, {val x404 = x402.c_custkey 
val x405 = x402.c_name 
val x406 = Record521(x404) 
val x407 = Record522(x404, x405, x406) 
x407}))
 }
}.groupByLabel() 
val x415 = x410.map{ case (x411, x412) => 
   val x413 = (x412) 
val x414 = Record523(x411, x413) 
x414 
} 
val M_flat1 = x415
//println("M_flat1")
val x416 = M_flat1
//M_flat1.collect.foreach(println(_))
val x418 = M_flat1 
val x422 = x418.flatMap{ case x419 => x419 match {
   case null => List((x419, null))
   case _ =>
   val x420 = x419._2 
x420 match {
     case x421 => x421.map{ case v2 => (x419, v2) }
  }
 }} 
val x427 = x422.map{ case (x423, x424) => 
   val x425 = x424.c_orders 
val x426 = Record524(x425) 
x426 
} 
val x428 = x427.distinct 
val M_ctx2 = x428
//println("M_ctx2")
val x429 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x431 = M_ctx2 
val x432 = O__D_1 
val x438 = x432.map(x433 => { val x434 = x433.o_orderkey 
val x435 = x433.o_orderdate 
val x436 = x433.o_custkey 
val x437 = Record525(x434, x435, x436) 
x437 }) 
val x444 = { val out1 = x431.map{ case x439 => ({val x441 = x439.lbl 
val x442 = x441.c__Fc_custkey 
x442}, x439) }
  val out2 = x438.map{ case x440 => ({val x443 = x440.o_custkey 
x443}, x440) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x454 = x444.flatMap{ case (x445, x446) => val x453 = (x446) 
x453 match {
   case (null) => Nil 
   case x452 => List(({val x447 = (x445) 
x447}, {val x448 = x446.o_orderkey 
val x449 = x446.o_orderdate 
val x450 = Record527(x448) 
val x451 = Record528(x448, x449, x450) 
x451}))
 }
}.groupByLabel() 
val x459 = x454.map{ case (x455, x456) => 
   val x457 = (x456) 
val x458 = Record529(x455, x457) 
x458 
} 
val M_flat2 = x459
//println("M_flat2")
val x460 = M_flat2
//M_flat2.collect.foreach(println(_))
val x462 = M_flat2 
val x466 = x462.flatMap{ case x463 => x463 match {
   case null => List((x463, null))
   case _ =>
   val x464 = x463._2 
x464 match {
     case x465 => x465.map{ case v2 => (x463, v2) }
  }
 }} 
val x471 = x466.map{ case (x467, x468) => 
   val x469 = x468.o_parts 
val x470 = Record530(x469) 
x470 
} 
val x472 = x471.distinct 
val M_ctx3 = x472
//println("M_ctx3")
val x473 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x475 = M_ctx3 
val x476 = L__D_1 
val x482 = x476.map(x477 => { val x478 = x477.l_quantity 
val x479 = x477.l_partkey 
val x480 = x477.l_orderkey 
val x481 = Record531(x478, x479, x480) 
x481 }) 
val x488 = { val out1 = x475.map{ case x483 => ({val x485 = x483.lbl 
val x486 = x485.o__Fo_orderkey 
x486}, x483) }
  val out2 = x482.map{ case x484 => ({val x487 = x484.l_orderkey 
x487}, x484) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x489 = P__D_1 
val x494 = x489.map(x490 => { val x491 = x490.p_name 
val x492 = x490.p_partkey 
val x493 = Record532(x491, x492) 
x493 }) 
val x500 = { val out1 = x488.map{ case (x495, x496) => ({val x498 = x496.l_partkey 
x498}, (x495, x496)) }
  val out2 = x494.map{ case x497 => ({val x499 = x497.p_partkey 
x499}, x497) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x510 = x500.flatMap{ case ((x501, x502), x503) => val x509 = (x502,x503) 
x509 match {
   case (_,null) => Nil 
   case x508 => List(({val x504 = (x501) 
x504}, {val x505 = x503.p_name 
val x506 = x502.l_quantity 
val x507 = Record534(x505, x506) 
x507}))
 }
}.groupByLabel() 
val x515 = x510.map{ case (x511, x512) => 
   val x513 = (x512) 
val x514 = Record535(x511, x513) 
x514 
} 
val M_flat3 = x515
//println("M_flat3")
val x516 = M_flat3
//M_flat3.collect.foreach(println(_))
val res = x516 
 
val Query4__F = M_ctx1
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
     val x546 = () 
val x547 = Record518(x546) 
val x548 = List(x547) 
val M_ctx1 = x548
val x551 = M_ctx1 
val x552 = Query4__D_1 
val x556 = x552.filter(x553 => { val x554 = x553.c_id 
val x555 = x554 < 1000 
x555 }) 
val x559 = x556.map{ case c => (x551.head, c) } 
val x561 = Query4__D_2c_orders_1 
val x566 = x561 
val x571 = { val out1 = x559.map{ case (a, null) => (null, (a, null)); case (x567, x568) => ({val x570 = x568.c_orders 
x570}, (x567, x568)) }
  val out2 = x566.flatMap(x569 => x569._2.map{case v2 => (x569._1.lbl, v2)})
  out1.outerLookup(out2)
} 
val x572 = Query4__D_2c_orders_2o_parts 
val x574 = x572 
val x576 = x574 
val x582 = { val out1 = x571.map{ case (a, null) => (null, (a, null)); case ((x577, x578), x579) => ({val x581 = x579.o_parts 
x581}, ((x577, x578), x579)) }
  val out2 = x576.flatMap(x580 => x580._2.map{case v2 => (x580._1.lbl, v2)})
  out1.outerLookup(out2)
} 
val x595 = x582.flatMap{ case (((x583, x584), x585), x586) => val x594 = (x583,x584,x586,x585) 
x594 match {
   case (_,null,_,_) => Nil
case (_,_,null,_) => Nil
case (_,_,_,null) => Nil
   case x593 => List(({val x587 = x584.c_name 
val x588 = x586.p_name 
val x589 = x585.o_orderdate 
val x590 = Record605(x587, x588, x589) 
val x591 = (x583,x590) 
x591}, {val x592 = x586.l_qty 
x592}))
 }
}.reduceByKey(_ + _) 
val x601 = x595.map{ case ((x596, x597), x598) => 
  (x596, (x597,x598))
}.groupByLabel() 
val M_flat1 = x601
//println("M_flat1")
val x602 = M_flat1
//M_flat1.collect.foreach(println(_))
val res = x602.count
   }
   f
   var end0 = System.currentTimeMillis() - start0
   println("ShredQuery4Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
