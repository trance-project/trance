package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record671(o_orderkey: Int, o_custkey: Int)
case class Record672(c_name: String, c_custkey: Int)
case class Record673(o_orderkey: Int, c_name: String)
case class Record674(s_name: String, s_suppkey: Int)
case class Record675(s__Fs_suppkey: Int)
case class Record676(s_name: String, customers2: Record675)
case class Record677(l_suppkey: Int, l_orderkey: Int)
case class Record679(c_name2: String)
//case class Record680(l__Fl_suppkey: Int)
case class Record681(_1: Record675, _2: Iterable[Record679]) 
case class Record720(c_name: String)
case class Record721(c__Fc_name: String)
case class Record722(c_name: String, suppliers: Record721)
case class Record723(co2__Fc_name2: String)
case class Record724(s_name: String)
case class Record725(_1: Record723, _2: Iterable[Record724])
object ShredQuery6Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6Spark"+sf)
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

   val x611 = O__D_1.map(x607 => { val x608 = x607.o_orderkey
val x609 = x607.o_custkey
val x610 = Record671(x608, x609)
x610 })
val x612 = C__D_1
val x617 = x612.map(x613 => { val x614 = x613.c_name
val x615 = x613.c_custkey
val x616 = Record672(x614, x615)
x616 })
val x622 = { val out1 = x611.map{ case x618 => ({val x620 = x618.o_custkey
x620}, x618) }
  val out2 = x617.map{ case x619 => ({val x621 = x619.c_custkey
x621}, x619) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
}
val x628 = x622.map{ case (x623, x624) =>
   val x625 = x623.o_orderkey
val x626 = x624.c_name
val x627 = Record673(x625, x626)
x627
}
val resultInner__D_1 = x628
val x629 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val x634 = S__D_1.map(x630 => { val x631 = x630.s_name
val x632 = x630.s_suppkey
val x633 = Record674(x631, x632)
x633 })
val x640 = x634.map{ case x635 =>
   val x636 = x635.s_name
val x637 = x635.s_suppkey
val x638 = Record675(x637)
val x639 = Record676(x636, x638)
x639
}
val M_flat1 = x640
val x641 = M_flat1
//M_flat1.collect.foreach(println(_))
val x646 = L__D_1.map(x642 => { val x643 = x642.l_suppkey
val x644 = x642.l_orderkey
val x645 = Record677(x643, x644)
x645 })
val x647 = resultInner__D_1
val x649 = x647
val x654 = { val out1 = x646.map{ case x650 => ({val x652 = x650.l_orderkey
x652}, x650) }
  val out2 = x649.map{ case x651 => ({val x653 = x651.o_orderkey
x653}, x651) }
  out1.join(out2).map{ case (k,v) => v }
}
val x662 = x654.flatMap{ case (x655, x656) => val x661 = (x656)
x661 match {
   case (null) => Nil
   case x660 => List(({val x657 = (x655)
x657}, {val x658 = x656.c_name
val x659 = Record679(x658)
x659}))
 }
}.groupByLabel()
val x668 = x662.map{ case (x663, x664) =>
   val x665 = x663.l_suppkey
val x666 = Record675(x665)
val x667 = Record681(x666, x664)
x667
}
val M_flat2 = x668
val x669 = M_flat2
//M_flat2.collect.foreach(println(_))
val Query2Full__D_1 = M_flat1
Query2Full__D_1.cache
Query2Full__D_1.count
val Query2Full__D_2customers2_1 = M_flat2.map(r => (r._1, r._2))
Query2Full__D_2customers2_1.cache
Query2Full__D_2customers2_1.count

def f = { 
 val x691 = C__D_1.map(x688 => { val x689 = x688.c_name 
val x690 = Record720(x689) 
x690 }) 
val x696 = x691.map{ case x692 => 
   val x693 = x692.c_name 
val x694 = Record721(x693) 
val x695 = Record722(x693, x694) 
x695 
} 
val M_flat1 = x696
val x697 = M_flat1
M_flat1.count
//M_flat1.collect.foreach(println(_))
val x699 = Query2Full__D_1 
val x701 = Query2Full__D_2customers2_1 
val x704 = x701 
val x708 = { val out1 = x699.map{ case x705 => ({val x707 = x705.customers2 
x707}, x705) }
  val out2 = x704.flatMapValues(x706 => x706)//._2.map{case v2 => (x706._1, v2)})
  out2.lookupSkewLeft(out1)//.map{ case (k, v) => v }
} 
val x717 = x708.map{ case (x710, x709) => 
   val x711 = x710.c_name2 
val x712 = Record723(x711) 
val x713 = x709.s_name 
val x714 = Record724(x713) 
val x715 = List(x714) 
val x716 = (x712, x715) 
x716 
}.groupByLabel()
val M_flat2 = x717
val x718 = M_flat2
M_flat2.collect.foreach(println(_))
x718.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery6Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
