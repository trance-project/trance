
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record653(s_name: String, s_suppkey: Int)
case class Record654(s__Fs_suppkey: Int)
case class Record655(s_name: String, customers2: Record654)
case class Record656(l_suppkey: Int, l_orderkey: Int)
case class Record657(o_custkey: Int, o_orderkey: Int)
case class Record658(c_name: String, c_custkey: Int)
case class Record660(c_name2: String)
case class Record661(l__Fl_suppkey: Int)
case class Record662(_1: Record661, _2: Iterable[Record660])
case class Record701(c_name: String)
case class Record702(c__Fc_name: String)
case class Record703(c_name: String, suppliers: Record702)
case class Record704(co2__Fc_name2: String)
case class Record705(s_name: String)
case class Record706(_1: Record704, _2: Iterable[Record705])
object ShredQuery6FullSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6FullSpark"+sf)
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

   val x600 = S__D_1.map(x596 => { val x597 = x596.s_name 
val x598 = x596.s_suppkey 
val x599 = Record653(x597, x598) 
x599 }) 
val x606 = x600.map{ case x601 => 
   val x602 = x601.s_name 
val x603 = x601.s_suppkey 
val x604 = Record654(x603) 
val x605 = Record655(x602, x604) 
x605 
} 
val M_flat1 = x606
val x607 = M_flat1
//M_flat1.collect.foreach(println(_))
val x612 = L__D_1.map(x608 => { val x609 = x608.l_suppkey 
val x610 = x608.l_orderkey 
val x611 = Record656(x609, x610) 
x611 }) 
val x613 = O__D_1 
val x618 = x613.map(x614 => { val x615 = x614.o_custkey 
val x616 = x614.o_orderkey 
val x617 = Record657(x615, x616) 
x617 }) 
val x623 = { val out1 = x612.map{ case x619 => ({val x621 = x619.l_orderkey 
x621}, x619) }
  val out2 = x618.map{ case x620 => ({val x622 = x620.o_orderkey 
x622}, x620) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x624 = C__D_1 
val x629 = x624.map(x625 => { val x626 = x625.c_name 
val x627 = x625.c_custkey 
val x628 = Record658(x626, x627) 
x628 }) 
val x635 = { val out1 = x623.map{ case (x630, x631) => ({val x633 = x631.o_custkey 
x633}, (x630, x631)) }
  val out2 = x629.map{ case x632 => ({val x634 = x632.c_custkey 
x634}, x632) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x644 = x635.flatMap{ case ((x636, x637), x638) => val x643 = (x637,x638) 
x643 match {
   case (_,null) => Nil 
   case x642 => List(({val x639 = (x636) 
x639}, {val x640 = x638.c_name 
val x641 = Record660(x640) 
x641}))
 }
}.groupByKey() 
val x650 = x644.map{ case (x645, x646) => 
   val x647 = x645.l_suppkey 
val x648 = Record661(x647) 
val x649 = Record662(x648, x646) 
x649 
} 
val M_flat2 = x650
val x651 = M_flat2
//M_flat2.collect.foreach(println(_))
val Query2Full__D_1 = M_flat1
Query2Full__D_1.cache
Query2Full__D_1.count
val Query2Full__D_2customers2_1 = M_flat2
Query2Full__D_2customers2_1.cache
Query2Full__D_2customers2_1.count
def f = { 
 val x672 = C__D_1.map(x669 => { val x670 = x669.c_name 
val x671 = Record701(x670) 
x671 }) 
val x677 = x672.map{ case x673 => 
   val x674 = x673.c_name 
val x675 = Record702(x674) 
val x676 = Record703(x674, x675) 
x676 
} 
val M_flat1 = x677
val x678 = M_flat1
//M_flat1.collect.foreach(println(_))
val x680 = Query2Full__D_1 
val x682 = Query2Full__D_2customers2_1 
val x685 = x682 
val x689 = { val out1 = x680.map{ case x686 => ({val x688 = x686.customers2 
x688}, x686) }
  val out2 = x685.flatMap(x687 => x687._2.map{case v2 => (x687._1, v2)})
  out2.joinSkewLeft(out1).map{ case (k, v) => v }
} 
val x698 = x689.map{ case (x691, x690) => 
   val x692 = x691.c_name2 
val x693 = Record704(x692) 
val x694 = x690.s_name 
val x695 = Record705(x694) 
val x696 = List(x695) 
val x697 = Record706(x693, x696) 
x697 
} 
val M_flat2 = x698
val x699 = M_flat2
//M_flat2.collect.foreach(println(_))
x699.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery6FullSpark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
