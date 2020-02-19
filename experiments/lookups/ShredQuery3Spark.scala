
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
import sprkloader.SkewDictRDD._
import sprkloader.DomainRDD._
case class Record165(lbl: Unit)
case class Record166(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record167(p_name: String, p_partkey: Int)
case class Record168(l_orderkey: Int, p_partkey: Int, l_qty: Double)
case class Record169(c_name: String, c_custkey: Int)
case class Record170(c__Fc_custkey: Int)
case class Record171(c_name: String, c_orders: Record170)
case class Record172(lbl: Record170)
case class Record173(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record175(o__Fo_orderkey: Int)
case class Record176(o_orderdate: String, o_parts: Record175)
case class Record177(lbl: Record175)
case class Record179(p_partkey: Int, l_qty: Double)
case class Record244(p_retailprice: Double, p_name: String)
case class Record246(c_name: String, p_name: String)
object ShredQuery3Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery3Spark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()

   val tpch = TPCHLoader(spark)
val L__F = 3
val L__D_1 = tpch.loadLineitemProjBzip
L__D_1.cache
spark.sparkContext.runJob(L__D_1, (iter: Iterator[_]) => {})
val P__F = 4
val P__D_1 = tpch.loadPartProj4
P__D_1.cache
spark.sparkContext.runJob(P__D_1, (iter: Iterator[_]) => {})
val C__F = 1
val C__D_1 = tpch.loadCustomersProj
C__D_1.cache
spark.sparkContext.runJob(C__D_1, (iter: Iterator[_]) => {})
val O__F = 2
val O__D_1 = tpch.loadOrdersProjBzip
O__D_1.cache
spark.sparkContext.runJob(O__D_1, (iter: Iterator[_]) => {})

val x56 = L__D_1
val x61 = P__D_1
val x63 = x56.map{ case x62 => ({val x64 = x62.l_partkey 
x64}, x62) }
val x64 = x63.joinSkew(x61, (p: PartProj4) => p.p_partkey)
 
val x73 = x64.map{ case (x67, x68) => 
   val x69 = x67.l_orderkey 
val x70 = x68.p_partkey 
val x71 = x67.l_quantity 
val x72 = Record168(x69, x70, x71) 
x72 
} 
val ljp__D_1 = x73
val x74 = ljp__D_1

val x80 = C__D_1.map(x76 => { val x77 = x76.c_name 
val x78 = x76.c_custkey 
val x79 = Record169(x77, x78) 
x79 }) 
val x86 = x80.map{ case x81 => 
   val x82 = x81.c_name 
val x83 = x81.c_custkey 
val x84 = Record170(x83) 
val x85 = Record171(x82, x84) 
x85 
} 
val M__D_1 = x86
val x87 = M__D_1

val x89 = M__D_1.createDomain(l => Record172(l.c_orders))
val M_ctx2 = x89
val x95 = M_ctx2

val x97 = M_ctx2 
val x103 = O__D_1.map(x98 => { val x99 = x98.o_orderdate 
val x100 = x98.o_orderkey 
val x101 = x98.o_custkey 
val x102 = Record173(x99, x100, x101) 
x102 }) 
val x104 = x103.map{ case x105 => ({val x108 = x105.o_custkey 
x108}, x105) }
val x109 = x104.joinDomainSkew(x97, (l: Record172) => l.lbl.c__Fc_custkey)

val x119 = x109.map{ case (x111, x110) => 
   ({val x112 = (x110) 
x112.lbl}, {val x113 = x111.o_orderdate 
val x114 = x111.o_orderkey 
val x115 = Record175(x114) 
val x116 = Record176(x113, x115) 
x116})
}.groupByLabel() 

val M__D_2 = x119
val x125 = M__D_2

val x127 = M__D_2.createDomain(l => Record177(l.o_parts))
val M_ctx3 = x127
val x138 = M_ctx3

val x140 = M_ctx3 
val x142 = ljp__D_1 
val x145 = x142.map{ case x144 => ({val x147 = x144.l_orderkey 
x147}, x144) }
val x148 = x145.joinDomainSkew(x140, (l: Record177) => l.lbl.o__Fo_orderkey)
   
val x157 = x148.map{ case (x150, x149) => 
   ({val x151 = (x149) 
x151.lbl}, {val x152 = x150.p_partkey 
val x153 = x150.l_qty 
val x154 = Record179(x152, x153) 
x154})
}.groupByLabel() 

val M__D_3 = x157
val x163 = M__D_3
//M__D_3.collect.foreach(println(_))
val Query1__D_1 = M__D_1
Query1__D_1.cache
spark.sparkContext.runJob(Query1__D_1, (iter: Iterator[_]) => {})
val Query1__D_2c_orders_1 = M__D_2
Query1__D_2c_orders_1.cache
spark.sparkContext.runJob(Query1__D_2c_orders_1, (iter: Iterator[_]) => {})
val Query1__D_2c_orders_2o_parts_1 = M__D_3
Query1__D_2c_orders_2o_parts_1.cache
spark.sparkContext.runJob(Query1__D_2c_orders_2o_parts_1, (iter: Iterator[_]) => {})

 def f = {
 
var start0 = System.currentTimeMillis()
val x207 = Query1__D_1 
val x209 = Query1__D_2c_orders_1
val x210 = Query1__D_2c_orders_2o_parts_1
val x221 = P__D_1
val x222 = x209.mapPartitions( it =>
  it.flatMap{ case (lbl, bag) => bag.map(b => (b.o_parts, lbl)) }, true)

val x223 = x210.flatMap{
  case (lbl, bag) => bag.map(p => (lbl, p.p_partkey) -> p.l_qty)
}.reduceByKey(_+_).map{
  case ((lbl, pk), tot) => lbl -> (pk, tot)
}
val x224 = x222.cogroup(x223).flatMap{pair =>
  for (l <- pair._2._1.iterator; (pk, tot) <- pair._2._2.iterator) yield (pk, (l, tot))
}
val x225 = x224.joinSkew(x221, (p: PartProj4) => p.p_partkey).map{
  case ((lbl, tot), p) => (lbl, p.p_name) -> tot*p.p_retailprice
}.reduceByKey(_+_).map{
  case ((lbl, pname), tot) => lbl -> (pname, tot)
}
/**val x222 = x210.flatMap{
  case (lbl, bag) => bag.map(p => p.p_partkey -> (lbl, p.l_qty))
}.joinSkew(x221, (p: PartProj4) => p.p_partkey).mapPartitions(
  it => it.map{ case ((lbl, qty), p) => (lbl, p.p_name) -> qty*p.p_retailprice}
 ).reduceByKey(_+_).map{
  case ((lbl, pname), tot) => lbl -> (pname, tot)
}

val x223 = x209.mapPartitions( it =>
  it.flatMap{ case (lbl, bag) => bag.map(b => (b.o_parts, (lbl, b.o_orderdate))) }, true)
val x224 = x223.cogroup(x222).flatMap{ pair => 
  for ((lbl, date) <- pair._2._1.iterator; (pname, tot) <- pair._2._2.iterator) yield lbl -> (pname, tot)
}**/

val x226 = x207.map(c => c.c_orders -> c.c_name).join(x225).mapPartitions(
  it => it.map{ case (_, (cname, (pname, tot))) => (cname, pname) -> tot}, true).reduceByKey(_+_)
val M__D_1 = x226
val x242 = M__D_1
//M__D_1.collect.foreach(println(_))
spark.sparkContext.runJob(M__D_1, (iter: Iterator[_]) => {})
var end0 = System.currentTimeMillis() - start0
println("ShredQuery3Spark,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
println("ShredQuery3Spark,"+sf+","+Config.datapath+",0,unshredding,"+spark.sparkContext.applicationId)
    
}
f
 }
}
