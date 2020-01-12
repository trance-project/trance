
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record112(lbl: Unit)
case class Record113(o_orderkey: Int, o_custkey: Int)
case class Record114(c_name: String, c_custkey: Int)
case class Record115(o_orderkey: Int, c_name: String)
case class Record116(s__Fs_suppkey: Int)
case class Record117(s_name: String, customers2: Record116)
case class Record118(lbl: Record116)
case class Record119(l_orderkey: Int, l_suppkey: Int)
case class Record121(c_name2: String)
case class Record204(c_name: String, s_name: String)
case class Record205(c__Fc_name: String)
case class Record206(c_name: String, suppliers: Record205)
case class Record207(lbl: Record205)
case class Record209(s_name: String)
case class Record231(c_name: String, suppliers: Iterable[Record209])
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

   val x33 = () 
val x34 = Record112(x33) 
val x35 = List(x34) 
val resultInner_ctx1 = x35
val x36 = resultInner_ctx1
//resultInner_ctx1.collect.foreach(println(_))
val x41 = O__D_1.map(x37 => { val x38 = x37.o_orderkey 
val x39 = x37.o_custkey 
val x40 = Record113(x38, x39) 
x40 }) 
val x46 = C__D_1.map(x42 => { val x43 = x42.c_name 
val x44 = x42.c_custkey 
val x45 = Record114(x43, x44) 
x45 }) 
val x51 = { val out1 = x41.map{ case x47 => ({val x49 = x47.o_custkey 
x49}, x47) }
  val out2 = x46.map{ case x48 => ({val x50 = x48.c_custkey 
x50}, x48) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x57 = x51.map{ case (x52, x53) => 
   val x54 = x52.o_orderkey 
val x55 = x53.c_name 
val x56 = Record115(x54, x55) 
x56 
} 
val resultInner__D_1 = x57
val x58 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val M_ctx1 = x35
val x59 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x65 = S__D_1.map{ case x60 => 
   val x61 = x60.s_name 
val x62 = x60.s_suppkey 
val x63 = Record116(x62) 
val x64 = Record117(x61, x63) 
x64 
} 
val M__D_1 = x65
val x66 = M__D_1
//M__D_1.collect.foreach(println(_))
val x68 = M__D_1 
val x72 = x68.map{ case x69 => 
   val x70 = x69.customers2 
val x71 = Record118(x70) 
x71 
} 
val x73 = x72.distinct 
val customers2_ctx1 = x73
val x74 = customers2_ctx1
//customers2_ctx1.collect.foreach(println(_))
val x76 = customers2_ctx1 
val x81 = L__D_1.map(x77 => { val x78 = x77.l_orderkey 
val x79 = x77.l_suppkey 
val x80 = Record119(x78, x79) 
x80 }) 
val x87 = { val out1 = x76.map{ case x82 => ({val x84 = x82.lbl 
val x85 = x84.s__Fs_suppkey 
x85}, x82) }
  val out2 = x81.map{ case x83 => ({val x86 = x83.l_suppkey 
x86}, x83) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x89 = resultInner__D_1 
val x95 = { val out1 = x87.map{ case (x90, x91) => ({val x93 = x91.l_orderkey 
x93}, (x90, x91)) }
  val out2 = x89.map{ case x92 => ({val x94 = x92.o_orderkey 
x94}, x92) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x104 = x95.flatMap{ case ((x96, x97), x98) => val x103 = (x97,x98) 
x103 match {
   case (_,null) => Nil 
   case x102 => List(({val x99 = (x96) 
x99}, {val x100 = x98.c_name 
val x101 = Record121(x100) 
x101}))
 }
}.groupByLabel() 
val x109 = x104.map{ case (x105, x106) => 
   val x107 = x105.lbl 
val x108 = (x107, x106) 
x108 
} 
val customers2__D_1 = x109
val x110 = customers2__D_1
//customers2__D_1.collect.foreach(println(_))
val Query2__D_1 = M__D_1//M_flat1
Query2__D_1.cache
Query2__D_1.count
val Query2__D_2customers2_1 = customers2__D_1//M_flat2
Query2__D_2customers2_1.cache
Query2__D_2customers2_1.count
 def f = {
 
var start0 = System.currentTimeMillis()
val x147 = () 
val x148 = Record112(x147) 
val x149 = List(x148) 
val cflat_ctx1 = x149
val x150 = cflat_ctx1
//cflat_ctx1.collect.foreach(println(_))
val x152 = Query2__D_1 
val x156 = { val out1 = x152.map{ case x153 => ({val x155 = x153.customers2 
x155}, x153) }
out1.lookupSkewLeft(Query2__D_2customers2_1.flatMapValues(identity))
/**.flatMap{ pair =>
 for (k <- pair._2._1.iterator; w <- pair._2._2.iterator) yield (k,w)
}**/
}
         
val x162 = x156.map{ case (x157, x158) => 
   val x159 = x158.c_name2 
val x160 = x157.s_name 
val x161 = Record204(x159, x160) 
x161 
} 
val cflat__D_1 = x162
val x163 = cflat__D_1
//cflat__D_1.collect.foreach(println(_))
val M_ctx1 = x149
val x164 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x169 = C__D_1.map{ case x165 => 
   val x166 = x165.c_name 
val x167 = Record205(x166) 
val x168 = Record206(x166, x167) 
x168 
} 
val M__D_1 = x169
val x170 = M__D_1
//M__D_1.collect.foreach(println(_))
/**val x172 = M__D_1 
val x176 = x172.map{ case x173 => 
   val x174 = x173.suppliers 
val x175 = Record207(x174) 
x175 
} 
val x177 = x176.distinct 
val suppliers_ctx1 = x177
val x178 = suppliers_ctx1
//suppliers_ctx1.collect.foreach(println(_))
val x180 = suppliers_ctx1 
val x182 = cflat__D_1 
val x188 = { val out1 = x180.map{ case x183 => ({val x185 = x183.lbl 
val x186 = x185.c__Fc_name 
x186}, x183) }
  val out2 = x182.map{ case x184 => ({val x187 = x184.c_name 
x187}, x184) }
  out1.join(out2).map{ case (k,v) => v }
} **/
val x188 = cflat__D_1
val x196 = x188.flatMap{ case (x190) => val x195 = (x190) 
x195 match {
   case (null) => Nil 
   case x194 => List(({val x191 = Record207(Record205(x190.c_name)) 
x191}, {val x192 = x190.s_name 
val x193 = Record209(x192) 
x193}))
 }
}.groupByLabel() 
val x201 = x196.map{ case (x197, x198) => 
   val x199 = x197.lbl 
val x200 = (x199, x198) 
x200 
} 
val suppliers__D_1 = x201
val x202 = suppliers__D_1
//suppliers__D_1.collect.foreach(println(_))
x202.count
var end0 = System.currentTimeMillis() - start0
println("ShredQuery6Spark,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    

var start1 = System.currentTimeMillis()
val x219 = M__D_1 
val x223 = { val out1 = x219.map{ case x220 => ({val x222 = x220.suppliers 
x222}, x220) }
out1.cogroup(suppliers__D_1).flatMap{
 case (_, (left, x221)) => left.map{ case x220 => (x220, x221.flatten) }}
}
         
val x228 = x223.map{ case (x224, x225) => 
   val x226 = x224.c_name 
val x227 = Record231(x226, x225) 
x227 
} 
val newM__D_1 = x228
val x229 = newM__D_1
//newM__D_1.collect.foreach(println(_))
x229.count
var end1 = System.currentTimeMillis() - start1
println("ShredQuery6Spark,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("ShredQuery6Spark"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
