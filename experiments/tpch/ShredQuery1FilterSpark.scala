
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record169(lbl: Unit)
case class Record170(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record171(p_name: String, p_partkey: Int)
case class Record172(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record173(c_name: String, c_custkey: Int)
case class Record174(c__Fc_custkey: Int)
case class Record175(c_name: String, c_orders: Record174)
case class Record176(lbl: Record174)
case class Record177(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record179(o__Fo_orderkey: Int)
case class Record180(o_orderdate: String, o_parts: Record179)
case class Record181(lbl: Record179)
case class Record183(p_name: String, l_qty: Double)
object ShredQuery1FilterSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1FilterSpark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L__F = 3
val L__D_1 = tpch.loadLineitem
L__D_1.cache
L__D_1.count
val P__F = 4
val P__D_1 = tpch.loadPart
P__D_1.cache
P__D_1.count
val C__F = 1
val C__D_1 = tpch.loadCustomers
C__D_1.cache
C__D_1.count
val O__F = 2
val O__D_1 = tpch.loadOrders
O__D_1.cache
O__D_1.count

    def f = {
 
var start0 = System.currentTimeMillis()
val x47 = () 
val x48 = Record169(x47) 
val x49 = List(x48) 
val ljp_ctx1 = x49
val x50 = ljp_ctx1
//ljp_ctx1.collect.foreach(println(_))
val x56 = L__D_1.map(x51 => { val x52 = x51.l_orderkey 
val x53 = x51.l_quantity 
val x54 = x51.l_partkey 
val x55 = Record170(x52, x53, x54) 
x55 }) 
val x61 = P__D_1.map(x57 => { val x58 = x57.p_name 
val x59 = x57.p_partkey 
val x60 = Record171(x58, x59) 
x60 }) 
val x66 = { val out1 = x56.map{ case x62 => ({val x64 = x62.l_partkey 
x64}, x62) }
  val out2 = x61.map{ case x63 => ({val x65 = x63.p_partkey 
x65}, x63) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x73 = x66.map{ case (x67, x68) => 
   val x69 = x67.l_orderkey 
val x70 = x68.p_name 
val x71 = x67.l_quantity 
val x72 = Record172(x69, x70, x71) 
x72 
} 
val ljp__D_1 = x73
val x74 = ljp__D_1
//ljp__D_1.collect.foreach(println(_))
val M_ctx1 = x49
val x75 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x82 = C__D_1.filter(x76 => { val x77 = x76.c_custkey 
val x78 = x77 <= 1500000 
x78 }).map(x76 => { val x79 = x76.c_name 
val x80 = x76.c_custkey 
val x81 = Record173(x79, x80) 
x81 }) 
val x88 = x82.map{ case x83 => 
   val x84 = x83.c_name 
val x85 = x83.c_custkey 
val x86 = Record174(x85) 
val x87 = Record175(x84, x86) 
x87 
} 
val M__D_1 = x88
val x89 = M__D_1
//M__D_1.collect.foreach(println(_))
val x91 = M__D_1 
val x95 = x91.map{ case x92 => 
   val x93 = x92.c_orders 
val x94 = Record176(x93) 
x94 
} 
val x96 = x95.distinct 
val c_orders_ctx1 = x96
val x97 = c_orders_ctx1
//c_orders_ctx1.collect.foreach(println(_))
val x99 = c_orders_ctx1 
val x107 = O__D_1.filter(x100 => { val x101 = x100.o_orderkey 
val x102 = x101 <= 150000000 
x102 }).map(x100 => { val x103 = x100.o_orderdate 
val x104 = x100.o_orderkey 
val x105 = x100.o_custkey 
val x106 = Record177(x103, x104, x105) 
x106 }) 
val x113 = { val out1 = x99.map{ case x108 => ({val x110 = x108.lbl 
val x111 = x110.c__Fc_custkey 
x111}, x108) }
  val out2 = x107.map{ case x109 => ({val x112 = x109.o_custkey 
x112}, x109) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x123 = x113.flatMap{ case (x114, x115) => val x122 = (x115) 
x122 match {
   case (null) => Nil 
   case x121 => List(({val x116 = (x114) 
x116}, {val x117 = x115.o_orderdate 
val x118 = x115.o_orderkey 
val x119 = Record179(x118) 
val x120 = Record180(x117, x119) 
x120}))
 }
}.groupByLabel() 
val x128 = x123.map{ case (x124, x125) => 
   val x126 = x124.lbl 
val x127 = (x126, x125) 
x127 
} 
val c_orders__D_1 = x128
val x129 = c_orders__D_1
//c_orders__D_1.collect.foreach(println(_))
val x131 = c_orders__D_1 
val x135 = x131.flatMap{ 
 case x132 => {val x133 = x132._2 
x133}.map{ case v2 => (x132._1, v2) }
}
         
val x140 = x135.map{ case (x136, x137) => 
   val x138 = x137.o_parts 
val x139 = Record181(x138) 
x139 
} 
val x141 = x140.distinct 
val o_parts_ctx1 = x141
val x142 = o_parts_ctx1
//o_parts_ctx1.collect.foreach(println(_))
val x144 = o_parts_ctx1 
val x146 = ljp__D_1 
val x152 = { val out1 = x144.map{ case x147 => ({val x149 = x147.lbl 
val x150 = x149.o__Fo_orderkey 
x150}, x147) }
  val out2 = x146.map{ case x148 => ({val x151 = x148.l_orderkey 
x151}, x148) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x161 = x152.flatMap{ case (x153, x154) => val x160 = (x154) 
x160 match {
   case (null) => Nil 
   case x159 => List(({val x155 = (x153) 
x155}, {val x156 = x154.p_name 
val x157 = x154.l_qty 
val x158 = Record183(x156, x157) 
x158}))
 }
}.groupByLabel() 
val x166 = x161.map{ case (x162, x163) => 
   val x164 = x162.lbl 
val x165 = (x164, x163) 
x165 
} 
val o_parts__D_1 = x166
val x167 = o_parts__D_1
//o_parts__D_1.collect.foreach(println(_))
x167.count
var end0 = System.currentTimeMillis() - start0
println("ShredQuery1FilterSpark,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("ShredQuery1FilterSpark"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
