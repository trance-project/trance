
package experiments
/** 
resultInner = For o in O Union
  For c in C Union
    If (c.c_custkey = o.o_custkey)
    Then Sng((o_orderkey := o.o_orderkey, c_name := c.c_name))

For s in S Union
  Sng((s_name := s.s_name, customers2 := For l in L Union
    If (s.s_suppkey = l.l_suppkey)
    Then For co in resultInner Union
      If (co.o_orderkey = l.l_orderkey)
      Then Sng((c_name2 := co.c_name))))
**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record143(o_orderkey: Int, o_custkey: Int)
case class Record144(c_name: String, c_custkey: Int)
case class Record145(o_orderkey: Int, c_name: String)
case class Record146(s__Fs_suppkey: Int)
case class Record147(s_name: String, customers2: Record146)
case class Record148(l_suppkey: Int, l_orderkey: Int)
case class Record150(c_name2: String)
object ShredQuery2SparkOpt {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery2SparkOpt"+sf)
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
/**
resultInner__D_1 :=  REDUCE[ (o_orderkey := x75.o_orderkey,c_name := x76.c_name) / true ]( <-- (x75,x76) -- (
 <-- (x75) -- SELECT[ true, (o_orderkey := x75.o_orderkey,o_custkey := x75.o_custkey) ](O__D._1)) JOIN[x75.o_custkey = x76.c_custkey](

   <-- (x76) -- SELECT[ true, (c_name := x76.c_name,c_custkey := x76.c_custkey) ](C__D._1)))
**/
 val x81 = O__D_1 
val x86 = x81.map(x82 => { val x83 = x82.o_orderkey 
val x84 = x82.o_custkey 
val x85 = Record143(x83, x84) 
x85 }) 
val x87 = C__D_1 
val x92 = x87.map(x88 => { val x89 = x88.c_name 
val x90 = x88.c_custkey 
val x91 = Record144(x89, x90) 
x91 }) 
val x97 = { val out1 = x86.map{ case x93 => ({val x95 = x93.o_custkey 
x95}, x93) }
  val out2 = x92.map{ case x94 => ({val x96 = x94.c_custkey 
x96}, x94) }
  out1.joiniSkewLeft(out2).map{ case (k,v) => v }
} 
val x103 = x97.map{ case (x98, x99) => 
   val x100 = x98.o_orderkey 
val x101 = x99.c_name 
val x102 = Record145(x100, x101) 
x102 
} 
val resultInner__D_1 = x103
val x104 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
/**
M_flat1 :=  REDUCE[ (s_name := x77.s_name,customers2 := (s__Fs_suppkey := x77.s_suppkey)) / true ](S__D._1)
**/
val x105 = S__D_1 
val x111 = x105.map{ case x106 => 
   val x107 = x106.s_name 
val x108 = x106.s_suppkey 
val x109 = Record146(x108) 
val x110 = Record147(x107, x109) 
x110 
} 
val M_flat1 = x111
val x112 = M_flat1
//M_flat1.collect.foreach(println(_))
/**
M_flat2 :=  REDUCE[ (key := (s__Fs_suppkey := x78.l_suppkey),value := x80) / true ]( <-- (x78,x80) -- NEST[ U / (c_name2 := x79.c_name) / (x78), true / (x79) ]( <-- (x78,x79) -- (
 <-- (x78) -- SELECT[ true, (l_suppkey := x78.l_suppkey,l_orderkey := x78.l_orderkey) ](L__D._1)) JOIN[x78.l_orderkey = x79.o_orderkey](

   <-- (x79) -- SELECT[ true, x79 ](resultInner__D._1))))
**/
val x113 = L__D_1 
val x118 = x113.map(x114 => { val x115 = x114.l_suppkey 
val x116 = x114.l_orderkey 
val x117 = Record148(x115, x116) 
x117 }) 
val x119 = resultInner__D_1 
val x121 = x119 
val x126 = { val out1 = x118.map{ case x122 => ({val x124 = x122.l_orderkey 
x124}, x122) }
  val out2 = x121.map{ case x123 => ({val x125 = x123.o_orderkey 
x125}, x123) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x134 = x126.flatMap{ case (x127, x128) => val x133 = (x128) 
x133 match {
   case (null) => Nil 
   case x132 => List(({val x129 = (x127) 
x129}, {val x130 = x128.c_name 
val x131 = Record150(x130) 
x131}))
 }
}.groupByLabel() 
val x140 = x134.map{ case (x135, x136) => 
   val x137 = x135.l_suppkey 
val x138 = Record146(x137) 
val x139 = (x138, x136) 
x139 
} 
val M_flat2 = x140
val x141 = M_flat2
//M_flat2.collect.foreach(println(_))
x141.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery2SparkOpt"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
