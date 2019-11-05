
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._
import org.apache.spark.Partitioner.defaultPartitioner
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record127(ps_partkey: Int, ps_suppkey: Int)
case class Record128(s_name: String, s_nationkey: Int, s_suppkey: Int)
case class Record129(ps_partkey: Int, s_name: String, s_nationkey: Int)
case class Record130(o_orderkey: Int, o_custkey: Int)
case class Record131(c_name: String, c_nationkey: Int, c_custkey: Int)
case class Record132(o_orderkey: Int, c_name: String, c_nationkey: Int)
case class Record133(l_partkey: Int, l_orderkey: Int)
case class Record134(l_partkey: Int, c_name: String, c_nationkey: Int)
case class Record135(p_name: String, p_partkey: Int)
case class Record136(p__Fp_partkey: Int)
case class Record137(p_name: String, suppliers: Record136, customers: Record136)
case class Record138(s_name: String, s_nationkey: Int)
case class Record139(c_name: String, c_nationkey: Int)
case class Record207(n_name: String, n_nationkey: Int)
case class Record208(n__Fn_nationkey: Int)
case class Record209(n_name: String, parts: Record208)
case class Record211(p_name: String)
object ShredQuery7FullSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery7FullSpark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val N__F = 6
val N__D_1 = tpch.loadNation
N__D_1.cache
N__D_1.count
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
val PS__F = 5
val PS__D_1 = tpch.loadPartSupp
PS__D_1.cache
PS__D_1.count
val S__F = 6
val S__D_1 = tpch.loadSupplier
S__D_1.cache
S__D_1.count

   val x23 = PS__D_1.map(x19 => { val x20 = x19.ps_partkey 
val x21 = x19.ps_suppkey 
val x22 = Record127(x20, x21) 
x22 }) 
val x24 = S__D_1 
val x30 = x24.map(x25 => { val x26 = x25.s_name 
val x27 = x25.s_nationkey 
val x28 = x25.s_suppkey 
val x29 = Record128(x26, x27, x28) 
x29 }) 
val x35 = { val out1 = x23.map{ case x31 => ({val x33 = x31.ps_suppkey 
x33}, x31) }
  val out2 = x30.map{ case x32 => ({val x34 = x32.s_suppkey 
x34}, x32) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x42 = x35.map{ case (x36, x37) => 
   val x38 = x36.ps_partkey 
val x39 = x37.s_name 
val x40 = x37.s_nationkey 
val x41 = Record129(x38, x39, x40) 
x41 
} 
val partsuppliers__D_1 = x42
val x43 = partsuppliers__D_1
//partsuppliers__D_1.collect.foreach(println(_))
val x48 = O__D_1.map(x44 => { val x45 = x44.o_orderkey 
val x46 = x44.o_custkey 
val x47 = Record130(x45, x46) 
x47 }) 
val x49 = C__D_1 
val x55 = x49.map(x50 => { val x51 = x50.c_name 
val x52 = x50.c_nationkey 
val x53 = x50.c_custkey 
val x54 = Record131(x51, x52, x53) 
x54 }) 
val x60 = { val out1 = x48.map{ case x56 => ({val x58 = x56.o_custkey 
x58}, x56) }
  val out2 = x55.map{ case x57 => ({val x59 = x57.c_custkey 
x59}, x57) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x67 = x60.map{ case (x61, x62) => 
   val x63 = x61.o_orderkey 
val x64 = x62.c_name 
val x65 = x62.c_nationkey 
val x66 = Record132(x63, x64, x65) 
x66 
} 
val custorders__D_1 = x67
val x68 = custorders__D_1
//custorders__D_1.collect.foreach(println(_))
val x70 = custorders__D_1 
val x71 = L__D_1 
val x76 = x71.map(x72 => { val x73 = x72.l_partkey 
val x74 = x72.l_orderkey 
val x75 = Record133(x73, x74) 
x75 }) 
val x81 = { val out1 = x70.map{ case x77 => ({val x79 = x77.o_orderkey 
x79}, x77) }
  val out2 = x76.map{ case x78 => ({val x80 = x78.l_orderkey 
x80}, x78) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x88 = x81.map{ case (x82, x83) => 
   val x84 = x83.l_partkey 
val x85 = x82.c_name 
val x86 = x82.c_nationkey 
val x87 = Record134(x84, x85, x86) 
x87 
} 
val cparts__D_1 = x88
val x89 = cparts__D_1
//cparts__D_1.collect.foreach(println(_))
val x94 = P__D_1.map(x90 => { val x91 = x90.p_name 
val x92 = x90.p_partkey 
val x93 = Record135(x91, x92) 
x93 }) 
val x100 = x94.map{ case x95 => 
   val x96 = x95.p_name 
val x97 = x95.p_partkey 
val x98 = Record136(x97) 
val x99 = Record137(x96, x98, x98) 
x99 
} 
val M_flat1 = x100
val x101 = M_flat1
//M_flat1.collect.foreach(println(_))
val x103 = partsuppliers__D_1 
val x112 = x103.map{ case x104 => 
   val x105 = x104.ps_partkey 
val x106 = Record136(x105) 
val x107 = x104.s_name 
val x108 = x104.s_nationkey 
val x109 = Record138(x107, x108) 
val x110 = List(x109) 
val x111 = (x106, x110) 
x111 
} 
val M_flat2 = x112
val x113 = M_flat2
//M_flat2.collect.foreach(println(_))
val x115 = cparts__D_1 
val x124 = x115.map{ case x116 => 
   val x117 = x116.l_partkey 
val x118 = Record136(x117) 
val x119 = x116.c_name 
val x120 = x116.c_nationkey 
val x121 = Record139(x119, x120) 
val x122 = List(x121) 
val x123 = (x118, x122) 
x123 
} 
val M_flat3 = x124
val x125 = M_flat3
//M_flat3.collect.foreach(println(_))
val Query3__D_1 = M_flat1
Query3__D_1.cache
Query3__D_1.count
val Query3__D_2suppliers_1 = M_flat2
Query3__D_2suppliers_1.cache
Query3__D_2suppliers_1.count
val Query3__D_2customers_1 = M_flat3
Query3__D_2customers_1.cache
Query3__D_2customers_1.count
def f = { 
 val x154 = N__D_1.map(x150 => { val x151 = x150.n_name 
val x152 = x150.n_nationkey 
val x153 = Record207(x151, x152) 
x153 }) 
val x160 = x154.map{ case x155 => 
   val x156 = x155.n_name 
val x157 = x155.n_nationkey 
val x158 = Record208(x157) 
val x159 = Record209(x156, x158) 
x159 
} 
val M_flat1 = x160
val x161 = M_flat1
//M_flat1.collect.foreach(println(_))
val x163 = Query3__D_1 
val x165 = Query3__D_2suppliers_1 
val x168 = x165 
val x173 = Query3__D_2customers_1 
val x176 = x173 
val x172 = { 
  //
  val out1 = x163.map{ case x169 => ({val x171 = x169.suppliers; x171}, x169) }
  val out2 = x168//.flatMapValues(identity)s
  val cg = new CoGroupedRDD(Seq(out1, out2), defaultPartitioner(out1, out2))
  cg.mapValues{
    case Array(vs, ws1) => (vs.asInstanceOf[Iterable[Record137]], ws1.asInstanceOf[Iterable[Record138]])
  }
} 
val x181 = {
    //
  val out1 = x172 // it's already keyed appropriately
  val out2 = x176//.flatMapValues(identity)
  out1.join(out2)
}
val x192 = x181.flatMap{ case (key, ((pnames, suppliers), customers)) => 
  if (suppliers.filterNot(customers.contains).isEmpty) Nil
  else List((key, pnames))
}.groupByLabel()
val M_flat2 = x192
val x205 = M_flat2
//M_flat2.collect.foreach(println(_))
x205.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery7FullSpark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
