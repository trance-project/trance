
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._
import org.apache.spark.Partitioner.defaultPartitioner
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record1052(p__Fp_partkey: Int)
case class Record1053(p_name: String, suppliers: Record1052, customers: Record1052)
case class Record1054(s_name: String, s_nationkey: Int)
case class Record1055(c_name: String, c_nationkey: Int)
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
val Query3__D_1 = spark.sparkContext.objectFile[Record1053]("/nfs_qc4/query3/Query3__D_1")//M_flat1
Query3__D_1.cache
Query3__D_1.count
val Query3__D_2suppliers_1 = spark.sparkContext.objectFile[(Record1052, Iterable[Record1054])]("/nfs_qc4/query3/Query3__D_2suppliers_1")//M_flat2
Query3__D_2suppliers_1.cache
Query3__D_2suppliers_1.count
val Query3__D_2customers_1 = spark.sparkContext.objectFile[(Record1052, Iterable[Record1055])]("/nfs_qc4/query3/Query3__D_2customers_1")//M_flat3
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
M_flat1.count
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
		  val out3 = x176
		    val cg = new CoGroupedRDD(Seq(out1, out2, out3), defaultPartitioner(out1, out2, out3))
			  cg.mapValues{
				    case Array(vs, ws1, ws2) => (vs.asInstanceOf[Iterable[Record137]], ws1.asInstanceOf[Iterable[Record138]], ws2.asInstanceOf[Iterable[Record138]])
					  }
} 
val x181 = x172
val x192 = x181.flatMap{ case (key, (pnames, suppliers, customers)) => 
  if (suppliers.filterNot(customers.toList.contains).isEmpty) Nil
	  else List((key, pnames.map{p => p.p_name}))
}//.groupByLabel()
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
