
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record1102(p_name: String, p_partkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1103(ps_suppkey: Int, ps_partkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1104(s_name: String, s_nationkey: Int, s_suppkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1106(s_name: String, s_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1107(l_orderkey: Int, l_partkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1108(o_custkey: Int, o_orderkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1109(c_name: String, c_nationkey: Int, c_custkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1111(c_name: String, c_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Query3Out(p_name: String, suppliers: Iterable[Record1106], customers: Iterable[Record1111], uniqueId: Long) extends CaseClassRecord
object Query3SparkSlenderShred {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query3SparkSlenderShred"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   
val tpch = TPCHLoader(spark)
val C = tpch.loadCustomers
C.cache
C.count
val O = tpch.loadOrders
O.cache
O.count
val L = tpch.loadLineitem
L.cache
L.count
val P = tpch.loadPart
P.cache
P.count
val PS = tpch.loadPartSupp
PS.cache
PS.count
val S = tpch.loadSupplier
S.cache
S.count
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
   var start0 = System.currentTimeMillis()
   val result_F = P.map(p => (p.p_name, p.p_partkey, p.p_partkey))
   result_F.count
  
   val partsupp = PS.map(ps => ps.ps_suppkey -> ps.ps_partkey)
   val suppliers = S.map(s => s.s_suppkey -> (s.s_name, s.s_nationkey))
   val result_G1 = partsupp.joinSkewLeft(suppliers).groupByLabel((x,y) => y)
   result_G1.count

   val orders = O.map(o => o.o_custkey -> o.o_orderkey) 
   val customers = C.map(c => c.c_custkey -> (c.c_name, c.c_nationkey))
   val lineitem = L.map(l => l.l_orderkey -> l.l_partkey)
   val result_G2 = orders.joinSkewLeft(customers).map{ case (_, (o_orderkey, c_info)) => 
                      o_orderkey -> c_info }.join(lineitem).groupByLabel((x,y) => y.swap)
   result_G2.count

   var end0 = System.currentTimeMillis() - start0
   println("Query3SparkSlenderShred"+sf+","+Config.datapath+","+end0,spark.sparkContext.applicationId)
 }
}
