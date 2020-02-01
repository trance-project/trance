
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record165(lbl: Unit)
case class Record166(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record167(p_name: String, p_partkey: Int)
case class Record168(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record169(c_name: String, c_custkey: Int)
case class Record170(c__Fc_custkey: Int)
case class Record171(c_name: String, c_orders: Record170)
case class Record172(lbl: Record170)
case class Record173(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record175(o__Fo_orderkey: Int)
case class Record176(o_orderdate: String, o_parts: Record175)
case class Record177(lbl: Record175)
case class Record179(p_name: String, l_qty: Double)
case class Record232(o_orderdate: String, o_parts: Iterable[Record179])
case class Record233(c_name: String, c_orders: Iterable[Record232])
object ExploreJoins {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ExploreJoins"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L__F = 3
val L__D_1 = tpch.loadLineitemProj
L__D_1.cache
spark.sparkContext.runJob(L__D_1, (iter: Iterator[_]) => {})
val P__F = 4
val P__D_1 = tpch.loadPartProj
P__D_1.cache
spark.sparkContext.runJob(P__D_1, (iter: Iterator[_]) => {})
/**val C__F = 1
val C__D_1 = tpch.loadCustomersProj
C__D_1.cache
spark.sparkContext.runJob(C__D_1, (iter: Iterator[_]) => {})
val O__F = 2
val O__D_1 = tpch.loadOrdersProj
O__D_1.cache
spark.sparkContext.runJob(O__D_1, (iter: Iterator[_]) => {})**/

tpch.triggerGC

	def f = {
 
var start0 = System.currentTimeMillis()
val x47 = () 
val x48 = Record165(x47) 
val x49 = List(x48) 
val ljp_ctx1 = x49
val x50 = ljp_ctx1
//ljp_ctx1.collect.foreach(println(_))
val x56 = L__D_1/**.map(x51 => { val x52 = x51.l_orderkey 
val x53 = x51.l_quantity 
val x54 = x51.l_partkey 
val x55 = Record166(x52, x53, x54) 
x55 }) **/
val x61 = P__D_1/**.map(x57 => { val x58 = x57.p_name 
val x59 = x57.p_partkey 
val x60 = Record167(x58, x59) 
x60 }) **/
val x66_out1 = x56.map{ case x62 => ({val x64 = x62.l_partkey 
x64}, x62) }
val x66_out2 = x61.map{ case x63 => ({val x65 = x63.p_partkey 
x65}, x63) }
val x66 =  x66_out1.joinSkewLeft(x66_out2)

val x73 = x66.map{ case (x67, x68) => 
   val x69 = x67.l_orderkey 
val x70 = x68.p_name 
val x71 = x67.l_quantity 
val x72 = Record168(x69, x70, x71) 
x72 
} 
val ljp__D_1 = x73
val x74 = ljp__D_1

spark.sparkContext.runJob(ljp__D_1, (iter: Iterator[_]) => {})

val heavykeys = x66_out1.heavyKeys()
val hkey = x66_out1.sparkContext.broadcast(heavykeys)

val (llight, rlight) = x66_out1.filterLight(x66_out2, hkey)
val light = llight.joinDropKey(rlight)
spark.sparkContext.runJob(light, (iter: Iterator[_]) => {})

val (lheavy, rheavy) = x66_out1.filterHeavy(x66_out2, hkey)
val (rekey, dupp) = lheavy.rekeyBySet(rheavy, hkey)
val heavy = rekey.joinDropKey(dupp)
spark.sparkContext.runJob(heavy, (iter: Iterator[_]) => {})

// just some random downstream job
light.heavyKeys()
heavy.heavyKeys()

var end0 = System.currentTimeMillis() - start0
println("ExploreJoins,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)    

}
f
  }
}
