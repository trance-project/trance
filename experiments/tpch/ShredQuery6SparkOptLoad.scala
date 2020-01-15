
package experiments
/**

For c in C Union
  Sng((c_name := c.c_name, suppliers := For co in Query2 Union
    For co2 in co.customers2 Union
      If (co2.c_name2 = c.c_name)
      Then Sng((s_name := co.s_name))))

**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record226(o_orderkey: Int, o_custkey: Int)
case class Record227(c_name: String, c_custkey: Int)
case class Record228(o_orderkey: Int, c_name: String)
case class Record229(s__Fs_suppkey: Int)
case class Record230(s_name: String, customers2: Record229)
case class Record231(l_suppkey: Int, l_orderkey: Int)
case class Record233(c_name2: String)
case class Record266(c__Fc_name: String)
case class Record267(c_name: String, suppliers: Record266)
case class Record268(s_name: String)
case class Record667(s__Fs_suppkey: Int)
case class Record668(s_name: String, customers2: Record667)
case class Record669(l_suppkey: Int, l_orderkey: Int)
case class Record671(c_name2: String)
object ShredQuery6SparkOptLoad {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6SparkOptLoad"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)

val C__F = 1
val C__D_1 = tpch.loadCustomers
C__D_1.cache
C__D_1.count

val Query2__D_1 = spark.sparkContext.objectFile[Record668]("/nfs_qc4/query3/Query2Full__D_1Skew")
Query2__D_1.cache
Query2__D_1.count
val Query2__D_2customers2_1 = spark.sparkContext.objectFile[(Record667, Iterable[Record671])]("/nfs_qc4/query3/Query2Full__D_2customers2_1Skew")
Query2__D_2customers2_1.cache
Query2__D_2customers2_1.count
def f = { 
/**
M_flat1 :=  REDUCE[ (c_name := x237.c_name,suppliers := (c__Fc_name := x237.c_name)) / true ](C__D_1)
**/ 
 
 val x247 = C__D_1.map{ case x243 => 
   val x244 = x243.c_name 
val x245 = Record266(x244) 
val x246 = Record267(x244, x245) 
x246 
} 
val M_flat1 = x247
val x248 = M_flat1
//M_flat1.collect.foreach(println(_))
/**
M_flat2 :=  REDUCE[ (key := (c__Fc_name := x242.c_name2),value := { (s_name := x241.s_name) }) / true ]( <-- (x238,x240) -- (
 <-- (x238) -- SELECT[ true, x238 ](Query2__D_1)) LOOKUP[x239.customers2, true = true](
   Query2__D_2customers2_1))
**/

val x250 = Query2__D_1 
val x254 = {
 val out1 = x250.map{case x251 => ({val x253 = x251.customers2 
x253},x251)}
 val out2 = Query2__D_2customers2_1.flatMapValues(identity)
 out2.lookupSkewLeft(out1)

} 
val x263 = x254.map{ case (x256, x255) => 
   val x257 = x256.c_name2 
val x258 = Record266(x257) 
val x259 = x255.s_name 
val x260 = Record268(x259) 
val x261 = List(x260) 
val x262 = (x258, x261) 
x262 
} 
val M_flat2 = x263
val x264 = M_flat2
//M_flat2.collect.foreach(println(_))
x264.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery6SparkOptLoad"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
