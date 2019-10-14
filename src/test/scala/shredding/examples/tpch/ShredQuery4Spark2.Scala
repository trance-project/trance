
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record894(lbl: Unit)
case class Record895(c_name: String, c_custkey: Int)
case class Record897(c__Fc_custkey: Int)
case class Record898(c_name: String, c_orders: Record897)
case class Record899(_1: Record894, _2: (Iterable[Record898]))
case class Record900(lbl: Record897)
case class Record901(o_orderdate: String, o_orderkey: Int, o_custkey: Int)
case class Record903(o__Fo_orderkey: Int)
case class Record904(o_orderdate: String, o_parts: Record903)
case class Record905(_1: Record900, _2: (Iterable[Record904]))
case class Record906(lbl: Record903)
case class Record907(l_quantity: Double, l_partkey: Int, l_orderkey: Int)
case class Record908(p_name: String, p_partkey: Int)
case class Record910(p_name: String, l_qty: Double)
case class Record911(_1: Record906, _2: (Iterable[Record910]))
case class Record977(c_name: String, p_name: String, month: String)
object ShredQuery4Spark2 {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4Spark2"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   
val x762 = () 
val x763 = Record894(x762) 
val x764 = List(x763) 
val M_ctx1 = x764
val x767 = M_ctx1 

val M_flat1 = spark.sparkContext.objectFile[Record899]("/nfs_qc4/M_flat1")
val M_flat2 = spark.sparkContext.objectFile[Record905]("/nfs_qc4/M_flat2")
val M_flat3 = spark.sparkContext.objectFile[Record911]("/nfs_qc4/M_flat3")

val Query4__F = M_ctx1
val Query4__D_1 = M_flat1.flatMap{ r => r._2 }
Query4__D_1.cache
Query4__D_1.count
val Query4__D_2c_orders_1 = M_flat2
Query4__D_2c_orders_1.cache
Query4__D_2c_orders_1.count
val Query4__D_2c_orders_2o_parts = M_flat3
Query4__D_2c_orders_2o_parts.cache
Query4__D_2c_orders_2o_parts.count
    
   var start0 = System.currentTimeMillis()
   def f() {
val x944 = Query4__D_2c_orders_2o_parts 
val x946 = x944 
val x948 = x946 
val out2 = x948.flatMap(x952 => x952._2.map{case v2 => (x952._1.lbl, v2)})
val x967 = out2.map{ 
  case (a, null) => (a, 0.0) 
  case (x957, x958) =>
    ({val x960 = x958.p_name
      val x961 = x957
      val x963 = (x957,x958)
     x963}, {val x964 = x958.l_qty
     x964})
}.reduceByKey(_+_)
val M_flat1 = x967

val x974 = M_flat1
//M_flat1.collect.foreach(println(_))
val res = x974.count
  }
   f
   var end0 = System.currentTimeMillis() - start0
   println("ShredQuery4Spark2"+sf+","+Config.datapath+","+end0)
 }
}
