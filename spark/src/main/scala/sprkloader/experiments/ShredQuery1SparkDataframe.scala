
package sprkloader.experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.HashMap
import sprkloader._
import sprkloader.PairRDDOperations._
import sprkloader.DictRDDOperations._
import sprkloader.TopRDD._
import org.apache.spark.sql._
case class Record6947(l_orderkey: Int, l_quantity: Double, p_name: String)
case class Record6948(p_name: String, p_partkey: Int)
case class Record6951(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record6952(c__F_c_custkey: Int)
case class Record6953(c_name: String, c_orders: Record6952)
case class Record6954(o__F_o_orderkey: Int)
case class Record6955(o_orderdate: String, _1: Int)
case class Record6958(p_name: String, l_qty: Double)
case class Record6957(_KEY: Int, o_orderdate: String, o_parts: Seq[Record6958])
case class Record6956(p_name: String, l_quantity: Double, _KEY: Record6954)
case class RecordIn(o_orderdate: String, o_parts: Seq[Record6958])
case class RecordTop(c_name: String, c_orders: Seq[RecordIn])
case class RecordC(c_name: String, c_orders: Int)
case class RecordO(_KEY: Int, o_orderdate: String, o_parts: Int)
case class RecordL(_KEY: Int, p_name: String, l_quantity: Double)

object ShredQuery1SparkDataframe extends App {
 override def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery1SparkDataframe"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
   import spark.implicits._
val IBag_L__D = tpch.loadLineitemDF()
IBag_L__D.cache
IBag_L__D.count
val IBag_P__D = tpch.loadPartDF()
IBag_P__D.cache
IBag_P__D.count
val IBag_C__D = tpch.loadCustomersDF()
IBag_C__D.cache
IBag_C__D.count
val IBag_O__D = tpch.loadOrdersDF()
IBag_O__D.cache
IBag_O__D.count
// implicit val ncodec = Encoders.product[RecordC]
implicit val ncode0 = Encoders.product[Record6957]
implicit val ncode1 = Encoders.product[Record6958]
implicit val ncode2 = Encoders.product[RecordTop]
implicit val ncode3 = Encoders.product[RecordIn]

    def f = {
 
var start0 = System.currentTimeMillis()
val MBag_Query1_1 = IBag_C__D
  .select("c_name", "c_custkey")
  .withColumnRenamed("c_custkey", "c_orders").as[RecordC]
MBag_Query1_1.cache
MBag_Query1_1.count

val MDict_Query1_1_c_orders_1 = IBag_O__D
  .select("o_custkey", "o_orderdate", "o_orderkey")
  .withColumnRenamed("o_custkey", "_KEY")
  .withColumnRenamed("o_orderkey", "o_parts").as[RecordO]
MDict_Query1_1_c_orders_1.cache
MDict_Query1_1_c_orders_1.count

val MBag_ljp_1 = IBag_L__D.join(IBag_P__D, 
  IBag_L__D("l_partkey") === IBag_P__D("p_partkey"))
val MDict_Query1_1_c_orders_1_o_parts_1 = MBag_ljp_1
  .select("l_orderkey", "p_name", "l_quantity")
  .withColumnRenamed("l_orderkey", "_KEY").as[RecordL]
MDict_Query1_1_c_orders_1_o_parts_1.cache
MDict_Query1_1_c_orders_1_o_parts_1.count

var end0 = System.currentTimeMillis() - start0
println("Shred,Standard,Query1,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
  
var start1 = System.currentTimeMillis()

val dict1 = MDict_Query1_1_c_orders_1_o_parts_1.groupByKey(x => x._KEY).mapGroups{
  case (key, value) => (key, value.map(x => Record6958(x.p_name, x.l_quantity)).toSeq)
}

val dict2 = dict1.join(MDict_Query1_1_c_orders_1, 
  dict1("_1") === MDict_Query1_1_c_orders_1("o_parts"))
  .drop("_1", "o_parts")
  .withColumnRenamed("_2", "o_parts").as[Record6957].groupByKey(_._KEY).mapGroups{
    case (key, value) => (key, value.map(x => RecordIn(x.o_orderdate, x.o_parts)).toSeq)
  }

val result = dict2.join(MBag_Query1_1, 
  MBag_Query1_1("c_orders") === dict2("_1"))
  .drop("_1", "c_orders")
  .withColumnRenamed("_2", "c_orders").as[RecordTop]

result.count

var end1 = System.currentTimeMillis() - start1
println("Shred,Standard,Query1,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)
  
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("Shred,Standard,Query1,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
