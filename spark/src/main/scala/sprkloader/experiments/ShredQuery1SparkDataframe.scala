
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
case class Record6947(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record6948(p_name: String, p_partkey: Int)
case class Record6951(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record6952(c__F_c_custkey: Int)
case class Record6953(c_name: String, c_orders: Record6952)
case class Record6954(o__F_o_orderkey: Int)
case class Record6955(o_orderdate: String, o_parts: Record6954, _1: Record6952)
case class Record6956(p_name: String, l_qty: Double, _1: Record6954)
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

    def f = {
 
var start0 = System.currentTimeMillis()
// val x6900 = IBag_L__D.map{ case x6895 => 
//    {val x6896 = x6895.l_orderkey 
// val x6897 = x6895.l_quantity 
// val x6898 = x6895.l_partkey 
// val x6899 = Record6947(x6896, x6897, x6898) 
// x6899}
// } 
// val x6905 = IBag_P__D.map{ case x6901 => 
//    {val x6902 = x6901.p_name 
// val x6903 = x6901.p_partkey 
// val x6904 = Record6948(x6902, x6903) 
// x6904}
// } 
// val x6949 = x6900.map{ case x6906 => ({val x6908 = x6906.l_partkey 
// x6908}, x6906) }
// val x6950 = x6905.map{ case x6907 => ({val x6909 = x6907.p_partkey 
// x6909}, x6907) }
// val x6911 = x6949.joinDropKey(x6950)
// val x6918 = x6911.map{ case (x6912, x6913) => 
//    {val x6914 = x6912.l_orderkey 
// val x6915 = x6913.p_name 
// val x6916 = x6912.l_quantity 
// val x6917 = Record6951(x6914, x6915, x6916) 
// x6917}
// } 
// val MBag_ljp_1 = x6918
// val x6919 = MBag_ljp_1
// //MBag_ljp_1.cache
// //MBag_ljp_1.print
// //MBag_ljp_1.evaluate

// val x6925 = IBag_C__D.map{ case x6920 => 
//    {val x6921 = x6920.c_name 
// val x6922 = x6920.c_custkey 
// val x6923 = Record6952(x6922) 
// val x6924 = Record6953(x6921, x6923) 
// x6924}
// } 
// val MBag_Query1_1 = x6925
val MBag_Query1_1 = IBag_C__D
  .select("c_name", "c_custkey")
  .withColumnRenamed("c_custkey", "c_orders")
// val x6926 = MBag_Query1_1
MBag_Query1_1.count
// //MBag_Query1_1.print
// //MBag_Query1_1.evaluate
// val x6934 = IBag_O__D.map{ case x6927 => 
//    {val x6928 = x6927.o_orderdate 
// val x6929 = x6927.o_orderkey 
// val x6930 = Record6954(x6929) 
// val x6931 = x6927.o_custkey 
// val x6932 = Record6952(x6931) 
// val x6933 = Record6955(x6928, x6930, x6932) 
// x6933}
// } 
// val x6935 = x6934 
// val MDict_Query1_1_c_orders_1 = x6935
// val x6936 = MDict_Query1_1_c_orders_1
// //MDict_Query1_1_c_orders_1.cache
// //MDict_Query1_1_c_orders_1.print
// //MDict_Query1_1_c_orders_1.evaluate
val MDict_Query1_1_c_orders_1 = IBag_O__D
  .select("o_custkey", "o_orderdate", "o_orderkey")
  .withColumnRenamed("o_custkey", "_KEY")
  .withColumnRenamed("o_orderkey", "o_parts")
  .select("o_orderdate", "o_parts")
MDict_Query1_1_c_orders_1.count
// val x6943 = MBag_ljp_1.map{ case x6937 => 
//    {val x6938 = x6937.p_name 
// val x6939 = x6937.l_qty 
// val x6940 = x6937.l_orderkey 
// val x6941 = Record6954(x6940) 
// val x6942 = Record6956(x6938, x6939, x6941) 
// x6942}
// } 
// val x6944 = x6943 
// val MDict_Query1_1_c_orders_1_o_parts_1 = x6944
// val x6945 = MDict_Query1_1_c_orders_1_o_parts_1
val MBag_ljp_1 = IBag_L__D.join(IBag_P__D, 
  IBag_L__D("l_partkey") === IBag_P__D("p_partkey"))
val MDict_Query1_1_c_orders_1_o_parts_1 = MBag_ljp_1
  .select("l_orderkey", "p_name", "l_quantity")
  .withColumnRenamed("l_orderkey", "_KEY")
MDict_Query1_1_c_orders_1_o_parts_1.count
// //MDict_Query1_1_c_orders_1_o_parts_1.print
// MDict_Query1_1_c_orders_1_o_parts_1.evaluate

// var end0 = System.currentTimeMillis() - start0
// println("Shred,Standard,Query1,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("Shred,Standard,Query1,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
