
package sprkloader.experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.scalalang._
import sprkloader._
import sprkloader.SkewDataset._
case class Recordb6da9420d9ce40abaa722b94f79cc9e0(p_name: String, l_quantity: Double)
case class Record5bbf08b66a334c7cb759302a016b6c5a(o_orderdate: String, o_parts: Seq[Recordb6da9420d9ce40abaa722b94f79cc9e0])
case class Recorde266b195b9d8481fa741d5c33d5f1162(p_name: String, l_quantity: Double, _1: Int, l_partkey: Int, p_partkey: Int)
case class Record10424c41ee0d457c8817eccf4536df48(o_orderdate: String, o_parts: Int)
case class Record5c597ab465cf424783bd772efe7cdf90(_1: Int, p_name: String)
case class Recorda9d8bd6858e14681bd06200070f63cc8(_1: Int, p_name: String, l_quantity: Double)
case class Recordc809c0177e5244f9b2a9fb72f43ba460(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, _1: Int, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Record1aabef409b8b4e3d9b9c3ba79dac83ca(l_quantity: Double, _1: Int, l_partkey: Int)
case class Record6e0b149e3efa48e4a669c8ffe5102ca8(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Int, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Recorda5041667d172454a973291c6a798ca06(p_name: String, p_partkey: Int)
object ShredTest1NNUnshredSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master)
     .setAppName("ShredTest1NNUnshredSpark"+sf)
     .set("spark.sql.shuffle.partitions", Config.lparts.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   
implicit val encoderRecorda9d8bd6858e14681bd06200070f63cc8: Encoder[Recorda9d8bd6858e14681bd06200070f63cc8] = Encoders.product[Recorda9d8bd6858e14681bd06200070f63cc8]
implicit val encoderRecordb6da9420d9ce40abaa722b94f79cc9e0: Encoder[Recordb6da9420d9ce40abaa722b94f79cc9e0] = Encoders.product[Recordb6da9420d9ce40abaa722b94f79cc9e0]
   import spark.implicits._
   val tpch = TPCHLoader(spark)
val IBag_L__D = tpch.loadLineitemDF()
IBag_L__D.cache
IBag_L__D.count
val IBag_P__D = tpch.loadPartDF()
IBag_P__D.cache
IBag_P__D.count
val IBag_O__D = tpch.loadOrderDF()
IBag_O__D.cache
IBag_O__D.count

   val x1519 = IBag_O__D
 .withColumn("o_parts", $"o_orderkey").as[Record6e0b149e3efa48e4a669c8ffe5102ca8]
 
val x1520 = x1519
val MBag_Test1Full_1 = x1520
//MBag_Test1Full_1.print
//MBag_Test1Full_1.cache
//MBag_Test1Full_1.count
val x1522 = IBag_L__D
 .withColumn("_1", $"l_orderkey").as[Recordc809c0177e5244f9b2a9fb72f43ba460]
 
val x1523 = x1522
val MDict_Test1Full_1_o_parts_1 = x1523.repartition($"_1")
//MDict_Test1Full_1_o_parts_1.print
//MDict_Test1Full_1_o_parts_1.cache
//MDict_Test1Full_1_o_parts_1.count


val IBag_Test1Full__D = MBag_Test1Full_1
IBag_Test1Full__D.cache
IBag_Test1Full__D.count
val IDict_Test1Full__D_o_parts = MDict_Test1Full_1_o_parts_1
IDict_Test1Full__D_o_parts.cache
IDict_Test1Full__D_o_parts.count
 def f = {
 
 
var start0 = System.currentTimeMillis()
val x1538 = MBag_Test1Full_1.select("o_orderdate", "o_parts")
            .as[Record10424c41ee0d457c8817eccf4536df48]
 
val x1539 = x1538
val MBag_Test1NN_1 = x1539
//MBag_Test1NN_1.print
MBag_Test1NN_1.cache
MBag_Test1NN_1.count
val x1541 = MDict_Test1Full_1_o_parts_1.select("l_quantity", "_1", "l_partkey")
            .as[Record1aabef409b8b4e3d9b9c3ba79dac83ca]
 
val x1543 = IBag_P__D.select("p_name", "p_partkey")
            .as[Recorda5041667d172454a973291c6a798ca06]
 
val x1546 = x1541.equiJoin[Recorda5041667d172454a973291c6a798ca06](x1543, Seq("l_partkey","p_partkey"), "inner")
 .as[Recorde266b195b9d8481fa741d5c33d5f1162]
 
val x1551 = x1546.reduceByKey(x1569 => 
 Record5c597ab465cf424783bd772efe7cdf90(x1569._1, x1569.p_name), x => x.l_quantity match { case _ => x.l_quantity}).mapPartitions(
     it => it.map{ case (x1569, x1570) => Recorda9d8bd6858e14681bd06200070f63cc8(x1569._1, x1569.p_name, x1570) })
val x1552 = x1551
val MDict_Test1NN_1_o_parts_1 = x1552.repartition($"_1")
//MDict_Test1NN_1_o_parts_1.print
MDict_Test1NN_1_o_parts_1.cache
MDict_Test1NN_1_o_parts_1.count



var end0 = System.currentTimeMillis() - start0
println("Shred,1,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    

var start1 = System.currentTimeMillis()
val x1567 = MBag_Test1NN_1.join(MDict_Test1NN_1_o_parts_1, $"_1" === $"o_parts", "left_outer")
val x1568 = x1567
val Test1NN = x1568
//Test1NN.print
//Test1NN.cache
Test1NN.count


var end1 = System.currentTimeMillis() - start1
println("Shred,1,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("Shred,1,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
