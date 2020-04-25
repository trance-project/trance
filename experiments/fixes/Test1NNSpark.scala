
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
case class Record385ff2a289754ccbbbab9e8bf269fe38(o_orderdate: String, o_parts: Seq[Recordab353457ff7346b0819f06d3db6dcef2])
case class Recordbda23cc1aef14465b522c04bde131be9(o_orderdate: String, o_parts: Seq[Recordab353457ff7346b0819f06d3db6dcef2], index: Long)
case class Recorda1e1f502b53b42978f448f7312b89833(o_orderdate: String, l_quantity: Option[Double], l_partkey: Option[Int], index: Long)
case class Record2a270432b3c6461d97135c57e09ade78(p_name: String, p_partkey: Int)
case class Record8f6bb04f8a31481d88356a3a9c546465(p_name: Option[String], o_orderdate: String, l_quantity: Option[Double], l_partkey: Option[Int], p_partkey: Option[Int], index: Long)
case class Recorddbb4f3ee216d46018a1c29afa2393053(p_name: Option[String], l_quantity: Option[Double])
case class Record08a3397aceb14e5282225089a0e7bce8(index: Long, o_orderdate: String, p_name: Option[String])
case class Record73b99337218e48a6ac1867f0e26e043c(index: Long, o_orderdate: String)
case class Record9c75463c696b4b5ea91449271385506e(o_orderdate: String, o_parts: Seq[Recorddbb4f3ee216d46018a1c29afa2393053])
case class Recordab353457ff7346b0819f06d3db6dcef2(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Record8efe46df39f64707a07b0eb3f9fe1b80(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Seq[Recordab353457ff7346b0819f06d3db6dcef2], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
object Test1NNSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master)
     .setAppName("Test1NNSpark"+sf)
     .set("spark.sql.shuffle.partitions", Config.lparts.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   implicit val encoderRecordab353457ff7346b0819f06d3db6dcef2: Encoder[Recordab353457ff7346b0819f06d3db6dcef2] = Encoders.product[Recordab353457ff7346b0819f06d3db6dcef2]
implicit val encoderRecorddbb4f3ee216d46018a1c29afa2393053: Encoder[Recorddbb4f3ee216d46018a1c29afa2393053] = Encoders.product[Recorddbb4f3ee216d46018a1c29afa2393053]
   import spark.implicits._
   val tpch = TPCHLoader(spark)
val L = tpch.loadLineitemDF()
L.cache
L.count
val P = tpch.loadPartDF()
P.cache
P.count
val O = tpch.loadOrderDF()
O.cache
O.count

   val x42 = O 
val x44 = L 
val x50 = x42.cogroup(x44.groupByKey(x46 => x46.l_orderkey), x45 => x45.o_orderkey)(
   (_, x52, x53) => {
     val x54 = x53.map(x46 => Recordab353457ff7346b0819f06d3db6dcef2(x46.l_returnflag, x46.l_comment, x46.l_linestatus, x46.l_shipmode, x46.l_shipinstruct, x46.l_quantity, x46.l_receiptdate, x46.l_linenumber, x46.l_tax, x46.l_shipdate, x46.l_extendedprice, x46.l_partkey, x46.l_discount, x46.l_commitdate, x46.l_suppkey, x46.l_orderkey)).toSeq
     x52.map(x48 => Record8efe46df39f64707a07b0eb3f9fe1b80(x48.o_shippriority, x48.o_orderdate, x48.o_custkey, x48.o_orderpriority, x54, x48.o_clerk, x48.o_orderstatus, x48.o_totalprice, x48.o_orderkey, x48.o_comment))
 }).as[Record8efe46df39f64707a07b0eb3f9fe1b80]
val x51 = x50
val Test1Full = x51
//Test1Full.print
Test1Full.cache
Test1Full.count


def f = { 
 
 val x76 = Test1Full.select("o_orderdate", "o_parts")
            .as[Record385ff2a289754ccbbbab9e8bf269fe38]
 
val x79 = x76.withColumn("index", monotonically_increasing_id())
 .as[Recordbda23cc1aef14465b522c04bde131be9].flatMap{
   case x96 => if (x96.o_parts.isEmpty) Seq(Recorda1e1f502b53b42978f448f7312b89833(x96.o_orderdate, None, None, x96.index)) 
     else x96.o_parts.map( x97 => Recorda1e1f502b53b42978f448f7312b89833(x96.o_orderdate, Some(x97.l_quantity), Some(x97.l_partkey), x96.index) )
}.as[Recorda1e1f502b53b42978f448f7312b89833]
 
val x81 = P.select("p_name", "p_partkey")
            .as[Record2a270432b3c6461d97135c57e09ade78]
 
val x85 = x79.equiJoin[Record2a270432b3c6461d97135c57e09ade78](x81, Seq("l_partkey","p_partkey"), "left_outer")
 .as[Record8f6bb04f8a31481d88356a3a9c546465]
 
val x91 = x85.reduceByKey(x98 => 
 Record08a3397aceb14e5282225089a0e7bce8(x98.index, x98.o_orderdate, x98.p_name), x => x.l_quantity match {
       case Some(r) => r; case _ => 0.0
   }).mapPartitions(
     it => it.map{ case (x98, x99) => (Record73b99337218e48a6ac1867f0e26e043c(x98.index, x98.o_orderdate), Recorddbb4f3ee216d46018a1c29afa2393053(x98.p_name, Some(x99)))
 }).groupByKey(_._1)
val x94 = x91.mapGroups{
    case (x92, x93) => 
    val grps = x93.flatMap{
      x => x._2.p_name match {
        case None => Seq()
        case _ => Seq(x._2)
      }
    }.toSeq
    Record9c75463c696b4b5ea91449271385506e(x92.o_orderdate, grps)
}.as[Record9c75463c696b4b5ea91449271385506e]
 
val x95 = x94
val Test1NN = x95
//Test1NN.print
//Test1NN.cache
Test1NN.count

}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start 
   println("Flat++,1,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
