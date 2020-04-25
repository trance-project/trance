
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
case class Recordbd551d18fd644f76a4158883624aaf7a(c_name: String, c_orders: Seq[Record00f993e61e564a8284a07633e446c239], index: Long)
case class Record4b4036cd33c6492e8013da5a0070e8ff(c_name: String, o_orderdate: Option[String], o_parts: Option[Seq[Record6654cf6625314d08849f78dbdf581679]], index: Long)
case class Recordfb52eaa240484384a05671a3f32d719f(index: Long, c_name: String, o_orderdate: Option[String], o_parts: Option[Seq[Record6654cf6625314d08849f78dbdf581679]])
case class Recordd394f010bbb44c5e94e012d6d3d8889d(o_orderdate: Option[String], l_quantity: Option[Double], c_name: String, l_partkey: Option[Int], index: Long)
case class Recordb9b915a2dc4941e398067b7e48dd8924(p_name: String, p_partkey: Int)
case class Record1a767458e198448a9928a9b3f2f794e4(p_name: Option[String], o_orderdate: Option[String], l_quantity: Option[Double], c_name: String, l_partkey: Option[Int], p_partkey: Option[Int], index: Long)
case class Recordfe59714538a6437b88cdf08d58485206(p_name: Option[String], l_quantity: Option[Double])
case class Record1b825f33ee3946fc908f9c16b1c45d4b(index: Long, c_name: String, o_orderdate: Option[String], p_name: Option[String])
case class Recorda61efcd8b9f7459fa401e8bce343e2b7(index: Long, c_name: String, o_orderdate: Option[String])
case class Record6b22f2ea5b094d3e816c56d3fe39090e(p_name: Option[String], o_orderdate: Option[String], l_quantity: Option[Double], c_name: String, index: Long)
case class Record4e256dd3ca684714883f0a5dcbf1e917(index: Long, c_name: String)
case class Record39a0f9bc61054790a39ad189b103fab3(p_name: String, l_quantity: Double)
case class Record5bab0bf7dd60483d8a1023cc216d0cf2(o_orderdate: String, o_parts: Seq[Record39a0f9bc61054790a39ad189b103fab3])
case class Record9987a3aac3b04f3186e3b575af3dec3c(c_name: String, c_orders: Seq[Record5bab0bf7dd60483d8a1023cc216d0cf2])
case class Recordf825ac3f24fc4f3abc8fee8e62742cbd(c_name: String, c_custkey: Int)
case class Record72d2846c65164acab221063434803ec6(o_custkey: Int, o_orderdate: String, o_orderkey: Int)
case class Record992f5f5c478647f6a8495b78eabdfb45(c_name: String, c_orders: Seq[Record00f993e61e564a8284a07633e446c239])
case class Record6654cf6625314d08849f78dbdf581679(l_partkey: Int, l_quantity: Double)
case class Recorde77832fdfd7946219abe00145d81082f(l_partkey: Int, l_quantity: Double, l_orderkey: Int)
case class Record0fb8e128a45c482d91b244372f94762b(o_custkey: Int, o_orderdate: String, o_parts: Seq[Record6654cf6625314d08849f78dbdf581679])
case class Record00f993e61e564a8284a07633e446c239(o_orderdate: String, o_parts: Seq[Record6654cf6625314d08849f78dbdf581679])
object Test2NNLSkewSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master)
     .setAppName("Test2NNLSkewSpark"+sf)
     .set("spark.sql.shuffle.partitions", Config.lparts.toString)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   implicit val encoderRecord6654cf6625314d08849f78dbdf581679: Encoder[Record6654cf6625314d08849f78dbdf581679] = Encoders.product[Record6654cf6625314d08849f78dbdf581679]
implicit val encoderRecord00f993e61e564a8284a07633e446c239: Encoder[Record00f993e61e564a8284a07633e446c239] = Encoders.product[Record00f993e61e564a8284a07633e446c239]
implicit val encoderRecordfe59714538a6437b88cdf08d58485206: Encoder[Recordfe59714538a6437b88cdf08d58485206] = Encoders.product[Recordfe59714538a6437b88cdf08d58485206]
implicit val encoderRecord39a0f9bc61054790a39ad189b103fab3: Encoder[Record39a0f9bc61054790a39ad189b103fab3] = Encoders.product[Record39a0f9bc61054790a39ad189b103fab3]
implicit val encoderRecord5bab0bf7dd60483d8a1023cc216d0cf2: Encoder[Record5bab0bf7dd60483d8a1023cc216d0cf2] = Encoders.product[Record5bab0bf7dd60483d8a1023cc216d0cf2]
   import spark.implicits._
   val tpch = TPCHLoader(spark)
val L_L = tpch.loadLineitemDF()
L_L.cache
L_L.count
val L_H = Seq.empty[Lineitem].toDS()
val L = (L_L, L_H)
val P_L = tpch.loadPartDF()
P_L.cache
P_L.count
val P_H = Seq.empty[Part].toDS()
val P = (P_L, P_H)
val C_L = tpch.loadCustomerDF()
C_L.cache
C_L.count
val C_H = Seq.empty[Customer].toDS()
val C = (C_L, C_H)
val O_L = tpch.loadOrderDF()
O_L.cache
O_L.count
val O_H = Seq.empty[Order].toDS()
val O = (O_L, O_H)

   val x2853 = O.select("o_custkey", "o_orderdate", "o_orderkey")
            .as[Record72d2846c65164acab221063434803ec6]
 
val x2855 = L.select("l_partkey", "l_quantity", "l_orderkey")
            .as[Recorde77832fdfd7946219abe00145d81082f]
 
val x2861 = x2853.cogroup(x2855.groupByKey(x2857 => x2857.l_orderkey), x2856 => x2856.o_orderkey)(
   (_, x2874, x2875) => {
     val x2876 = x2875.map(x2857 => Record6654cf6625314d08849f78dbdf581679(x2857.l_partkey, x2857.l_quantity)).toSeq
     x2874.map(x2859 => Record0fb8e128a45c482d91b244372f94762b(x2859.o_custkey, x2859.o_orderdate, x2876))
 }).as[Record0fb8e128a45c482d91b244372f94762b]
val x2862 = x2861
val orders = x2862
//orders.print
orders.cache
//orders.count
val x2864 = C.select("c_name", "c_custkey")
            .as[Recordf825ac3f24fc4f3abc8fee8e62742cbd]
 
val x2866 = orders 
val x2872 = x2864.cogroup(x2866.groupByKey(x2868 => x2868.o_custkey), x2867 => x2867.c_custkey)(
   (_, x2877, x2878) => {
     val x2879 = x2878.map(x2868 => Record00f993e61e564a8284a07633e446c239(x2868.o_orderdate, x2868.o_parts)).toSeq
     x2877.map(x2870 => Record992f5f5c478647f6a8495b78eabdfb45(x2870.c_name, x2879))
 }).as[Record992f5f5c478647f6a8495b78eabdfb45]
val x2873 = x2872
val Test2 = x2873
//Test2.print
Test2.cache
Test2.count




def f = { 
 
 val x2912 = Test2 
val x2915 = x2912.withColumn("index", monotonically_increasing_id())
 .as[Recordbd551d18fd644f76a4158883624aaf7a].flatMap{
   case x2943 => if (x2943.c_orders.isEmpty) Seq(Record4b4036cd33c6492e8013da5a0070e8ff(x2943.c_name, None, None, x2943.index)) 
     else x2943.c_orders.map( x2944 => Record4b4036cd33c6492e8013da5a0070e8ff(x2943.c_name, Some(x2944.o_orderdate), Some(x2944.o_parts), x2943.index) )
}.as[Record4b4036cd33c6492e8013da5a0070e8ff]
 
val x2919 = x2915.withColumn("index", monotonically_increasing_id())
 .as[Recordfb52eaa240484384a05671a3f32d719f].flatMap{
   case x2945 => x2945.o_parts match {
     case None => Seq(Recordd394f010bbb44c5e94e012d6d3d8889d(x2945.o_orderdate, None, x2945.c_name, None, x2945.index))
     case Some(bag) if bag.isEmpty => Seq(Recordd394f010bbb44c5e94e012d6d3d8889d(x2945.o_orderdate, None, x2945.c_name, None, x2945.index))
     case Some(bag) => bag.map( x2946 => Recordd394f010bbb44c5e94e012d6d3d8889d(x2945.o_orderdate, Some(x2946.l_quantity), x2945.c_name, Some(x2946.l_partkey), x2945.index) )
 }}.as[Recordd394f010bbb44c5e94e012d6d3d8889d]
 
val x2921 = P.select("p_name", "p_partkey")
            .as[Recordb9b915a2dc4941e398067b7e48dd8924]
 
val x2926 = x2919.equiJoin[Recordb9b915a2dc4941e398067b7e48dd8924, Int](x2921, Seq("l_partkey","p_partkey"), "left_outer")
 .as[Record1a767458e198448a9928a9b3f2f794e4]
 
val x2933 = x2926.reduceByKey(x2947 => 
 Record1b825f33ee3946fc908f9c16b1c45d4b(x2947.index, x2947.c_name, x2947.o_orderdate, x2947.p_name), x => x.l_quantity match {
       case Some(r) => r; case _ => 0.0
   }).mapPartitions(
     it => it.map{ case (x2947, x2948) => (Recorda61efcd8b9f7459fa401e8bce343e2b7(x2947.index, x2947.c_name, x2947.o_orderdate), Recordfe59714538a6437b88cdf08d58485206(x2947.p_name, Some(x2948)))
 })

val x2934 = x2933.setGroups(_._1).mapGroups{
    case (x92, x93) => 
    val grps = x93.flatMap{
      x => x._2.p_name match {
        case None => Seq()
        case _ => Seq(Record39a0f9bc61054790a39ad189b103fab3(x._2.p_name.get, x._2.l_quantity.get))
      }
    }.toSeq
    (x92, grps)
}

val x2938 = x2934
 .setGroups(x2949 =>
   Record4e256dd3ca684714883f0a5dcbf1e917(x2949._1.index,x2949._1.c_name)).mapGroups{
   case (x2950, group) => 
     val ngroup = group.flatMap{
       x2949 => x2949._1.o_orderdate match {
         case None => Seq()
         case _ => Seq(Record5bab0bf7dd60483d8a1023cc216d0cf2(x2949._1.o_orderdate.get,x2949._2))
        }
     }.toSeq
   (x2950, ngroup)
}
 
val x2941 = x2938.mapPartitions{ it => it.map{
 case (x2939,x2940) => Record9987a3aac3b04f3186e3b575af3dec3c(x2939.c_name, x2940)
}}.as[Record9987a3aac3b04f3186e3b575af3dec3c]
 
val x2942 = x2941
val Test2NNL = x2942
//Test2NNL.print
//Test2NNL.cache
  Test2NNL.count
                


}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start 
   println("Flat++,Skew,2,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
