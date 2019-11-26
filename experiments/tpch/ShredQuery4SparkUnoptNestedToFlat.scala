
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record151(c__Fc_custkey: Int)
case class Record152(c_name: String, corders: Record151)
case class Record153(o_orderkey: Int, o_orderdate: String)
case class Record228(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record229(p_name: String, p_partkey: Int)
case class Record230(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record232(orderdate: String, pname: String)
object ShredQuery4SparkUnoptNestedToFlat {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4SparkUnoptNestedToFlat"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L__F = 3
val L__D_1 = tpch.loadLineitem
L__D_1.cache
L__D_1.count
val P__F = 4
val P__D_1 = tpch.loadPart
P__D_1.cache
P__D_1.count
val C__F = 1
val C__D_1 = tpch.loadCustomers
C__D_1.cache
C__D_1.count
val O__F = 2
val O__D_1 = tpch.loadOrders
O__D_1.cache
O__D_1.count

   val x138 = C__D_1.map{ case x133 => 
   val x134 = x133.c_name 
val x135 = x133.c_custkey 
val x136 = Record151(x135) 
val x137 = Record152(x134, x136) 
x137 
} 
val M_flat1 = x138
val x139 = M_flat1
//M_flat1.collect.foreach(println(_))
val x148 = O__D_1.map{ case x140 => 
   val x141 = x140.o_custkey 
val x142 = Record151(x141) 
val x143 = x140.o_orderkey 
val x144 = x140.o_orderdate 
val x145 = Record153(x143, x144) 
val x146 = List(x145) 
val x147 = (x142, x146) 
x147 
} 
val M_flat2 = x148
val x149 = M_flat2
//M_flat2.collect.foreach(println(_))
val CustOrders__D_1 = M_flat1
CustOrders__D_1.cache
CustOrders__D_1.count
val CustOrders__D_2corders_1 = M_flat2
CustOrders__D_2corders_1.cache
CustOrders__D_2corders_1.count
def f = { 
 val x183 = L__D_1.map(x178 => { val x179 = x178.l_orderkey 
val x180 = x178.l_quantity 
val x181 = x178.l_partkey 
val x182 = Record228(x179, x180, x181) 
x182 }) 
val x188 = P__D_1.map(x184 => { val x185 = x184.p_name 
val x186 = x184.p_partkey 
val x187 = Record229(x185, x186) 
x187 }) 
val x193 = { val out1 = x183.map{ case x189 => ({val x191 = x189.l_partkey 
x191}, x189) }
  val out2 = x188.map{ case x190 => ({val x192 = x190.p_partkey 
x192}, x190) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x200 = x193.map{ case (x194, x195) => 
   val x196 = x194.l_orderkey 
val x197 = x195.p_name 
val x198 = x194.l_quantity 
val x199 = Record230(x196, x197, x198) 
x199 
} 
val parts__D_1 = x200
val x201 = parts__D_1
//parts__D_1.collect.foreach(println(_))
val x203 = CustOrders__D_1 
val x207 = { val out1 = x203.map{ case x204 => ({val x206 = x204.corders 
x206}, x204) }
  val out2 = CustOrders__D_2corders_1.flatMapValues(identity)
  out1.lookup(out2)
} 
val x209 = parts__D_1 
val x215 = { val out1 = x207.map{ case (x210, x211) => ({val x213 = x211.o_orderkey 
x213}, (x210, x211)) }
  val out2 = x209.map{ case x212 => ({val x214 = x212.l_orderkey 
x214}, x212) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x225 = x215.flatMap{ case ((x216, x217), x218) => val x224 = (x217,x218) 
x224 match {
   case (_,null) => Nil
   case x223 => List(({val x219 = x217.o_orderdate 
val x220 = x218.p_name 
val x221 = Record232(x219, x220) 
x221}, {val x222 = x218.l_qty 
x222}))
 }
}.reduceByKey(_ + _) 
val M_flat1 = x225
val x226 = M_flat1
//M_flat1.collect.foreach(println(_))
x226.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery4SparkUnoptNestedToFlat"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
