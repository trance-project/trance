
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record149(c__Fc_custkey: Int)
case class Record150(c_name: String, corders: Record149)
case class Record151(o_orderkey: Int, o_orderdate: String)
case class Record227(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record228(p_name: String, p_partkey: Int)
case class Record229(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record231(c_name: String, orderdate: String, pname: String)
object ShredQuery4CSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4CSpark"+sf)
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

   val x136 = C__D_1.map{ case x131 => 
   val x132 = x131.c_name 
val x133 = x131.c_custkey 
val x134 = Record149(x133) 
val x135 = Record150(x132, x134) 
x135 
} 
val M_flat1 = x136
val x137 = M_flat1
//M_flat1.collect.foreach(println(_))
val x146 = O__D_1.map{ case x138 => 
   val x139 = x138.o_custkey 
val x140 = Record149(x139) 
val x141 = x138.o_orderkey 
val x142 = x138.o_orderdate 
val x143 = Record151(x141, x142) 
val x144 = List(x143) 
val x145 = (x140, x144) 
x145 
} 
val M_flat2 = x146
val x147 = M_flat2
//M_flat2.collect.foreach(println(_))
val CustOrders__D_1 = M_flat1
CustOrders__D_1.cache
CustOrders__D_1.count
val CustOrders__D_2corders_1 = M_flat2
CustOrders__D_2corders_1.cache
CustOrders__D_2corders_1.count
def f = { 
 val x181 = L__D_1.map(x176 => { val x177 = x176.l_orderkey 
val x178 = x176.l_quantity 
val x179 = x176.l_partkey 
val x180 = Record227(x177, x178, x179) 
x180 }) 
val x186 = P__D_1.map(x182 => { val x183 = x182.p_name 
val x184 = x182.p_partkey 
val x185 = Record228(x183, x184) 
x185 }) 
val x191 = { val out1 = x181.map{ case x187 => ({val x189 = x187.l_partkey 
x189}, x187) }
  val out2 = x186.map{ case x188 => ({val x190 = x188.p_partkey 
x190}, x188) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x198 = x191.map{ case (x192, x193) => 
   val x194 = x192.l_orderkey 
val x195 = x193.p_name 
val x196 = x192.l_quantity 
val x197 = Record229(x194, x195, x196) 
x197 
} 
val parts__D_1 = x198
val x199 = parts__D_1
//parts__D_1.collect.foreach(println(_))
val x201 = CustOrders__D_1 
/**val x205 = { val out1 = x201.map{ case x202 => ({val x204 = x202.corders
x204}, x202) }
  val out2 = CustOrders__D_2corders_1.flatMapValues(identity)
  out2.lookupSkewLeft(out1)
}
val x207 = parts__D_1
val x213 = { val out1 = x205.map{ case (x209, x208) => ({val x211 = x209.o_orderkey
x211}, (x208, x209)) }
  val out2 = x207.map{ case x210 => ({val x212 = x210.l_orderkey
x212}, x210) }
  out1.join(out2).map{ case (k,v) => v }
} **/
//val x205 = CustOrders__D_2corders_1
val x207 = parts__D_1
val x213 = { val out1 = CustOrders__D_2corders_1.flatMap{ case (x208, x209) => x209.map( v=> ({val x211 = v.o_orderkey
x211}, (x208, v))) }
  val out2 = x207.map{ case x210 => ({val x212 = x210.l_orderkey
x212}, x210) }
  out1.join(out2).map{ case (k,v) => v }
}
val x224 = x213.flatMap{ case ((x214, x215), x216) => val x223 = (x214,x215,x216) 
x223 match {
   case (_,null,_) => Nil
case (_,_,null) => Nil
   case x222 => List(({val x217 = x214//.c_name 
val x218 = x215.o_orderdate 
val x219 = x216.p_name 
val x220 = (x217, x218, x219) 
x220}, {val x221 = x216.l_qty 
x221}))
 }
}.reduceByKey(_ + _) 
val x205 = { val out1 = x201.map{ case x202 => ({val x204 = x202.corders
x204}, x202) }
  val out2 = x224.map{ case ((lbl, orderdate, pname), cnt) => (lbl, (orderdate, pname, cnt))}
  out2.lookupSkewLeft(out1)
}.map{
  case ((orderdate, pname, cnt), cname) => (cname.c_name, orderdate, pname) -> cnt 
}.reduceByKey(_ + _)
val M_flat1 = x205
val x225 = M_flat1
//M_flat1.collect.foreach(println(_))
x225.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery4CSpark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
