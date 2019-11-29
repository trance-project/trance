
package experiments
/** 
let cflat := For co in Query2 Union
  For co2 in co.customers2 Union
    Sng((c_name := co2.c_name2, s_name := co.s_name))
    
 For c in C Union
  Sng((c_name := c.c_name, suppliers := For cf in cflat Union
    If (cf.c_name = c.c_name)
    Then Sng((s_name := cf.s_name))))
**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record87(o_orderkey: Int, o_custkey: Int)
case class Record88(c_name: String, c_custkey: Int)
case class Record89(o_orderkey: Int, c_name: String)
case class Record90(s_name: String, s_suppkey: Int)
case class Record91(l_orderkey: Int, l_suppkey: Int)
case class Record93(c_name2: String)
case class Record94(s_name: String, customers2: Iterable[Record93])
case class Record152(c_name: String, s_name: String)
case class Record153(c_name: String)
case class Record155(s_name: String)
case class Record156(c_name: String, suppliers: Iterable[Record155])
object Query6SparkUnnest {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query6SparkUnnest"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L = tpch.loadLineitem
L.cache
L.count
val C = tpch.loadCustomers
C.cache
C.count
val O = tpch.loadOrders
O.cache
O.count
val S = tpch.loadSupplier
S.cache
S.count

   val Query2 = {
 val x31 = O.map(x27 => { val x28 = x27.o_orderkey 
val x29 = x27.o_custkey 
val x30 = Record87(x28, x29) 
x30 }) 
val x36 = C.map(x32 => { val x33 = x32.c_name 
val x34 = x32.c_custkey 
val x35 = Record88(x33, x34) 
x35 }) 
val x41 = { val out1 = x31.map{ case x37 => ({val x39 = x37.o_custkey 
x39}, x37) }
  val out2 = x36.map{ case x38 => ({val x40 = x38.c_custkey 
x40}, x38) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x47 = x41.map{ case (x42, x43) => 
   val x44 = x42.o_orderkey 
val x45 = x43.c_name 
val x46 = Record89(x44, x45) 
x46 
} 
val resultInner = x47
val x48 = resultInner
//resultInner.collect.foreach(println(_))
val x53 = S.map(x49 => { val x50 = x49.s_name 
val x51 = x49.s_suppkey 
val x52 = Record90(x50, x51) 
x52 }) 
val x58 = L.map(x54 => { val x55 = x54.l_orderkey 
val x56 = x54.l_suppkey 
val x57 = Record91(x55, x56) 
x57 }) 
val x63 = { val out1 = x53.map{ case x59 => ({val x61 = x59.s_suppkey 
x61}, x59) }
  val out2 = x58.map{ case x60 => ({val x62 = x60.l_suppkey 
x62}, x60) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x65 = resultInner 
val x71 = { val out1 = x63.map{ case (x66, x67) => ({val x69 = x67.l_orderkey 
x69}, (x66, x67)) }
  val out2 = x65.map{ case x68 => ({val x70 = x68.o_orderkey 
x70}, x68) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x80 = x71.flatMap{ case ((x72, x73), x74) => val x79 = (x73,x74) 
x79 match {
   case (_,null) => Nil 
   case x78 => List(({val x75 = (x72) 
x75}, {val x76 = x74.c_name 
val x77 = Record93(x76) 
x77}))
 }
}.groupByKey() 
val x85 = x80.map{ case (x81, x82) => 
   val x83 = x81.s_name 
val x84 = Record94(x83, x82) 
x84 
} 
x85
}
Query2.cache
Query2.count
def f = { 
 val x115 = Query2 
val x119 = x115.flatMap{ case x116 => x116 match {
   case null => List((x116, null))
   case _ =>
   val x117 = x116.customers2 
x117 match {
     case x118 => x118.map{ case v2 => (x116, v2) }
  }
 }} 
val x125 = x119.map{ case (x120, x121) => 
   val x122 = x121.c_name2 
val x123 = x120.s_name 
val x124 = Record152(x122, x123) 
x124 
} 
val cflat = x125
val x126 = cflat
//cflat.collect.foreach(println(_))
val x130 = C.map(x127 => { val x128 = x127.c_name 
val x129 = Record153(x128) 
x129 }) 
val x132 = cflat 
val x137 = { val out1 = x130.map{ case x133 => ({val x135 = x133.c_name 
x135}, x133) }
  val out2 = x132.map{ case x134 => ({val x136 = x134.c_name 
x136}, x134) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x145 = x137.flatMap{ case (x138, x139) => val x144 = (x139) 
x144 match {
   case (null) => Nil 
   case x143 => List(({val x140 = (x138) 
x140}, {val x141 = x139.s_name 
val x142 = Record155(x141) 
x142}))
 }
}.groupByKey() 
val x150 = x145.map{ case (x146, x147) => 
   val x148 = x146.c_name 
val x149 = Record156(x148, x147) 
x149 
} 
x150.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("Query6SparkUnnest"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
