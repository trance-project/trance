
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
case class Record67(o_orderkey: Int, o_custkey: Int)
case class Record68(c_name: String, c_custkey: Int)
case class Record69(o_orderkey: Int, c_name: String)
case class Record70(s_name: String, s_suppkey: Int)
case class Record71(l_orderkey: Int, l_suppkey: Int)
case class Record73(c_name2: String)
case class Record74(s_name: String, customers2: Iterable[Record73])
case class Record110(c_name: String)
case class Record112(s_name: String)
case class Record113(c_name: String, suppliers: Iterable[Record112])
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
 val x11 = O.map(x7 => { val x8 = x7.o_orderkey 
val x9 = x7.o_custkey 
val x10 = Record67(x8, x9) 
x10 }) 
val x16 = C.map(x12 => { val x13 = x12.c_name 
val x14 = x12.c_custkey 
val x15 = Record68(x13, x14) 
x15 }) 
val x21 = { val out1 = x11.map{ case x17 => ({val x19 = x17.o_custkey 
x19}, x17) }
  val out2 = x16.map{ case x18 => ({val x20 = x18.c_custkey 
x20}, x18) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x27 = x21.map{ case (x22, x23) => 
   val x24 = x22.o_orderkey 
val x25 = x23.c_name 
val x26 = Record69(x24, x25) 
x26 
} 
val resultInner = x27
val x28 = resultInner
//resultInner.collect.foreach(println(_))
val x33 = S.map(x29 => { val x30 = x29.s_name 
val x31 = x29.s_suppkey 
val x32 = Record70(x30, x31) 
x32 }) 
val x38 = L.map(x34 => { val x35 = x34.l_orderkey 
val x36 = x34.l_suppkey 
val x37 = Record71(x35, x36) 
x37 }) 
val x43 = { val out1 = x33.map{ case x39 => ({val x41 = x39.s_suppkey 
x41}, x39) }
  val out2 = x38.map{ case x40 => ({val x42 = x40.l_suppkey 
x42}, x40) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x45 = resultInner 
val x51 = { val out1 = x43.map{ case (x46, x47) => ({val x49 = x47.l_orderkey 
x49}, (x46, x47)) }
  val out2 = x45.map{ case x48 => ({val x50 = x48.o_orderkey 
x50}, x48) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x60 = x51.flatMap{ case ((x52, x53), x54) => val x59 = (x53,x54) 
x59 match {
   case (_,null) => Nil 
   case x58 => List(({val x55 = (x52) 
x55}, {val x56 = x54.c_name 
val x57 = Record73(x56) 
x57}))
 }
}.groupByKey() 
val x65 = x60.map{ case (x61, x62) => 
   val x63 = x61.s_name 
val x64 = Record74(x63, x62) 
x64 
} 
x65
}
Query2.cache
Query2.count
def f = { 
  /**
 REDUCE[ (c_name := x75.c_name,suppliers := x78) / true ]( <-- (x75,x78) -- NEST[ U / (s_name := x76.s_name) / (x75), x77.c_name2 == x75.c_name / (x76,x77) ](  <-- (x75,x76,x77) -- OUTERUNNEST[ x76.customers2 / true ]( <-- (x75,x76) -- (
 <-- (x75) -- SELECT[ true, (c_name := x75.c_name) ](C)) OUTERJOIN[true = true](

   <-- (x76) -- SELECT[ true, x76 ](Query2)))))
  **/
 val x82 = C.map(x79 => { val x80 = x79.c_name 
val x81 = Record110(x80) 
x81 }) 
val x84 = Query2 
val x87 = { val out1 = x82.map{ case x85 => ({true}, x85) }
  val out2 = x84.map{ case x86 => ({true}, x86) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x92 = x87.flatMap{ case (x88, x89) => (x88, x89) match {
   case (_, null) => List(((x88, x89), null))
   case _ => 
   {val x90 = x89.customers2 
x90} match {
     case Nil => List(((x88, x89), null))
     case lst => lst.map{ case x91 => ((x88, x89), x91) }
  }
 }} 
val x104 = x92.flatMap{ case ((x93, x94), x95) => val x103 = (x94,x95) 
x103 match {
   case x99 if {val x100 = x95.c_name2 
val x101 = x93.c_name 
val x102 = x100 == x101 
x102} => List(({val x96 = (x93) 
x96}, {val x97 = x94.s_name 
val x98 = Record112(x97) 
x98}))
   case x99 => List(({val x96 = (x93) 
x96}, null))
 }    
}.groupByKey() 
val x109 = x104.map{ case (x105, x106) => 
   val x107 = x105.c_name 
val x108 = Record113(x107, x106) 
x108 
} 
x109.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("Query6SparkUnnest"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
