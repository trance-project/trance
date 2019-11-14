
package experiments
/** Generated 
resultInner = For o in O Union
  For c in C Union
    If (c.c_custkey = o.o_custkey)
    Then Sng((o_orderkey := o.o_orderkey, c_name := c.c_name))

For s in S Union
  Sng((s_name := s.s_name, customers2 := For l in L Union
    If (s.s_suppkey = l.l_suppkey)
    Then For co in resultInner Union
      If (co.o_orderkey = l.l_orderkey)
      Then Sng((c_name2 := co.c_name))))
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
object Query2SparkUnnest {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query2SparkUnnest"+sf)
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

   def f = {
/**
resultInner :=  REDUCE[ (o_orderkey := x1.o_orderkey,c_name := x2.c_name) / true ]( <-- (x1,x2) -- (
 <-- (x1) -- SELECT[ true, (o_orderkey := x1.o_orderkey,o_custkey := x1.o_custkey) ](O)) JOIN[x1.o_custkey = x2.c_custkey](

   <-- (x2) -- SELECT[ true, (c_name := x2.c_name,c_custkey := x2.c_custkey) ](C)))

 **/ 
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
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
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
/**
 REDUCE[ (s_name := x3.s_name,customers2 := x6) / true ]( <-- (x3,x6) -- NEST[ U / (c_name2 := x5.c_name) / (x3), true / (x4,x5) ]( <-- (x3,x4,x5) -- ( <-- (x3,x4) -- (
 <-- (x3) -- SELECT[ true, (s_name := x3.s_name,s_suppkey := x3.s_suppkey) ](S)) OUTERJOIN[x3.s_suppkey = x4.l_suppkey](

   <-- (x4) -- SELECT[ true, (l_orderkey := x4.l_orderkey,l_suppkey := x4.l_suppkey) ](L))) OUTERJOIN[x4.l_orderkey = x5.o_orderkey](

   <-- (x5) -- SELECT[ true, x5 ](resultInner))))
**/
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
x65.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("Query2SparkUnnest"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
