
package experiments
/**

inputs:
CustOrders = For c in C Union
  Sng((c_name := c.c_name, corders := For o in O Union
    If (o.o_custkey = c.c_custkey)
    Then Sng((o_orderkey := o.o_orderkey, o_orderdate := o.o_orderdate))))

let parts = 
For l in L Union
  For p in P Union
    If (l.l_partkey = p.p_partkey)
    Then Sng((l_orderkey := l.l_orderkey, p_name := p.p_name, l_qty := l.l_quantity))
in
(For customer in CustOrders Union
  For order in customer.corders Union
    For part in parts Union
      If (order.o_orderkey = part.l_orderkey)
      Then Sng((c_name := customer.c_name, orderdate := order.o_orderdate, pname := part.p_name, l_qty := part.l_qty))).groupBy+((c_name := x1.c_name, orderdate := x1.orderdate, pname := x1.pname)), x1.l_qty)
**/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record23(c__Fc_custkey: Int)
case class Record24(c_name: String, corders: Record23)
case class Record25(o_orderkey: Int, o_orderdate: String)
case class Record101(l_orderkey: Int, l_quantity: Double, l_partkey: Int)
case class Record102(p_name: String, p_partkey: Int)
case class Record103(l_orderkey: Int, p_name: String, l_qty: Double)
case class Record105(c_name: String, orderdate: String, pname: String)
object ShredQuery4SparkOptNestedToFlat {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery4SparkOptNestedToFlat"+sf)
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

   val x10 = C__D_1.map{ case x5 => 
   val x6 = x5.c_name 
val x7 = x5.c_custkey 
val x8 = Record23(x7) 
val x9 = Record24(x6, x8) 
x9 
} 
val M_flat1 = x10
val x11 = M_flat1
//M_flat1.collect.foreach(println(_))
val x20 = O__D_1.map{ case x12 => 
   val x13 = x12.o_custkey 
val x14 = Record23(x13) 
val x15 = x12.o_orderkey 
val x16 = x12.o_orderdate 
val x17 = Record25(x15, x16) 
val x18 = List(x17) 
val x19 = (x14, x18) 
x19 
} 
val M_flat2 = x20
val x21 = M_flat2
//M_flat2.collect.foreach(println(_))
val CustOrders__D_1 = M_flat1
CustOrders__D_1.cache
CustOrders__D_1.count
val CustOrders__D_2corders_1 = M_flat2
CustOrders__D_2corders_1.cache
CustOrders__D_2corders_1.count
def f = { 
/**
parts__D_1 :=  REDUCE[ (l_orderkey := x37.l_orderkey,p_name := x38.p_name,l_qty := x37.l_quantity) / true ]( <-- (x33,x36) -- (
 <-- (x33) -- SELECT[ true, (l_orderkey := x33.l_orderkey,l_quantity := x33.l_quantity,l_partkey := x33.l_partkey) ](L__D_1)) JOIN[x35.l_partkey = x36.p_partkey](

   <-- (x34) -- SELECT[ true, (p_name := x34.p_name,p_partkey := x34.p_partkey) ](P__D_1)))
**/

 val x55 = L__D_1.map(x50 => { val x51 = x50.l_orderkey 
val x52 = x50.l_quantity 
val x53 = x50.l_partkey 
val x54 = Record101(x51, x52, x53) 
x54 }) 
val x60 = P__D_1.map(x56 => { val x57 = x56.p_name 
val x58 = x56.p_partkey 
val x59 = Record102(x57, x58) 
x59 }) 
val x65 = { val out1 = x55.map{ case x61 => ({val x63 = x61.l_partkey 
x63}, x61) }
  val out2 = x60.map{ case x62 => ({val x64 = x62.p_partkey 
x64}, x62) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x72 = x65.map{ case (x66, x67) => 
   val x68 = x66.l_orderkey 
val x69 = x67.p_name 
val x70 = x66.l_quantity 
val x71 = Record103(x68, x69, x70) 
x71 
} 
val parts__D_1 = x72
val x73 = parts__D_1
//parts__D_1.collect.foreach(println(_))
/**
M_flat1 :=   NEST[ U / x48.l_qty / (c_name := x46.c_name,orderdate := x47.o_orderdate,pname := x48.p_name), true / (x46,x47,x48) ]( <-- (x39,x41,x45) -- ( <-- (x39,x41) -- (
 <-- (x39) -- SELECT[ true, x39 ](CustOrders__D_1)) LOOKUP[x40.corders, true = true](
   CustOrders__D_2corders_1)) JOIN[x44.o_orderkey = x45.l_orderkey](

   <-- (x42) -- SELECT[ true, x42 ](parts__D_1)))
**/

val x75 = CustOrders__D_1 
val x79 = { val out1 = x75.map{ case x76 => ({val x78 = x76.corders 
x78}, x76) }
  //val out2 = CustOrders__D_2corders_1.flatMapValues(identity)
  out1.cogroup(CustOrders__D_2corders_1)
  //out1.lookup(out2)
}
val x81 = parts__D_1 
val x87 = { val out1 = x79.map{ case (x82, x83) => ({val x85 = x83.o_orderkey 
x85}, (x82, x83)) }
  val out2 = x81.map{ case x84 => ({val x86 = x84.l_orderkey 
x86}, x84) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x98 = x87.flatMap{ case ((x88, x89), x90) => val x97 = (x88,x89,x90) 
x97 match {
   case (_,null,_) => Nil
case (_,_,null) => Nil
   case x96 => List(({val x91 = x88.c_name 
val x92 = x89.o_orderdate 
val x93 = x90.p_name 
val x94 = Record105(x91, x92, x93) 
x94}, {val x95 = x90.l_qty 
x95}))
 }
}.reduceByKey(_ + _) 
val M_flat1 = x98
val x99 = M_flat1
//M_flat1.collect.foreach(println(_))
x99.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery4SparkOptNestedToFlat"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
