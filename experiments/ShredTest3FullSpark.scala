
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.PairRDDOperations._
import sprkloader.DictRDDOperations._
import sprkloader.TopRDD._
case class Record163(n__F_n_nationkey: Int)
case class Record164(n_regionkey: Int, n_nationkey: Int, n_custs: Record163, n_name: String, n_comment: String)
case class Record165(c__F_c_custkey: Int)
case class Record166(c_acctbal: Double, c_name: String, c_nationkey: Int, _1: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Int, c_mktsegment: String, c_phone: String)
case class Record167(o__F_o_orderkey: Int)
case class Record168(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Record167, _1: Int, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record169(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, _1: Int, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Record172(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, _1: Int, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record173(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Record174(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Vector[Record173], _1: Int, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record177(c_acctbal: Double, c_name: String, c_nationkey: Int, _1: Int, c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String)
case class Record178(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Vector[Record173], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record179(c_acctbal: Double, c_name: String, c_nationkey: Int, _1: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Vector[Record178], c_mktsegment: String, c_phone: String)
case class Record182(n_regionkey: Int, n_nationkey: Int, n_name: String, n_comment: String)
case class Record183(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Vector[Record178], c_mktsegment: String, c_phone: String)
case class Record184(n_regionkey: Int, n_nationkey: Int, n_custs: Vector[Record183], n_name: String, n_comment: String)
object ShredTest3FullSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredTest3FullSpark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val IBag_N__D = tpch.loadNation()
IBag_N__D.cache
spark.sparkContext.runJob(IBag_N__D, (iter: Iterator[_]) => {})
val IBag_L__D = tpch.loadLineitem()
IBag_L__D.cache
spark.sparkContext.runJob(IBag_L__D, (iter: Iterator[_]) => {})
val IBag_C__D = tpch.loadCustomer()
IBag_C__D.cache
spark.sparkContext.runJob(IBag_C__D, (iter: Iterator[_]) => {})
val IBag_O__D = tpch.loadOrder()
IBag_O__D.cache
spark.sparkContext.runJob(IBag_O__D, (iter: Iterator[_]) => {})

    def f = {
 
var start0 = System.currentTimeMillis()
val x19 = IBag_N__D
// .map{ case x12 => 
//    {val x13 = x12.n_regionkey 
// val x14 = x12.n_nationkey 
// val x15 = Record163(x14) 
// val x16 = x12.n_name 
// val x17 = x12.n_comment 
// val x18 = Record164(x13, x14, x15, x16, x17) 
// x18}
// } 
val MBag_Test3Full_1 = x19
val x20 = MBag_Test3Full_1
//MBag_Test3Full_1.cache
//MBag_Test3Full_1.print
// MBag_Test3Full_1.evaluate
val x22 = IBag_C__D 
// val x35 = x22.map{ case x23 => 
//    {val x24 = x23.c_acctbal 
// val x25 = x23.c_name 
// val x26 = x23.c_nationkey 
// val x27 = Record163(x26) 
// val x28 = x23.c_custkey 
// val x29 = x23.c_comment 
// val x30 = x23.c_address 
// val x31 = Record165(x28) 
// val x32 = x23.c_mktsegment 
// val x33 = x23.c_phone 
// val x34 = Record166(x24, x25, x26, x27, x28, x29, x30, x31, x32, x33) 
// x34}
// } 
// val x36 = x35 
val MDict_Test3Full_1_n_custs_1 = x22//x36
val x37 = MDict_Test3Full_1_n_custs_1
//MDict_Test3Full_1_n_custs_1.cache
//MDict_Test3Full_1_n_custs_1.print
// MDict_Test3Full_1_n_custs_1.evaluate
val x39 = IBag_O__D 
// val x53 = x39.map{ case x40 => 
//    {val x41 = x40.o_shippriority 
// val x42 = x40.o_orderdate 
// val x43 = x40.o_custkey 
// val x44 = x40.o_orderpriority 
// val x45 = x40.o_orderkey 
// val x46 = Record167(x45) 
// val x47 = Record165(x43) 
// val x48 = x40.o_clerk 
// val x49 = x40.o_orderstatus 
// val x50 = x40.o_totalprice 
// val x51 = x40.o_comment 
// val x52 = Record168(x41, x42, x43, x44, x46, x47, x48, x49, x50, x45, x51) 
// x52}
// } 
// val x54 = x53 
val MDict_Test3Full_1_n_custs_1_c_orders_1 = x39//x54
val x55 = MDict_Test3Full_1_n_custs_1_c_orders_1
//MDict_Test3Full_1_n_custs_1_c_orders_1.cache
//MDict_Test3Full_1_n_custs_1_c_orders_1.print
// MDict_Test3Full_1_n_custs_1_c_orders_1.evaluate
val x57 = IBag_L__D 
// val x77 = x57.map{ case x58 => 
//    {val x59 = x58.l_returnflag 
// val x60 = x58.l_comment 
// val x61 = x58.l_linestatus 
// val x62 = x58.l_shipmode 
// val x63 = x58.l_shipinstruct 
// val x64 = x58.l_quantity 
// val x65 = x58.l_receiptdate 
// val x66 = x58.l_linenumber 
// val x67 = x58.l_tax 
// val x68 = x58.l_shipdate 
// val x69 = x58.l_orderkey 
// val x70 = Record167(x69) 
// val x71 = x58.l_extendedprice 
// val x72 = x58.l_partkey 
// val x73 = x58.l_discount 
// val x74 = x58.l_commitdate 
// val x75 = x58.l_suppkey 
// val x76 = Record169(x59, x60, x61, x62, x63, x64, x65, x66, x67, x68, x70, x71, x72, x73, x74, x75, x69) 
// x76}
// } 
// val x78 = x77 
val MDict_Test3Full_1_n_custs_1_c_orders_1_o_parts_1 = x57//x78
val x79 = MDict_Test3Full_1_n_custs_1_c_orders_1_o_parts_1
//MDict_Test3Full_1_n_custs_1_c_orders_1_o_parts_1.cache
//MDict_Test3Full_1_n_custs_1_c_orders_1_o_parts_1.print
// MDict_Test3Full_1_n_custs_1_c_orders_1_o_parts_1.evaluate
var end0 = System.currentTimeMillis() - start0
println("Shred,3,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    

var start1 = System.currentTimeMillis()
val x170 = MDict_Test3Full_1_n_custs_1_c_orders_1.map{ case x105 => (x105.o_orderkey, Record172(x105.o_shippriority, x105.o_orderdate, x105.o_custkey, x105.o_orderpriority, x105.o_custkey, x105.o_clerk, x105.o_orderstatus, x105.o_totalprice, x105.o_orderkey, x105.o_comment))}
val x171 = MDict_Test3Full_1_n_custs_1_c_orders_1_o_parts_1.map{ x106 => (x106.l_orderkey, Record173(x106.l_returnflag, x106.l_comment, x106.l_linestatus, x106.l_shipmode, x106.l_shipinstruct, x106.l_quantity, x106.l_receiptdate, x106.l_linenumber, x106.l_tax, x106.l_shipdate, x106.l_extendedprice, x106.l_partkey, x106.l_discount, x106.l_commitdate, x106.l_suppkey, x106.l_orderkey)) }
val x108 = x170.cogroupDropKey(x171)
val x122 = x108.map{ case (x109, x110) => 
   {val x111 = x109.o_shippriority 
val x112 = x109.o_orderdate 
val x113 = x109.o_custkey 
val x114 = x109.o_orderpriority 
val x115 = x109._1 
val x116 = x109.o_clerk 
val x117 = x109.o_orderstatus 
val x118 = x109.o_totalprice 
val x119 = x109.o_orderkey 
val x120 = x109.o_comment 
val x121 = Record174(x111, x112, x113, x114, x110, x115, x116, x117, x118, x119, x120) 
x121}
} 
val x123 = x122 
val UDict_Test3Full_1_n_custs_1_c_orders_1 = x123
val x124 = UDict_Test3Full_1_n_custs_1_c_orders_1
//UDict_Test3Full_1_n_custs_1_c_orders_1.cache
//UDict_Test3Full_1_n_custs_1_c_orders_1.print
// UDict_Test3Full_1_n_custs_1_c_orders_1.evaluate
val x175 = MDict_Test3Full_1_n_custs_1.map{ case x128 => (x128.c_custkey, Record177(x128.c_acctbal, x128.c_name, x128.c_nationkey, x128.c_custkey, x128.c_custkey, x128.c_comment, x128.c_address, x128.c_mktsegment, x128.c_phone))}
val x176 = UDict_Test3Full_1_n_custs_1_c_orders_1.map{ x129 => (x129.o_custkey, Record178(x129.o_shippriority, x129.o_orderdate, x129.o_custkey, x129.o_orderpriority, x129.o_parts, x129.o_clerk, x129.o_orderstatus, x129.o_totalprice, x129.o_orderkey, x129.o_comment)) }
val x131 = x175.cogroupDropKey(x176)
val x144 = x131.map{ case (x132, x133) => 
   {val x134 = x132.c_acctbal 
val x135 = x132.c_name 
val x136 = x132.c_nationkey 
val x137 = x132._1 
val x138 = x132.c_custkey 
val x139 = x132.c_comment 
val x140 = x132.c_address 
val x141 = x132.c_mktsegment 
val x142 = x132.c_phone 
val x143 = Record179(x134, x135, x136, x137, x138, x139, x140, x133, x141, x142) 
x143}
} 
val x145 = x144 
val UDict_Test3Full_1_n_custs_1 = x145
val x146 = UDict_Test3Full_1_n_custs_1
//UDict_Test3Full_1_n_custs_1.cache
//UDict_Test3Full_1_n_custs_1.print
// UDict_Test3Full_1_n_custs_1.evaluate
val x180 = MBag_Test3Full_1.map{ case x149 => (x149.n_nationkey, Record182(x149.n_regionkey, x149.n_nationkey, x149.n_name, x149.n_comment))}
val x181 = UDict_Test3Full_1_n_custs_1.map{ x150 => (x150.c_nationkey, Record183(x150.c_acctbal, x150.c_name, x150.c_nationkey, x150.c_custkey, x150.c_comment, x150.c_address, x150.c_orders, x150.c_mktsegment, x150.c_phone)) }
val x152 = x180.cogroupDropKey(x181)
val x160 = x152.map{ case (x153, x154) => 
   {val x155 = x153.n_regionkey 
val x156 = x153.n_nationkey 
val x157 = x153.n_name 
val x158 = x153.n_comment 
val x159 = Record184(x155, x156, x154, x157, x158) 
x159}
} 
val Test3Full = x160
val x161 = Test3Full
//Test3Full.cache
// Test3Full.print
Test3Full.evaluate

Test3Full.flatMap{
      case nations => if (nations.n_custs.isEmpty) Vector((nations.n_name, null, null, -1, -1.0))
        else nations.n_custs.flatMap{
          case customers => if (customers.c_orders.isEmpty) Vector((nations.n_name, customers.c_name, null, -1, -1.0))
            else customers.c_orders.flatMap{
              case orders => if (orders.o_parts.isEmpty) Vector((nations.n_name, customers.c_name, orders.o_orderdate, -1, -1.0))
                else orders.o_parts.map{ p => (nations.n_name, customers.c_name, orders.o_orderdate, p.l_partkey, p.l_quantity) }
                }}}.collect.foreach(println(_))
        
        
        
var end1 = System.currentTimeMillis() - start1
println("Shred,3,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("Shred,3,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
