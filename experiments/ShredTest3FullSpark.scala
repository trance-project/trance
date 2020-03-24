
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.PairRDDOperations._
import sprkloader.DictRDDOperations._
import sprkloader.TopRDD._
import org.apache.spark.HashPartitioner
case class Record4481(n__F_n_nationkey: Int)
case class Record4482(n_regionkey: Int, n_nationkey: Int, n_custs: Record4481, n_name: String, n_comment: String)
case class Record4484(c__F_c_custkey: Int)
case class Record4485(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Record4484, c_mktsegment: String, c_phone: String)
case class Record4487(o__F_o_orderkey: Int)
case class Record4488(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Record4487, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record4490(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Record4491(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record4492(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_parts: Vector[Record4490], o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record4493(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String)
case class Record4494(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_orders: Vector[Record4492], c_mktsegment: String, c_phone: String)
case class Record4496(n_regionkey: Int, n_nationkey: Int, n_name: String, n_comment: String)
case class Record4497(n_regionkey: Int, n_nationkey: Int, n_custs: Vector[Record4494], n_name: String, n_comment: String)
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
val x4311 = IBag_N__D.map{ case x4304 => 
   {val x4305 = x4304.n_regionkey 
val x4306 = x4304.n_nationkey 
val x4307 = Record4481(x4306) 
val x4308 = x4304.n_name 
val x4309 = x4304.n_comment 
val x4310 = Record4482(x4305, x4306, x4307, x4308, x4309) 
x4310}
} 
val MBag_Test3Full_1 = x4311
val x4312 = MBag_Test3Full_1
MBag_Test3Full_1.cache
//MBag_Test3Full_1.print
MBag_Test3Full_1.evaluate
val x4314 = IBag_C__D 
val x4330 = x4314.map{ 
  case x4315 => ({val x4316 = x4315.c_nationkey 
val x4317 = Record4481(x4316) 
x4317}, {val x4318 = x4315.c_acctbal 
val x4319 = x4315.c_name 
val x4320 = x4315.c_nationkey 
val x4321 = x4315.c_custkey 
val x4322 = x4315.c_comment 
val x4323 = x4315.c_address 
val x4324 = Record4484(x4321) 
val x4325 = x4315.c_mktsegment 
val x4326 = x4315.c_phone 
val x4327 = Record4485(x4318, x4319, x4320, x4321, x4322, x4323, x4324, x4325, x4326) 
x4327})
}.partitionBy(new HashPartitioner(400))//.group(_++_) 
val MDict_Test3Full_1_n_custs_1 = x4330
val x4331 = MDict_Test3Full_1_n_custs_1
MDict_Test3Full_1_n_custs_1.cache
//MDict_Test3Full_1_n_custs_1.print
MDict_Test3Full_1_n_custs_1.evaluate
val x4333 = IBag_O__D 
val x4350 = x4333.map{ 
  case x4334 => ({val x4335 = x4334.o_custkey 
val x4336 = Record4484(x4335) 
x4336}, {val x4337 = x4334.o_shippriority 
val x4338 = x4334.o_orderdate 
val x4339 = x4334.o_custkey 
val x4340 = x4334.o_orderpriority 
val x4341 = x4334.o_orderkey 
val x4342 = Record4487(x4341) 
val x4343 = x4334.o_clerk 
val x4344 = x4334.o_orderstatus 
val x4345 = x4334.o_totalprice 
val x4346 = x4334.o_comment 
val x4347 = Record4488(x4337, x4338, x4339, x4340, x4342, x4343, x4344, x4345, x4341, x4346) 
x4347})
}.partitionBy(new HashPartitioner(500))//.group(_++_) 
val MDict_Test3Full_1_n_custs_1_c_orders_1 = x4350
val x4351 = MDict_Test3Full_1_n_custs_1_c_orders_1
MDict_Test3Full_1_n_custs_1_c_orders_1.cache
//MDict_Test3Full_1_n_custs_1_c_orders_1.print
MDict_Test3Full_1_n_custs_1_c_orders_1.evaluate
val x4353 = IBag_L__D 
val x4376 = x4353.map{ 
  case x4354 => ({val x4355 = x4354.l_orderkey 
val x4356 = Record4487(x4355) 
x4356}, {val x4357 = x4354.l_returnflag 
val x4358 = x4354.l_comment 
val x4359 = x4354.l_linestatus 
val x4360 = x4354.l_shipmode 
val x4361 = x4354.l_shipinstruct 
val x4362 = x4354.l_quantity 
val x4363 = x4354.l_receiptdate 
val x4364 = x4354.l_linenumber 
val x4365 = x4354.l_tax 
val x4366 = x4354.l_shipdate 
val x4367 = x4354.l_extendedprice 
val x4368 = x4354.l_partkey 
val x4369 = x4354.l_discount 
val x4370 = x4354.l_commitdate 
val x4371 = x4354.l_suppkey 
val x4372 = x4354.l_orderkey 
val x4373 = Record4490(x4357, x4358, x4359, x4360, x4361, x4362, x4363, x4364, x4365, x4366, x4367, x4368, x4369, x4370, x4371, x4372) 
x4373})
}.partitionBy(new HashPartitioner(1000))//.group(_++_) 
val MDict_Test3Full_1_n_custs_1_c_orders_1_o_parts_1 = x4376
val x4377 = MDict_Test3Full_1_n_custs_1_c_orders_1_o_parts_1
MDict_Test3Full_1_n_custs_1_c_orders_1_o_parts_1.cache
//MDict_Test3Full_1_n_custs_1_c_orders_1_o_parts_1.print
MDict_Test3Full_1_n_custs_1_c_orders_1_o_parts_1.evaluate
var end0 = System.currentTimeMillis() - start0
println("Shred,3,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)

IBag_L__D.unpersist()
IBag_O__D.unpersist()
IBag_C__D.unpersist()
IBag_N__D.unpersist()

tpch.triggerGC

var start1 = System.currentTimeMillis()
val x4413 = MDict_Test3Full_1_n_custs_1_c_orders_1 
val x4417 = x4413.map{case (x4414, x4416) => 
   (x4416.o_parts, (x4414, Record4491(x4416.o_shippriority, x4416.o_orderdate, x4416.o_custkey, x4416.o_orderpriority, 
    x4416.o_clerk, x4416.o_orderstatus, x4416.o_totalprice, x4416.o_orderkey, x4416.o_comment)))
}
val x4422 = MDict_Test3Full_1_n_custs_1_c_orders_1_o_parts_1.rightCoGroupDropKey(x4417)
val x4437 = x4422.map{ case ((x4423, x4424), x4425) => 
   {val x4426 = x4424.o_shippriority 
val x4427 = x4424.o_orderdate 
val x4428 = x4424.o_custkey 
val x4429 = x4424.o_orderpriority 
val x4430 = x4424.o_clerk 
val x4431 = x4424.o_orderstatus 
val x4432 = x4424.o_totalprice 
val x4433 = x4424.o_orderkey 
val x4434 = x4424.o_comment 
val x4435 = Record4492(x4426, x4427, x4428, x4429, x4425, x4430, x4431, x4432, x4433, x4434) 
val x4436 = (x4423,x4435) 
x4436}
} 
val UDict_Test3Full_1_n_custs_1_c_orders_1 = x4437
val x4438 = UDict_Test3Full_1_n_custs_1_c_orders_1
//UDict_Test3Full_1_n_custs_1_c_orders_1.cache
//UDict_Test3Full_1_n_custs_1_c_orders_1.print
// UDict_Test3Full_1_n_custs_1_c_orders_1.evaluate
val x4440 = MDict_Test3Full_1_n_custs_1 
val x4444 = x4440.map{case (x4441, x4443) => 
   (x4443.c_orders, (x4441, Record4493(x4443.c_acctbal, x4443.c_name, x4443.c_nationkey, x4443.c_custkey, x4443.c_comment, 
    x4443.c_address, x4443.c_mktsegment, x4443.c_phone)))
}
val x4449 = UDict_Test3Full_1_n_custs_1_c_orders_1.rightCoGroupDropKey(x4444)
val x4463 = x4449.map{ case ((x4450, x4451), x4452) => 
   {val x4453 = x4451.c_acctbal 
val x4454 = x4451.c_name 
val x4455 = x4451.c_nationkey 
val x4456 = x4451.c_custkey 
val x4457 = x4451.c_comment 
val x4458 = x4451.c_address 
val x4459 = x4451.c_mktsegment 
val x4460 = x4451.c_phone 
val x4461 = Record4494(x4453, x4454, x4455, x4456, x4457, x4458, x4452, x4459, x4460) 
val x4462 = (x4450,x4461) 
x4462}
} 
val UDict_Test3Full_1_n_custs_1 = x4463
val x4464 = UDict_Test3Full_1_n_custs_1
//UDict_Test3Full_1_n_custs_1.cache
//UDict_Test3Full_1_n_custs_1.print
// UDict_Test3Full_1_n_custs_1.evaluate
val x4466 = MBag_Test3Full_1 
val x4495 = x4466.map{ case x4467 => (x4467.n_custs, Record4496(x4467.n_regionkey, x4467.n_nationkey, x4467.n_name, x4467.n_comment))}
val x4470 = UDict_Test3Full_1_n_custs_1.rightCoGroupDropKey(x4495)
val x4478 = x4470.map{ case (x4471, x4472) => 
   {val x4473 = x4471.n_regionkey 
val x4474 = x4471.n_nationkey 
val x4475 = x4471.n_name 
val x4476 = x4471.n_comment 
val x4477 = Record4497(x4473, x4474, x4472, x4475, x4476) 
x4477}
} 
val Test3Full = x4478
val x4479 = Test3Full
//Test3Full.cache
// Test3Full.print
Test3Full.evaluate



var end1 = System.currentTimeMillis() - start1
println("Shred,3,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("Shred,3,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
