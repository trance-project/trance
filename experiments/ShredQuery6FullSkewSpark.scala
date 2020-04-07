
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.HashMap
import sprkloader._
import sprkloader.SkewPairRDD._
import sprkloader.SkewDictRDD._
import sprkloader.SkewTopRDD._
case class Record1492(c__F_c_name: String)
case class Record1493(c_name: String, suppliers: Record1492)
case class Record1498(s_name: String, s_nationkey: Int)
case class Record1499(s_name: String, _1: Record1492)
case class Record1489(s__F_s_suppkey: Int)
case class Record1481(c_name: String, c_nationkey: Int, c_custkey: Int)
case class Record1491(c_name: String, c_nationkey: Int, _1: Record1489)
case class Record1485(l_suppkey: Int, l_orderkey: Int)
case class Record1484(c_orderkey: Int, c_name: String, c_nationkey: Int)
case class Record1480(o_orderkey: Int, o_custkey: Int)
case class Record1488(c_name: String, c_nationkey: Int, c_suppkey: Int)
case class Record1490(s_name: String, s_nationkey: Int, customers2: Record1489)
object ShredQuery6FullSkewSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6FullSkewSpark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val IBag_L__D_L = tpch.loadLineitem()
IBag_L__D_L.cache
spark.sparkContext.runJob(IBag_L__D_L, (iter: Iterator[_]) => {})
val IBag_L__D_H = spark.sparkContext.emptyRDD[Lineitem]
val IBag_L__D = (IBag_L__D_L, IBag_L__D_H)
val IBag_C__D_L = tpch.loadCustomer()
IBag_C__D_L.cache
spark.sparkContext.runJob(IBag_C__D_L, (iter: Iterator[_]) => {})
val IBag_C__D_H = spark.sparkContext.emptyRDD[Customer]
val IBag_C__D = (IBag_C__D_L, IBag_C__D_H)
val IBag_O__D_L = tpch.loadOrder()
IBag_O__D_L.cache
spark.sparkContext.runJob(IBag_O__D_L, (iter: Iterator[_]) => {})
val IBag_O__D_H = spark.sparkContext.emptyRDD[Order]
val IBag_O__D = (IBag_O__D_L, IBag_O__D_H)
val IBag_S__D_L = tpch.loadSupplier()
IBag_S__D_L.cache
spark.sparkContext.runJob(IBag_S__D_L, (iter: Iterator[_]) => {})
val IBag_S__D_H = spark.sparkContext.emptyRDD[Supplier]
val IBag_S__D = (IBag_S__D_L, IBag_S__D_H)

   val x1366 = IBag_O__D.map{ case x1362 => 
   {val x1363 = x1362.o_orderkey 
val x1364 = x1362.o_custkey 
val x1365 = Record1480(x1363, x1364) 
x1365}
} 
val x1372 = IBag_C__D.map{ case x1367 => 
   {val x1368 = x1367.c_name 
val x1369 = x1367.c_nationkey 
val x1370 = x1367.c_custkey 
val x1371 = Record1481(x1368, x1369, x1370) 
x1371}
} 
val x1482 = x1366.map{ case x1373 => ({val x1375 = x1373.o_custkey 
x1375}, x1373) }
val x1483 = x1372.map{ case x1374 => ({val x1376 = x1374.c_custkey 
x1376}, x1374) }
val x1378 = x1482.joinDropKey(x1483)
val x1385 = x1378.map{ case (x1379, x1380) => 
   {val x1381 = x1379.o_orderkey 
val x1382 = x1380.c_name 
val x1383 = x1380.c_nationkey 
val x1384 = Record1484(x1381, x1382, x1383) 
x1384}
} 
val MBag_customers_1 = x1385
val x1386 = MBag_customers_1
//MBag_customers_1.cache
//MBag_customers_1.print
//MBag_customers_1.evaluate
val x1388 = MBag_customers_1 
val x1393 = IBag_L__D.map{ case x1389 => 
   {val x1390 = x1389.l_suppkey 
val x1391 = x1389.l_orderkey 
val x1392 = Record1485(x1390, x1391) 
x1392}
} 
val x1486 = x1388.map{ case x1394 => ({val x1396 = x1394.c_orderkey 
x1396}, x1394) }
val x1487 = x1393.map{ case x1395 => ({val x1397 = x1395.l_orderkey 
x1397}, x1395) }
val x1399 = x1486.joinDropKey(x1487)
val x1406 = x1399.map{ case (x1400, x1401) => 
   {val x1402 = x1400.c_name 
val x1403 = x1400.c_nationkey 
val x1404 = x1401.l_suppkey 
val x1405 = Record1488(x1402, x1403, x1404) 
x1405}
} 
val MBag_csupps_1 = x1406
val x1407 = MBag_csupps_1
//MBag_csupps_1.cache
//MBag_csupps_1.print
//MBag_csupps_1.evaluate
val x1414 = IBag_S__D.map{ case x1408 => 
   {val x1409 = x1408.s_name 
val x1410 = x1408.s_nationkey 
val x1411 = x1408.s_suppkey 
val x1412 = Record1489(x1411) 
val x1413 = Record1490(x1409, x1410, x1412) 
x1413}
} 
val MBag_Query5_1 = x1414
val x1415 = MBag_Query5_1
//MBag_Query5_1.cache
//MBag_Query5_1.print
//MBag_Query5_1.evaluate
val x1422 = MBag_csupps_1.map{ case x1416 => 
   {val x1417 = x1416.c_name 
val x1418 = x1416.c_nationkey 
val x1419 = x1416.c_suppkey 
val x1420 = Record1489(x1419) 
val x1421 = Record1491(x1417, x1418, x1420) 
x1421}
} 
val x1423 = x1422 
val MDict_Query5_1_customers2_1 = x1423
val x1424 = MDict_Query5_1_customers2_1
//MDict_Query5_1_customers2_1.cache
//MDict_Query5_1_customers2_1.print
//MDict_Query5_1_customers2_1.evaluate


val IBag_Query5__D = MBag_Query5_1
IBag_Query5__D.cache
IBag_Query5__D.evaluate
val IDict_Query5__D_customers2 = MDict_Query5_1_customers2_1
IDict_Query5__D_customers2.cache
IDict_Query5__D_customers2.evaluate
 def f = {
 
var start0 = System.currentTimeMillis()
val x1447 = IBag_C__D.map{ case x1443 => 
   {val x1444 = x1443.c_name 
val x1445 = Record1492(x1444) 
val x1446 = Record1493(x1444, x1445) 
x1446}
} 
val MBag_Query6Full_1 = x1447
val x1448 = MBag_Query6Full_1
//MBag_Query6Full_1.cache
//MBag_Query6Full_1.print
//MBag_Query6Full_1.evaluate
val x1452 = MBag_Query6Full_1.createDomain(l => l.suppliers) 
val Dom_suppliers_1 = x1452
val x1453 = Dom_suppliers_1
//Dom_suppliers_1.cache
//Dom_suppliers_1.print
//Dom_suppliers_1.evaluate
val x1455 = Dom_suppliers_1 
val x1457 = MBag_Query5_1 
 val x1494 = x1455
 val x1495 = x1457.map{ case x1459 => (Record1492(x1459._1), Record1490(x1459.s_name, x1459.s_nationkey, x1459.customers2)) }
 val x1461 = x1495.joinDomain(x1494)
 val x1496 = x1461.map{ case (x1462, x1463) => (x1463.customers2, Record1498(x1463.s_name, x1463.s_nationkey))}
val x1469 = x1496.cogroupDropKey(MDict_Query5_1_customers2_1)
val x1476 = x1469.map{ case ((x1470, x1471), x1472) => 
   {val x1473 = x1471.s_name 
val x1474 = x1470 
val x1475 = Record1499(x1473, x1474) 
x1475}
} 
val x1477 = x1476 
val MDict_Query6Full_1_suppliers_1 = x1477
val x1478 = MDict_Query6Full_1_suppliers_1
//MDict_Query6Full_1_suppliers_1.cache
//MDict_Query6Full_1_suppliers_1.print
MDict_Query6Full_1_suppliers_1.evaluate

        
var end0 = System.currentTimeMillis() - start0
println("Shred,Skew,Query6,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("Shred,Skew,Query6,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
