
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.HashMap
import sprkloader._
import sprkloader.SkewPairRDD._
import sprkloader.SkewDictRDD._
import sprkloader.SkewTopRDD._
case class Record1670(c__F_c_name: String)
case class Record1671(c_name: String, suppliers: Record1670)
case class Record1676(s_name: String, s_nationkey: Int)
case class Record1677(s_name: String, _1: Record1670)
case class Record1680(c_name: String)
case class Record1681(s_name: String)
case class Record1682(c_name: String, suppliers: Vector[Record1681])
case class Record1667(s__F_s_suppkey: Int)
case class Record1659(c_name: String, c_nationkey: Int, c_custkey: Int)
case class Record1669(c_name: String, c_nationkey: Int, _1: Record1667)
case class Record1663(l_suppkey: Int, l_orderkey: Int)
case class Record1662(c_orderkey: Int, c_name: String, c_nationkey: Int)
case class Record1658(o_orderkey: Int, o_custkey: Int)
case class Record1666(c_name: String, c_nationkey: Int, c_suppkey: Int)
case class Record1690(c_name: String, c_nationkey: Int)
case class Record1668(s_name: String, s_nationkey: Int, customers2: Record1667)
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

   val x1524 = IBag_O__D.map{ case x1520 => 
   {val x1521 = x1520.o_orderkey 
val x1522 = x1520.o_custkey 
val x1523 = Record1658(x1521, x1522) 
x1523}
} 
val x1530 = IBag_C__D.map{ case x1525 => 
   {val x1526 = x1525.c_name 
val x1527 = x1525.c_nationkey 
val x1528 = x1525.c_custkey 
val x1529 = Record1659(x1526, x1527, x1528) 
x1529}
} 
val x1660 = x1524.map{ case x1531 => ({val x1533 = x1531.o_custkey 
x1533}, x1531) }
val x1661 = x1530.map{ case x1532 => ({val x1534 = x1532.c_custkey 
x1534}, x1532) }
val x1536 = x1660.joinDropKey(x1661)
val x1543 = x1536.map{ case (x1537, x1538) => 
   {val x1539 = x1537.o_orderkey 
val x1540 = x1538.c_name 
val x1541 = x1538.c_nationkey 
val x1542 = Record1662(x1539, x1540, x1541) 
x1542}
} 
val MBag_customers_1 = x1543
val x1544 = MBag_customers_1
//MBag_customers_1.cache
//MBag_customers_1.print
//MBag_customers_1.evaluate
val x1546 = MBag_customers_1 
val x1551 = IBag_L__D.map{ case x1547 => 
   {val x1548 = x1547.l_suppkey 
val x1549 = x1547.l_orderkey 
val x1550 = Record1663(x1548, x1549) 
x1550}
} 
val x1664 = x1546.map{ case x1552 => ({val x1554 = x1552.c_orderkey 
x1554}, x1552) }
val x1665 = x1551.map{ case x1553 => ({val x1555 = x1553.l_orderkey 
x1555}, x1553) }
val x1557 = x1664.joinDropKey(x1665)
val x1564 = x1557.map{ case (x1558, x1559) => 
   {val x1560 = x1558.c_name 
val x1561 = x1558.c_nationkey 
val x1562 = x1559.l_suppkey 
val x1563 = Record1666(x1560, x1561, x1562) 
x1563}
} 
val MBag_csupps_1 = x1564
val x1565 = MBag_csupps_1
//MBag_csupps_1.cache
//MBag_csupps_1.print
//MBag_csupps_1.evaluate
val x1572 = IBag_S__D.map{ case x1566 => 
   {val x1567 = x1566.s_name 
val x1568 = x1566.s_nationkey 
val x1569 = x1566.s_suppkey 
val x1570 = Record1667(x1569) 
val x1571 = Record1668(x1567, x1568, x1570) 
x1571}
} 
val MBag_Query5_1 = x1572
val x1573 = MBag_Query5_1
//MBag_Query5_1.cache
//MBag_Query5_1.print
//MBag_Query5_1.evaluate
val x1580 = MBag_csupps_1.map{ case x1574 => 
   {val x1575 = x1574.c_name 
val x1576 = x1574.c_nationkey 
val x1577 = x1574.c_suppkey 
val x1578 = Record1667(x1577) 
val x1579 = Record1669(x1575, x1576, x1578) 
x1579}
} 
val x1581 = x1580 
val MDict_Query5_1_customers2_1 = x1581
val x1582 = MDict_Query5_1_customers2_1
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
val x1605 = IBag_C__D.map{ case x1601 => 
   {val x1602 = x1601.c_name 
val x1603 = Record1670(x1602) 
val x1604 = Record1671(x1602, x1603) 
x1604}
} 
val MBag_Query6Full_1 = x1605
val x1606 = MBag_Query6Full_1
//MBag_Query6Full_1.cache
//MBag_Query6Full_1.print
MBag_Query6Full_1.evaluate
// val x1610 = MBag_Query6Full_1.createDomain(l => l.suppliers) 
// val Dom_suppliers_1 = x1610
// val x1611 = Dom_suppliers_1
// //Dom_suppliers_1.cache
// //Dom_suppliers_1.print
// //Dom_suppliers_1.evaluate
// val x1613 = Dom_suppliers_1 
val x1615 = MBag_Query5_1 
//  val x1672 = x1613
//  val x1673 = x1615.map{ case x1617 => (Record1670(x1617.customers2), Record1668(x1617.s_name, x1617.s_nationkey, x1617.customers2)) }
//  val x1619 = x1673.joinDomain(x1672)
 val x1674 = x1615.map{ case x1621 => (x1621.customers2, Record1676(x1621.s_name, x1621.s_nationkey))}
val x1626 = MDict_Query5_1_customers2_1.map(v => (v._1, Record1690(v.c_name, v.c_nationkey)))
val x1627 = x1674.joinDropKey(x1626)//cogroupDropKey(x1626)
val x1634 = x1627.map{ case (x1629, x1630) => 
   {val x1631 = x1629.s_name 
val x1632 = Record1670(x1630.c_name)
val x1633 = Record1677(x1631, x1632) 
x1633}
} 
val x1635 = x1634 
val MDict_Query6Full_1_suppliers_1 = x1635
val x1636 = MDict_Query6Full_1_suppliers_1
// MDict_Query6Full_1_suppliers_1.cache
// MDict_Query6Full_1_suppliers_1.print
MDict_Query6Full_1_suppliers_1.evaluate

        
var end0 = System.currentTimeMillis() - start0
println("Shred,Skew,Query6,"+sf+","+Config.datapath+","+end0+",query,"+spark.sparkContext.applicationId)
    

var start1 = System.currentTimeMillis()
val x1678 = MBag_Query6Full_1.map{ case x1647 => (x1647.suppliers, Record1680(x1647.c_name))}
val x1679 = MDict_Query6Full_1_suppliers_1.map{ x1648 => (x1648._1, Record1681(x1648.s_name)) }
val x1650 = x1678.cogroupDropKey(x1679)
val x1655 = x1650.map{ case (x1651, x1652) => 
   {val x1653 = x1651.c_name 
val x1654 = Record1682(x1653, x1652) 
x1654}
} 
val Query6Full = x1655
val x1656 = Query6Full
// Query6Full.cache
// Query6Full.print
// Query6Full.evaluate
// Query6Full.union.flatMap{
//     case cs => if (cs.suppliers.isEmpty) Vector((cs.c_name, null))
//       else cs.suppliers.map( s => (cs.c_name, s.s_name))
//   }.collect.foreach(println(_))
        
var end1 = System.currentTimeMillis() - start1
println("Shred,Skew,Query6,"+sf+","+Config.datapath+","+end1+",unshredding,"+spark.sparkContext.applicationId)
    
}
var start = System.currentTimeMillis()
f
var end = System.currentTimeMillis() - start
    
   println("Shred,Skew,Query6,"+sf+","+Config.datapath+","+end+",total,"+spark.sparkContext.applicationId)
 }
}
