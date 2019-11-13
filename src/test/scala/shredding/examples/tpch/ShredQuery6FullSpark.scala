
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record1572(lbl: Unit)
case class Record1573(o_orderkey: Int, o_custkey: Int)
case class Record1574(c_name: String, c_custkey: Int)
case class Record1575(o_orderkey: Int, c_name: String)
case class Record1642(c__Fc_name: String)
case class Record1643(c_name: String, suppliers: Record1642)
case class Label1644(c__Fc_name: String)
case class Record1645(lbl: Label1644)
case class Record1646(s_name: String)
object ShredQuery6FullSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6FullSpark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L__F = 3
val L__D_1 = tpch.loadLineitem
L__D_1.cache
L__D_1.count
val C__F = 1
val C__D_1 = tpch.loadCustomers
C__D_1.cache
C__D_1.count
val O__F = 2
val O__D_1 = tpch.loadOrders
O__D_1.cache
O__D_1.count
val S__F = 6
val S__D_1 = tpch.loadSupplier
S__D_1.cache
S__D_1.count

   val x1490 = () 
val x1491 = Record1572(x1490) 
val x1492 = List(x1491) 
val M_ctx1 = x1492
val x1493 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x1498 = O__D.map(x1494 => { val x1495 = x1494.o_orderkey 
val x1496 = x1494.o_custkey 
val x1497 = Record1573(x1495, x1496) 
x1497 }) 
val x1499 = C__D_1 
val x1504 = x1499.map(x1500 => { val x1501 = x1500.c_name 
val x1502 = x1500.c_custkey 
val x1503 = Record1574(x1501, x1502) 
x1503 }) 
val x1509 = { val out1 = x1498.map{ case x1505 => ({val x1507 = x1505.o_custkey 
x1507}, x1505) }
  val out2 = x1504.map{ case x1506 => ({val x1508 = x1479.c_custkey 
x1508}, x1506) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1515 = x1509.map{ case (x1510, x1511) => 
   val x1512 = x1510.o_orderkey 
val x1513 = x1479.c_name 
val x1514 = Record1575(x1512, x1513) 
x1514 
} 
val M_flat1 = x1515
val x1516 = M_flat1
//M_flat1.collect.foreach(println(_))
val Query2Full__D_1 = M_flat1
Query2Full__D_1.cache
Query2Full__D_1.count
val Query2Full__D_2customers2_1 = M_flat2
Query2Full__D_2customers2_1.cache
Query2Full__D_2customers2_1.count
def f = { 
 val x1585 = () 
val x1586 = Record1572(x1585) 
val x1587 = List(x1586) 
val M_ctx1 = x1587
val x1588 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x1594 = C__D.map{ case x1589 => 
   val x1590 = x1589.c_name 
val x1591 = Record1642(x1590) 
val x1592 = Record1643(x1590, x1591) 
x1592 
}.filter(x1589 => {val x1593 = true && true 
x1593}) 
val M_flat1 = x1594
val x1595 = M_flat1
//M_flat1.collect.foreach(println(_))
val x1597 = M_flat1 
val x1601 = x1597.map{ case x1598 => 
   val x1599 = x1598.suppliers 
val x1600 = Record1645(x1599) 
x1600 
} 
val x1602 = x1601.distinct 
val M_ctx2 = x1602
val x1603 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x1605 = M_ctx2 
val x1606 = Query2Full__D_1 
val x1610 = x1606.map(x1607 => { val x1608 = x1607.s_name 
val x1609 = Record1646(x1608) 
x1609 }) 
val x1613 = x1610.map{ case c => (x1605.head, c) } 
val x1615 = Query2Full__D_2customers2_1 
val x1618 = x1615 
val x1625 = { val out1 = x1613.map{ case (x1619, x1620) => ({val x1622 = x1619.lbl 
val x1623 = x1622.c__Fc_name 
x1623}, (x1619, x1620)) }
  val out2 = x1618.map{ case x1621 => ({val x1624 = x1581.c_name2 
x1624}, x1621) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1634 = x1625.flatMap{ case ((x1626, x1627), x1628) => val x1633 = (x1627,x1628) 
x1633 match {
   case (_,null) => Nil 
   case x1632 => List(({val x1629 = (x1626) 
x1629}, {val x1630 = x1580.s_name 
val x1631 = Record1646(x1630) 
x1631}))
 }
}.groupByLabel() 
val x1639 = x1634.map{ case (x1635, x1636) => 
   val x1637 = x1635.lbl 
val x1638 = (x1637, x1636) 
x1638 
} 
val M_flat2 = x1639
val x1640 = M_flat2
//M_flat2.collect.foreach(println(_))
x1640.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery6FullSpark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
