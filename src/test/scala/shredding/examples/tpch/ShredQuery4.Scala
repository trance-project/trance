
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record1548(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record1549(c_name: String, c_orders: Record1548)
case class Record1555(o__F: Orders, P__F: Int, L__F: Int)
case class Record1556(o_orderdate: String, o_parts: Record1555)
case class Record1564(p_name: String, l_qty: Double)
case class Record1650(lbl: Q1Flat2)
case class Input_Query4__DFlat1651(c_name: String, c_orders: Int)
case class Flat1652(o_orderdate: String, o_parts: Int)
case class Dict1652(o_parts: (List[(Int, List[Record1564])], Unit))
case class Input_Query4__DDict1651(c_orders: (List[(Int, List[Flat1652])], Dict1652))
case class Record1663(c_name: String, p_name: String, month: String, t_qty: Double)
case class Record1664(_1: Q1Flat2, _2: List[Record1663])
case class RecM_flat2(_1: Record1548, _2: List[Record1556])
case class RecM_flat3(_1: Record1555, _2: List[Record1564])
case class RecM_flat1(_1: Q1Flat, _2: List[Record1549])
case class RecM_ctx3(lbl: Record1555)
case class RecM_ctx2(lbl: Record1548)
case class RecM_ctx1(lbl: Q1Flat)
object ShredQuery4 {
 def main(args: Array[String]){
    var start0 = System.currentTimeMillis()
    
val C__F = 1
val C__D = (List((C__F, TPCHLoader.loadCustomer[Customer].toList)), ())
val O__F = 2
val O__D = (List((O__F, TPCHLoader.loadOrders[Orders].toList)), ())
val L__F = 3
val L__D = (List((L__F, TPCHLoader.loadLineitem[Lineitem].toList)), ())
val P__F = 4
val P__D = (List((P__F, TPCHLoader.loadPart[Part].toList)), ())
    val ShredQuery4 = { val x1414 = Q1Flat(P__F, C__F, L__F, O__F)
val x1415 = RecM_ctx1(x1414)
val x1416 = List(x1415)
val M_ctx1 = x1416
val x1417 = M_ctx1
val x1419 = M_ctx1
val x1420 = C__D._1
val x1422 = x1420
val x1427 = { val hm1545 = x1419.groupBy{case x1423 => { val x1425 = x1423.lbl
val x1426 = x1425.C__F
x1426 } }
 x1422.flatMap{x1424 => hm1545.get(x1424._1) match {
 case Some(a) => a.map(a1 => (a1, x1424._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x1439 = { val grps1546 = x1427.groupBy{ case (x1428, x1429) => { val x1430 = (x1428)
x1430 }}
 grps1546.map(x1438 => (x1438._1, x1438._2.map{case (x1428, x1429) => val x1431 = x1429.c_name
val x1432 = x1428.lbl
val x1433 = x1432.P__F
val x1434 = x1432.L__F
val x1435 = x1432.O__F
val x1436 = Record1548(x1433, x1434, x1435, x1429)
val x1437 = Record1549(x1431, x1436)
x1437})).toList }
val x1444 = x1439.map{ case (x1440, x1441) => { 
  val x1442 = x1440.lbl
  val x1443 = RecM_flat1(x1442, x1441)
  x1443 }}
val M_flat1 = x1444
val x1445 = M_flat1
val x1447 = M_flat1
val x1451 = x1447.flatMap{ case x1448 => 
  val x1449 = x1448._2
  x1449.map(x1450 => {
  val nv1550 = (x1448, x1450) 
     nv1550
})}
val x1456 = x1451.map{ case (x1452, x1453) => { 
  val x1454 = x1453.c_orders
  val x1455 = RecM_ctx2(x1454)
  x1455 }}
val x1457 = x1456.distinct
val M_ctx2 = x1457
val x1458 = M_ctx2
val x1460 = M_ctx2
val x1461 = O__D._1
val x1463 = x1461
val x1472 = { val hm1552 = x1460.groupBy{case x1464 => { val x1466 = x1464.lbl
val x1467 = x1466.O__F
x1467 } }
 val join1 = x1463.flatMap{x1465 => hm1552.get(x1465._1) match {
 case Some(a) => x1465._2
 case _ => Nil
 }}
 val join2 = x1460.groupBy{case x1464 => { val x1469 = x1464.lbl
val x1470 = x1469.c__F
val x1471 = x1470.c_custkey
x1471 } }
 join1.flatMap(x1465 => join2.get({ val x1468 = x1465.o_custkey
x1468 }) match {
   case Some(a) => a.map(a1 => (a1, x1465))
   case _ => Nil
 })
}
val x1483 = { val grps1553 = x1472.groupBy{ case (x1473, x1474) => { val x1475 = (x1473)
x1475 }}
 grps1553.map(x1482 => (x1482._1, x1482._2.map{case (x1473, x1474) => val x1476 = x1474.o_orderdate
val x1477 = x1473.lbl
val x1478 = x1477.P__F
val x1479 = x1477.L__F
val x1480 = Record1555(x1474, x1478, x1479)
val x1481 = Record1556(x1476, x1480)
x1481})).toList }
val x1488 = x1483.map{ case (x1484, x1485) => { 
  val x1486 = x1484.lbl
  val x1487 = RecM_flat2(x1486, x1485)
  x1487 }}
val M_flat2 = x1488
val x1489 = M_flat2
val x1491 = M_flat2
val x1495 = x1491.flatMap{ case x1492 => 
  val x1493 = x1492._2
  x1493.map(x1494 => {
  val nv1557 = (x1492, x1494) 
     nv1557
})}
val x1500 = x1495.map{ case (x1496, x1497) => { 
  val x1498 = x1497.o_parts
  val x1499 = RecM_ctx3(x1498)
  x1499 }}
val x1501 = x1500.distinct
val M_ctx3 = x1501
val x1502 = M_ctx3
val x1504 = M_ctx3
val x1505 = L__D._1
val x1507 = x1505
val x1516 = { val hm1559 = x1504.groupBy{case x1508 => { val x1510 = x1508.lbl
val x1511 = x1510.L__F
x1511 } }
 val join1 = x1507.flatMap{x1509 => hm1559.get(x1509._1) match {
 case Some(a) => x1509._2
 case _ => Nil
 }}
 val join2 = x1504.groupBy{case x1508 => { val x1513 = x1508.lbl
val x1514 = x1513.o__F
val x1515 = x1514.o_orderkey
x1515 } }
 join1.flatMap(x1509 => join2.get({ val x1512 = x1509.l_orderkey
x1512 }) match {
   case Some(a) => a.map(a1 => (a1, x1509))
   case _ => Nil
 })
}
val x1517 = P__D._1
val x1519 = x1517
val x1527 = { val hm1561 = x1516.groupBy{case (x1520, x1521) => { val x1523 = x1520.lbl
val x1524 = x1523.P__F
x1524 } }
 val join1 = x1519.flatMap{x1522 => hm1561.get(x1522._1) match {
 case Some(a) => x1522._2
 case _ => Nil
 }}
 val join2 = x1516.groupBy{case (x1520, x1521) => { val x1526 = x1521.l_partkey
x1526 } }
 join1.flatMap(x1522 => join2.get({ val x1525 = x1522.p_partkey
x1525 }) match {
   case Some(a) => a.map(a1 => (a1, x1522))
   case _ => Nil
 })
}
val x1536 = { val grps1562 = x1527.groupBy{ case ((x1528, x1529), x1530) => { val x1531 = (x1528)
x1531 }}
 grps1562.map(x1535 => (x1535._1, x1535._2.map{case ((x1528, x1529), x1530) => val x1532 = x1530.p_name
val x1533 = x1529.l_quantity
val x1534 = Record1564(x1532, x1533)
x1534})).toList }
val x1541 = x1536.map{ case (x1537, x1538) => { 
  val x1539 = x1537.lbl
  val x1540 = RecM_flat3(x1539, x1538)
  x1540 }}
val M_flat3 = x1541
val x1542 = M_flat3
val x1543 = (x1417,x1445,x1458,x1489,x1502,x1542)
x1543 }
    var end0 = System.currentTimeMillis() - start0
    
case class Input_Q1_Dict2(o_parts: (List[RecM_flat3], Unit))
case class Input_Q1_Dict1(c_orders: (List[RecM_flat2], Input_Q1_Dict2))
val Query4__F = ShredQuery4._1.head.lbl
val Query4__D = (ShredQuery4._2, Input_Q1_Dict1((ShredQuery4._4, Input_Q1_Dict2((ShredQuery4._6, Unit)))))
    def f(){
      val x1576 = Q1Flat2(Query4__F)
val x1577 = Record1650(x1576)
val x1578 = List(x1577)
val M_ctx1 = x1578
val x1579 = M_ctx1
val x1581 = M_ctx1
val x1582 = Query4__D._1
val x1584 = x1582
val x1589 = { val hm1654 = x1581.groupBy{case x1585 => { val x1587 = x1585.lbl
val x1588 = x1587.Query4__F
x1588 } }
 x1584.flatMap{x1586 => hm1654.get(x1586._1) match {
 case Some(a) => a.map(a1 => (a1, x1586._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x1590 = Query4__D._2
val x1591 = x1590.c_orders
val x1592 = x1591._1
val x1594 = x1592
val x1599 = { val hm1656 = x1589.groupBy{case (x1595, x1596) => { val x1598 = x1596.c_orders
x1598 } }
 x1594.flatMap{x1597 => hm1656.get(x1597._1) match {
 case Some(a) => a.map(a1 => (a1, x1597._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x1600 = x1591._2
val x1601 = x1600.o_parts
val x1602 = x1601._1
val x1604 = x1602
val x1610 = { val hm1657 = x1599.groupBy{case ((x1605, x1606), x1607) => { val x1609 = x1607.o_parts
x1609 } }
 x1604.flatMap{x1608 => hm1657.get(x1608._1) match {
 case Some(a) => a.map(a1 => (a1, x1608._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x1612 = x1602
val x1621 = { val hm1658 = x1610.groupBy{case (((x1613, x1614), x1615), x1616) => { val x1618 = x1615.o_parts
x1618 } }
 val join1 = x1612.flatMap{x1617 => hm1658.get(x1617._1) match {
 case Some(a) => x1617._2
 case _ => Nil
 }}
 val join2 = x1610.groupBy{case (((x1613, x1614), x1615), x1616) => { val x1620 = x1616.p_name
x1620 } }
 join1.flatMap(x1617 => join2.get({ val x1619 = x1617.p_name
x1619 }) match {
   case Some(a) => a.map(a1 => (a1, x1617))
   case _ => Nil
 })
}
val x1630 = { val grps1659 = x1621.groupBy{ case ((((x1622, x1623), x1624), x1625), x1626) => { val x1627 = (x1622,x1623,x1624,x1625)
x1627 }}
 grps1659.map(x1629 => (x1629._1, x1629._2.foldLeft(0.0){ case (acc1660, ((((x1622, x1623), x1624), x1625), x1626)) => acc1660 + {val x1628 = x1626.l_qty
x1628} })).toList }
val x1642 = { val grps1661 = x1630.groupBy{ case ((x1631, x1632, x1633, x1634), x1635) => { val x1636 = (x1631)
x1636 }}
 grps1661.map(x1641 => (x1641._1, x1641._2.map{case ((x1631, x1632, x1633, x1634), x1635) => val x1637 = x1632.c_name
val x1638 = x1634.p_name
val x1639 = x1633.o_orderdate
val x1640 = Record1663(x1637, x1638, x1639, x1635)
x1640})).toList }
val x1647 = x1642.map{ case (x1643, x1644) => { 
  val x1645 = x1643.lbl
  val x1646 = Record1664(x1645, x1644)
  x1646 }}
val M_flat1 = x1647
val x1648 = M_flat1
val x1649 = (x1579,x1648)
x1649
    }
    var time = List[Long]()
    for (i <- 1 to 5) {
     var start = System.currentTimeMillis()
      f
      var end = System.currentTimeMillis() - start
      time = time :+ end
    }
    val avg = (time.sum/5)
    println(end0+","+avg)
 }
}
