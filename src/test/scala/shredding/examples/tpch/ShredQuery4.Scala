
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
case class Record1557(P__F: Int, L__F: Int, O__F: Int, c__F: Customer, uniqueId: Long) extends CaseClassRecord
case class Record1558(c_name: String, c_orders: Record1557, uniqueId: Long) extends CaseClassRecord
case class Record1563(o__F: Orders, P__F: Int, L__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record1564(o_orderdate: String, o_parts: Record1563, uniqueId: Long) extends CaseClassRecord
case class Record1571(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record1659(lbl: Q1Flat2, uniqueId: Long) extends CaseClassRecord
case class Input_Query4__DFlat1660(c_name: String, c_orders: Int, uniqueId: Long) extends CaseClassRecord
case class Flat1661(o_orderdate: String, o_parts: Int, uniqueId: Long) extends CaseClassRecord
case class Dict1661(o_parts: (List[(Int, List[Record1571])], Unit), uniqueId: Long) extends CaseClassRecord
case class Input_Query4__DDict1660(c_orders: (List[(Int, List[Flat1661])], Dict1661), uniqueId: Long) extends CaseClassRecord
case class Record1672(c_name: String, p_name: String, month: String, t_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record1673(_1: Q1Flat2, _2: List[Record1672], uniqueId: Long) extends CaseClassRecord
case class RecM_flat2(_1: Record1557, _2: List[Record1564], uniqueId: Long) extends CaseClassRecord
case class RecM_flat3(_1: Record1563, _2: List[Record1571], uniqueId: Long) extends CaseClassRecord
case class RecM_flat1(_1: Q1Flat, _2: List[Record1558], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx3(lbl: Record1563, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx2(lbl: Record1557, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx1(lbl: Q1Flat, uniqueId: Long) extends CaseClassRecord
object ShredQuery4 {
 def main(args: Array[String]){
    var start0 = System.currentTimeMillis()
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
    
val C__F = 1
val C__D = (List((C__F, TPCHLoader.loadCustomer[Customer].toList)), ())
val O__F = 2
val O__D = (List((O__F, TPCHLoader.loadOrders[Orders].toList)), ())
val L__F = 3
val L__D = (List((L__F, TPCHLoader.loadLineitem[Lineitem].toList)), ())
val P__F = 4
val P__D = (List((P__F, TPCHLoader.loadPart[Part].toList)), ())
    val ShredQuery4 = { val x1420 = Q1Flat(P__F, C__F, L__F, O__F, newId) 
val x1421 = RecM_ctx1(x1420, newId) 
val x1422 = List(x1421) 
val M_ctx1 = x1422
val x1423 = M_ctx1
val x1425 = M_ctx1 
val x1426 = C__D._1 
val x1428 = x1426 
val x1433 = { val hm1554 = x1425.groupBy{case x1429 => { val x1431 = x1429.lbl 
val x1432 = x1431.C__F 
x1432   } }
 x1428.flatMap{x1430 => hm1554.get(x1430._1) match {
 case Some(a) => a.map(a1 => (a1, x1430._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
} 
val x1446 = { val grps1555 = x1433.groupBy{ case (x1434, x1435) => { val x1436 = (x1434) 
x1436  } }
 grps1555.toList.map(x1444 => (x1444._1, x1444._2.flatMap{ 
   case (x1434, null) =>  Nil
   case (x1434, x1435) => {val x1445 = (x1435) 
x1445 } match {
   case (null) => Nil
   case _ => List({val x1437 = x1435.c_name 
val x1438 = x1434.lbl 
val x1439 = x1438.P__F 
val x1440 = x1438.L__F 
val x1441 = x1438.O__F 
val x1442 = Record1557(x1439, x1440, x1441, x1435, newId) 
val x1443 = Record1558(x1437, x1442, newId) 
x1443       })
 }
} ) ) } 
val x1451 = x1446.map{ case (x1447, x1448) => { 
  val x1449 = x1447.lbl 
  val x1450 = RecM_flat1(x1449, x1448, newId) 
  x1450 }} 
val M_flat1 = x1451
val x1452 = M_flat1
val x1454 = M_flat1 
val x1458 = x1454.flatMap{ case x1455 => 
  val x1456 = x1455._2 
  x1456.flatMap(x1457 => {
    List((x1455, x1457))
})} 
val x1463 = x1458.map{ case (x1459, x1460) => { 
  val x1461 = x1460.c_orders 
  val x1462 = RecM_ctx2(x1461, newId) 
  x1462 }} 
val x1464 = x1463.distinct 
val M_ctx2 = x1464
val x1465 = M_ctx2
val x1467 = M_ctx2 
val x1468 = O__D._1 
val x1470 = x1468 
val x1479 = { val hm1560 = x1467.groupBy{case x1471 => { val x1473 = x1471.lbl 
val x1474 = x1473.O__F 
x1474   } }
 val join1 = x1470.flatMap{x1472 => hm1560.get(x1472._1) match {
 case Some(a) => x1472._2
 case _ => Nil
 }}
 val join2 = x1467.groupBy{case x1471 => { val x1476 = x1471.lbl 
val x1477 = x1476.c__F 
val x1478 = x1477.c_custkey 
x1478    } }
 join1.flatMap(x1472 => join2.get({ val x1475 = x1472.o_custkey 
x1475  }) match {
   case Some(a) => a.map(a1 => (a1, x1472))
   case _ => Nil
 })
} 
val x1491 = { val grps1561 = x1479.groupBy{ case (x1480, x1481) => { val x1482 = (x1480) 
x1482  } }
 grps1561.toList.map(x1489 => (x1489._1, x1489._2.flatMap{ 
   case (x1480, null) =>  Nil
   case (x1480, x1481) => {val x1490 = (x1481) 
x1490 } match {
   case (null) => Nil
   case _ => List({val x1483 = x1481.o_orderdate 
val x1484 = x1480.lbl 
val x1485 = x1484.P__F 
val x1486 = x1484.L__F 
val x1487 = Record1563(x1481, x1485, x1486, newId) 
val x1488 = Record1564(x1483, x1487, newId) 
x1488      })
 }
} ) ) } 
val x1496 = x1491.map{ case (x1492, x1493) => { 
  val x1494 = x1492.lbl 
  val x1495 = RecM_flat2(x1494, x1493, newId) 
  x1495 }} 
val M_flat2 = x1496
val x1497 = M_flat2
val x1499 = M_flat2 
val x1503 = x1499.flatMap{ case x1500 => 
  val x1501 = x1500._2 
  x1501.flatMap(x1502 => {
    List((x1500, x1502))
})} 
val x1508 = x1503.map{ case (x1504, x1505) => { 
  val x1506 = x1505.o_parts 
  val x1507 = RecM_ctx3(x1506, newId) 
  x1507 }} 
val x1509 = x1508.distinct 
val M_ctx3 = x1509
val x1510 = M_ctx3
val x1512 = M_ctx3 
val x1513 = L__D._1 
val x1515 = x1513 
val x1524 = { val hm1566 = x1512.groupBy{case x1516 => { val x1518 = x1516.lbl 
val x1519 = x1518.L__F 
x1519   } }
 val join1 = x1515.flatMap{x1517 => hm1566.get(x1517._1) match {
 case Some(a) => x1517._2
 case _ => Nil
 }}
 val join2 = x1512.groupBy{case x1516 => { val x1521 = x1516.lbl 
val x1522 = x1521.o__F 
val x1523 = x1522.o_orderkey 
x1523    } }
 join1.flatMap(x1517 => join2.get({ val x1520 = x1517.l_orderkey 
x1520  }) match {
   case Some(a) => a.map(a1 => (a1, x1517))
   case _ => Nil
 })
} 
val x1525 = P__D._1 
val x1527 = x1525 
val x1535 = { val hm1568 = x1524.groupBy{case (x1528, x1529) => { val x1531 = x1528.lbl 
val x1532 = x1531.P__F 
x1532   } }
 val join1 = x1527.flatMap{x1530 => hm1568.get(x1530._1) match {
 case Some(a) => x1530._2
 case _ => Nil
 }}
 val join2 = x1524.groupBy{case (x1528, x1529) => { val x1534 = x1529.l_partkey 
x1534  } }
 join1.flatMap(x1530 => join2.get({ val x1533 = x1530.p_partkey 
x1533  }) match {
   case Some(a) => a.map(a1 => (a1, x1530))
   case _ => Nil
 })
} 
val x1545 = { val grps1569 = x1535.groupBy{ case ((x1536, x1537), x1538) => { val x1539 = (x1536) 
x1539  } }
 grps1569.toList.map(x1543 => (x1543._1, x1543._2.flatMap{ 
   case ((x1536, x1537), null) =>  Nil
   case ((x1536, x1537), x1538) => {val x1544 = (x1537,x1538) 
x1544 } match {
   case (null,_) => Nil
   case _ => List({val x1540 = x1538.p_name 
val x1541 = x1537.l_quantity 
val x1542 = Record1571(x1540, x1541, newId) 
x1542   })
 }
} ) ) } 
val x1550 = x1545.map{ case (x1546, x1547) => { 
  val x1548 = x1546.lbl 
  val x1549 = RecM_flat3(x1548, x1547, newId) 
  x1549 }} 
val M_flat3 = x1550
val x1551 = M_flat3
val x1552 = (x1423,x1452,x1465,x1497,x1510,x1551) 
x1552                                  }
    var end0 = System.currentTimeMillis() - start0
    
case class Input_Q1_Dict2(o_parts: (List[RecM_flat3], Unit))
case class Input_Q1_Dict1(c_orders: (List[RecM_flat2], Input_Q1_Dict2))
val Query4__F = ShredQuery4._1.head.lbl
val Query4__D = (ShredQuery4._2, Input_Q1_Dict1((ShredQuery4._4, Input_Q1_Dict2((ShredQuery4._6, Unit)))))
    def f(){
      val x1583 = Q1Flat2(Query4__F, newId) 
val x1584 = Record1659(x1583, newId) 
val x1585 = List(x1584) 
val M_ctx1 = x1585
val x1586 = M_ctx1
val x1588 = M_ctx1 
val x1589 = Query4__D._1 
val x1591 = x1589 
val x1596 = { val hm1663 = x1588.groupBy{case x1592 => { val x1594 = x1592.lbl 
val x1595 = x1594.Query4__F 
x1595   } }
 x1591.flatMap{x1593 => hm1663.get(x1593._1) match {
 case Some(a) => a.map(a1 => (a1, x1593._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
} 
val x1597 = Query4__D._2 
val x1598 = x1597.c_orders 
val x1599 = x1598._1 
val x1601 = x1599 
val x1606 = { val hm1665 = x1596.groupBy{case (x1602, x1603) => { val x1605 = x1603.c_orders 
x1605  } }
 x1601.flatMap{x1604 => hm1665.get(x1604._1) match {
 case Some(a) => a.map(a1 => (a1, x1604._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
} 
val x1607 = x1598._2 
val x1608 = x1607.o_parts 
val x1609 = x1608._1 
val x1611 = x1609 
val x1617 = { val hm1666 = x1606.groupBy{case ((x1612, x1613), x1614) => { val x1616 = x1614.o_parts 
x1616  } }
 x1611.flatMap{x1615 => hm1666.get(x1615._1) match {
 case Some(a) => a.map(a1 => (a1, x1615._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
} 
val x1619 = x1609 
val x1628 = { val hm1667 = x1617.groupBy{case (((x1620, x1621), x1622), x1623) => { val x1625 = x1622.o_parts 
x1625  } }
 val join1 = x1619.flatMap{x1624 => hm1667.get(x1624._1) match {
 case Some(a) => x1624._2
 case _ => Nil
 }}
 val join2 = x1617.groupBy{case (((x1620, x1621), x1622), x1623) => { val x1627 = x1623.p_name 
x1627  } }
 join1.flatMap(x1624 => join2.get({ val x1626 = x1624.p_name 
x1626  }) match {
   case Some(a) => a.map(a1 => (a1, x1624))
   case _ => Nil
 })
} 
val x1638 = { val grps1668 = x1628.groupBy{ case ((((x1629, x1630), x1631), x1632), x1633) => { val x1634 = (x1629,x1630,x1631,x1632) 
x1634  } }
 grps1668.toList.map(x1636 => (x1636._1, x1636._2.foldLeft(0){ 
 case (acc1669, ((((x1629, x1630), x1631), x1632), x1633)) => {val x1637 = (x1633) 
x1637 } match {
   case (null) => acc1669
   case _ => acc1669 + {val x1635 = x1633.l_qty 
x1635 }
 }
} ) ) } 
val x1651 = { val grps1670 = x1638.groupBy{ case ((x1639, x1640, x1641, x1642), x1643) => { val x1644 = (x1639) 
x1644  } }
 grps1670.toList.map(x1649 => (x1649._1, x1649._2.flatMap{ 
   case ((x1639, x1640, x1641, x1642), null) =>  Nil
   case ((x1639, x1640, x1641, x1642), x1643) => {val x1650 = (x1643,x1640,x1641,x1642) 
x1650 } match {
   case (null,null,null,_) => Nil
   case _ => List({val x1645 = x1640.c_name 
val x1646 = x1642.p_name 
val x1647 = x1641.o_orderdate 
val x1648 = Record1672(x1645, x1646, x1647, x1643, newId) 
x1648    })
 }
} ) ) } 
val x1656 = x1651.map{ case (x1652, x1653) => { 
  val x1654 = x1652.lbl 
  val x1655 = Record1673(x1654, x1653, newId) 
  x1655 }} 
val M_flat1 = x1656
val x1657 = M_flat1
val x1658 = (x1586,x1657) 
x1658                       
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
