
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record1541(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record1542(c_name: String, c_orders: Record1541)
case class Record1547(o__F: Orders, P__F: Int, L__F: Int)
case class Record1548(o_orderdate: String, o_parts: Record1547)
case class Record1555(p_name: String, l_qty: Double)
case class Record1641(lbl: Q1Flat2)
case class Input_Query4__DFlat1642(c_name: String, c_orders: Int)
case class Flat1643(o_orderdate: String, o_parts: Int)
case class Dict1643(o_parts: (List[(Int, List[Record1555])], Unit))
case class Input_Query4__DDict1642(c_orders: (List[(Int, List[Flat1643])], Dict1643))
case class Record1654(c_name: String, p_name: String, month: String, t_qty: Double)
case class Record1655(_1: Q1Flat2, _2: List[Record1654])
case class RecM_flat2(_1: Record1541, _2: List[Record1548])
case class RecM_flat3(_1: Record1547, _2: List[Record1555])
case class RecM_flat1(_1: Q1Flat, _2: List[Record1542])
case class RecM_ctx3(lbl: Record1547)
case class RecM_ctx2(lbl: Record1541)
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
    val ShredQuery4 = { val x1407 = Q1Flat(P__F, C__F, L__F, O__F) 
val x1408 = RecM_ctx1(x1407) 
val x1409 = List(x1408) 
val M_ctx1 = x1409
val x1410 = M_ctx1
val x1412 = M_ctx1 
val x1413 = C__D._1 
val x1415 = x1413 
val x1420 = { val hm1538 = x1412.groupBy{case x1416 => { val x1418 = x1416.lbl 
val x1419 = x1418.C__F 
x1419   } }
 x1415.flatMap{x1417 => hm1538.get(x1417._1) match {
 case Some(a) => a.map(a1 => (a1, x1417._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
} 
val x1432 = { val grps1539 = x1420.groupBy{ case (x1421, x1422) => { val x1423 = (x1421) 
x1423  }}
 grps1539.toList.map(x1431 => (x1431._1, x1431._2.map{ case (x1421, x1422) => 
 { val x1424 = x1422.c_name 
val x1425 = x1421.lbl 
val x1426 = x1425.P__F 
val x1427 = x1425.L__F 
val x1428 = x1425.O__F 
val x1429 = Record1541(x1426, x1427, x1428, x1422) 
val x1430 = Record1542(x1424, x1429) 
x1430        } } ) ) } 
val x1437 = x1432.map{ case (x1433, x1434) => { 
  val x1435 = x1433.lbl 
  val x1436 = RecM_flat1(x1435, x1434) 
  x1436 }} 
val M_flat1 = x1437
val x1438 = M_flat1
val x1440 = M_flat1 
val x1444 = x1440.flatMap{ case x1441 => 
  val x1442 = x1441._2 
  x1442.flatMap(x1443 => {
    List((x1441, x1443))
})} 
val x1449 = x1444.map{ case (x1445, x1446) => { 
  val x1447 = x1446.c_orders 
  val x1448 = RecM_ctx2(x1447) 
  x1448 }} 
val x1450 = x1449.distinct 
val M_ctx2 = x1450
val x1451 = M_ctx2
val x1453 = M_ctx2 
val x1454 = O__D._1 
val x1456 = x1454 
val x1465 = { val hm1544 = x1453.groupBy{case x1457 => { val x1459 = x1457.lbl 
val x1460 = x1459.O__F 
x1460   } }
 val join1 = x1456.flatMap{x1458 => hm1544.get(x1458._1) match {
 case Some(a) => x1458._2
 case _ => Nil
 }}
 val join2 = x1453.groupBy{case x1457 => { val x1462 = x1457.lbl 
val x1463 = x1462.c__F 
val x1464 = x1463.c_custkey 
x1464    } }
 join1.flatMap(x1458 => join2.get({ val x1461 = x1458.o_custkey 
x1461  }) match {
   case Some(a) => a.map(a1 => (a1, x1458))
   case _ => Nil
 })
} 
val x1476 = { val grps1545 = x1465.groupBy{ case (x1466, x1467) => { val x1468 = (x1466) 
x1468  }}
 grps1545.toList.map(x1475 => (x1475._1, x1475._2.map{ case (x1466, x1467) => 
 { val x1469 = x1467.o_orderdate 
val x1470 = x1466.lbl 
val x1471 = x1470.P__F 
val x1472 = x1470.L__F 
val x1473 = Record1547(x1467, x1471, x1472) 
val x1474 = Record1548(x1469, x1473) 
x1474       } } ) ) } 
val x1481 = x1476.map{ case (x1477, x1478) => { 
  val x1479 = x1477.lbl 
  val x1480 = RecM_flat2(x1479, x1478) 
  x1480 }} 
val M_flat2 = x1481
val x1482 = M_flat2
val x1484 = M_flat2 
val x1488 = x1484.flatMap{ case x1485 => 
  val x1486 = x1485._2 
  x1486.flatMap(x1487 => {
    List((x1485, x1487))
})} 
val x1493 = x1488.map{ case (x1489, x1490) => { 
  val x1491 = x1490.o_parts 
  val x1492 = RecM_ctx3(x1491) 
  x1492 }} 
val x1494 = x1493.distinct 
val M_ctx3 = x1494
val x1495 = M_ctx3
val x1497 = M_ctx3 
val x1498 = L__D._1 
val x1500 = x1498 
val x1509 = { val hm1550 = x1497.groupBy{case x1501 => { val x1503 = x1501.lbl 
val x1504 = x1503.L__F 
x1504   } }
 val join1 = x1500.flatMap{x1502 => hm1550.get(x1502._1) match {
 case Some(a) => x1502._2
 case _ => Nil
 }}
 val join2 = x1497.groupBy{case x1501 => { val x1506 = x1501.lbl 
val x1507 = x1506.o__F 
val x1508 = x1507.o_orderkey 
x1508    } }
 join1.flatMap(x1502 => join2.get({ val x1505 = x1502.l_orderkey 
x1505  }) match {
   case Some(a) => a.map(a1 => (a1, x1502))
   case _ => Nil
 })
} 
val x1510 = P__D._1 
val x1512 = x1510 
val x1520 = { val hm1552 = x1509.groupBy{case (x1513, x1514) => { val x1516 = x1513.lbl 
val x1517 = x1516.P__F 
x1517   } }
 val join1 = x1512.flatMap{x1515 => hm1552.get(x1515._1) match {
 case Some(a) => x1515._2
 case _ => Nil
 }}
 val join2 = x1509.groupBy{case (x1513, x1514) => { val x1519 = x1514.l_partkey 
x1519  } }
 join1.flatMap(x1515 => join2.get({ val x1518 = x1515.p_partkey 
x1518  }) match {
   case Some(a) => a.map(a1 => (a1, x1515))
   case _ => Nil
 })
} 
val x1529 = { val grps1553 = x1520.groupBy{ case ((x1521, x1522), x1523) => { val x1524 = (x1521) 
x1524  }}
 grps1553.toList.map(x1528 => (x1528._1, x1528._2.map{ case ((x1521, x1522), x1523) => 
 { val x1525 = x1523.p_name 
val x1526 = x1522.l_quantity 
val x1527 = Record1555(x1525, x1526) 
x1527    } } ) ) } 
val x1534 = x1529.map{ case (x1530, x1531) => { 
  val x1532 = x1530.lbl 
  val x1533 = RecM_flat3(x1532, x1531) 
  x1533 }} 
val M_flat3 = x1534
val x1535 = M_flat3
val x1536 = (x1410,x1438,x1451,x1482,x1495,x1535) 
x1536                                  }
    var end0 = System.currentTimeMillis() - start0
    
case class Input_Q1_Dict2(o_parts: (List[RecM_flat3], Unit))
case class Input_Q1_Dict1(c_orders: (List[RecM_flat2], Input_Q1_Dict2))
val Query4__F = ShredQuery4._1.head.lbl
val Query4__D = (ShredQuery4._2, Input_Q1_Dict1((ShredQuery4._4, Input_Q1_Dict2((ShredQuery4._6, Unit)))))
    def f(){
      val x1567 = Q1Flat2(Query4__F) 
val x1568 = Record1641(x1567) 
val x1569 = List(x1568) 
val M_ctx1 = x1569
val x1570 = M_ctx1
val x1572 = M_ctx1 
val x1573 = Query4__D._1 
val x1575 = x1573 
val x1580 = { val hm1645 = x1572.groupBy{case x1576 => { val x1578 = x1576.lbl 
val x1579 = x1578.Query4__F 
x1579   } }
 x1575.flatMap{x1577 => hm1645.get(x1577._1) match {
 case Some(a) => a.map(a1 => (a1, x1577._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
} 
val x1581 = Query4__D._2 
val x1582 = x1581.c_orders 
val x1583 = x1582._1 
val x1585 = x1583 
val x1590 = { val hm1647 = x1580.groupBy{case (x1586, x1587) => { val x1589 = x1587.c_orders 
x1589  } }
 x1585.flatMap{x1588 => hm1647.get(x1588._1) match {
 case Some(a) => a.map(a1 => (a1, x1588._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
} 
val x1591 = x1582._2 
val x1592 = x1591.o_parts 
val x1593 = x1592._1 
val x1595 = x1593 
val x1601 = { val hm1648 = x1590.groupBy{case ((x1596, x1597), x1598) => { val x1600 = x1598.o_parts 
x1600  } }
 x1595.flatMap{x1599 => hm1648.get(x1599._1) match {
 case Some(a) => a.map(a1 => (a1, x1599._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
} 
val x1603 = x1593 
val x1612 = { val hm1649 = x1601.groupBy{case (((x1604, x1605), x1606), x1607) => { val x1609 = x1606.o_parts 
x1609  } }
 val join1 = x1603.flatMap{x1608 => hm1649.get(x1608._1) match {
 case Some(a) => x1608._2
 case _ => Nil
 }}
 val join2 = x1601.groupBy{case (((x1604, x1605), x1606), x1607) => { val x1611 = x1607.p_name 
x1611  } }
 join1.flatMap(x1608 => join2.get({ val x1610 = x1608.p_name 
x1610  }) match {
   case Some(a) => a.map(a1 => (a1, x1608))
   case _ => Nil
 })
} 
val x1621 = { val grps1650 = x1612.groupBy{ case ((((x1613, x1614), x1615), x1616), x1617) => { val x1618 = (x1613,x1614,x1615,x1616) 
x1618  }}
 grps1650.toList.map(x1620 => (x1620._1, x1620._2.foldLeft(0){ case (acc1651, ((((x1613, x1614), x1615), x1616), x1617)) => 
 if (((((x1613, x1614), x1615), x1616), x1617)._2.equals(None)) { 0 } else { acc1651 + {val x1619 = x1617.l_qty 
x1619 } } } ) ) } 
val x1633 = { val grps1652 = x1621.groupBy{ case ((x1622, x1623, x1624, x1625), x1626) => { val x1627 = (x1622) 
x1627  }}
 grps1652.toList.map(x1632 => (x1632._1, x1632._2.map{ case ((x1622, x1623, x1624, x1625), x1626) => 
 { val x1628 = x1623.c_name 
val x1629 = x1625.p_name 
val x1630 = x1624.o_orderdate 
val x1631 = Record1654(x1628, x1629, x1630, x1626) 
x1631     } } ) ) } 
val x1638 = x1633.map{ case (x1634, x1635) => { 
  val x1636 = x1634.lbl 
  val x1637 = Record1655(x1636, x1635) 
  x1637 }} 
val M_flat1 = x1638
val x1639 = M_flat1
val x1640 = (x1570,x1639) 
x1640                       
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
