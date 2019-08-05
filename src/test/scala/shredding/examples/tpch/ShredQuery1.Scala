
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
    case class Record1730(P__F: Int, L__F: Int, O__F: Int, c__F: Customer, uniqueId: Long) extends CaseClassRecord
case class Record1731(c_name: String, c_orders: Record1730, uniqueId: Long) extends CaseClassRecord
case class Record1736(o__F: Orders, P__F: Int, L__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record1737(o_orderdate: String, o_parts: Record1736, uniqueId: Long) extends CaseClassRecord
case class Record1744(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class RecM_flat2(_1: Record1730, _2: List[Record1737], uniqueId: Long) extends CaseClassRecord
case class RecM_flat3(_1: Record1736, _2: List[Record1744], uniqueId: Long) extends CaseClassRecord
case class RecM_flat1(_1: Q1Flat, _2: List[Record1731], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx3(lbl: Record1736, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx2(lbl: Record1730, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx1(lbl: Q1Flat, uniqueId: Long) extends CaseClassRecord
object ShredQuery1 {
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
    var end0 = System.currentTimeMillis() - start0
    def f(){
      val x1601 = Q1Flat(P__F, C__F, L__F, O__F, newId) 
val x1602 = RecM_ctx1(x1601, newId) 
val x1603 = List(x1602) 
val M_ctx1 = x1603
val x1604 = M_ctx1
val x1606 = M_ctx1 
val x1607 = C__D._1 
val x1609 = x1607 
val x1612 = x1609.flatMap{ x1611 => 
 x1606.map{ x1610 => (x1610, x1611) }
} 
val x1625 = { val grps1728 = x1612.groupBy{ case (x1613, x1614) => { val x1615 = (x1613) 
x1615  } }
 grps1728.toList.map(x1623 => (x1623._1, x1623._2.flatMap{ 
   case (x1613, null) =>  Nil
   case (x1613, x1614) => {val x1624 = (x1614) 
x1624 } match {
   case (null) => Nil
   case (x1614) => List({val x1616 = x1614.c_name 
val x1617 = x1613.lbl 
val x1618 = x1617.P__F 
val x1619 = x1617.L__F 
val x1620 = x1617.O__F 
val x1621 = Record1730(x1618, x1619, x1620, x1614, newId) 
val x1622 = Record1731(x1616, x1621, newId) 
x1622       })
 }
} ) ) } 
val x1630 = x1625.map{ case (x1626, x1627) => { 
  val x1628 = x1626.lbl 
  val x1629 = RecM_flat1(x1628, x1627, newId) 
  x1629 }} 
val M_flat1 = x1630
val x1631 = M_flat1
val x1633 = M_flat1 
val x1637 = x1633.flatMap{ case x1634 => 
  val x1635 = x1634._2 
  x1635.flatMap(x1636 => {
    List((x1634, x1636))
})} 
val x1642 = x1637.map{ case (x1638, x1639) => { 
  val x1640 = x1639.c_orders 
  val x1641 = RecM_ctx2(x1640, newId) 
  x1641 }} 
val x1643 = x1642.distinct 
val M_ctx2 = x1643
val x1644 = M_ctx2
val x1646 = M_ctx2 
val x1647 = O__D._1 
val x1649 = x1647 
val x1656 = { val hm1733 = x1646.groupBy{ case x1650 => {val x1652 = x1650.lbl 
val x1653 = x1652.c__F 
val x1654 = x1653.c_custkey 
x1654   } }
x1649.flatMap(x1651 => hm1733.get({val x1655 = x1651.o_custkey 
x1655 }) match {
 case Some(a) => a.map(v => (v, x1651))
 case _ => Nil
}) } 
val x1668 = { val grps1734 = x1656.groupBy{ case (x1657, x1658) => { val x1659 = (x1657) 
x1659  } }
 grps1734.toList.map(x1666 => (x1666._1, x1666._2.flatMap{ 
   case (x1657, null) =>  Nil
   case (x1657, x1658) => {val x1667 = (x1658) 
x1667 } match {
   case (null) => Nil
   case (x1658) => List({val x1660 = x1658.o_orderdate 
val x1661 = x1657.lbl 
val x1662 = x1661.P__F 
val x1663 = x1661.L__F 
val x1664 = Record1736(x1658, x1662, x1663, newId) 
val x1665 = Record1737(x1660, x1664, newId) 
x1665      })
 }
} ) ) } 
val x1673 = x1668.map{ case (x1669, x1670) => { 
  val x1671 = x1669.lbl 
  val x1672 = RecM_flat2(x1671, x1670, newId) 
  x1672 }} 
val M_flat2 = x1673
val x1674 = M_flat2
val x1676 = M_flat2 
val x1680 = x1676.flatMap{ case x1677 => 
  val x1678 = x1677._2 
  x1678.flatMap(x1679 => {
    List((x1677, x1679))
})} 
val x1685 = x1680.map{ case (x1681, x1682) => { 
  val x1683 = x1682.o_parts 
  val x1684 = RecM_ctx3(x1683, newId) 
  x1684 }} 
val x1686 = x1685.distinct 
val M_ctx3 = x1686
val x1687 = M_ctx3
val x1689 = M_ctx3 
val x1690 = L__D._1 
val x1692 = x1690 
val x1699 = { val hm1739 = x1689.groupBy{ case x1693 => {val x1695 = x1693.lbl 
val x1696 = x1695.o__F 
val x1697 = x1696.o_orderkey 
x1697   } }
x1692.flatMap(x1694 => hm1739.get({val x1698 = x1694.l_orderkey 
x1698 }) match {
 case Some(a) => a.map(v => (v, x1694))
 case _ => Nil
}) } 
val x1700 = P__D._1 
val x1702 = x1700 
val x1708 = { val hm1741 = x1699.groupBy{ case (x1703, x1704) => {val x1706 = x1704.l_partkey 
x1706 } }
x1702.flatMap(x1705 => hm1741.get({val x1707 = x1705.p_partkey 
x1707 }) match {
 case Some(a) => a.map(v => (v, x1705))
 case _ => Nil
}) } 
val x1718 = { val grps1742 = x1708.groupBy{ case ((x1709, x1710), x1711) => { val x1712 = (x1709) 
x1712  } }
 grps1742.toList.map(x1716 => (x1716._1, x1716._2.flatMap{ 
   case ((x1709, x1710), null) =>  Nil
   case ((x1709, x1710), x1711) => {val x1717 = (x1710,x1711) 
x1717 } match {
   case (null,_) => Nil
   case (x1710,x1711) => List({val x1713 = x1711.p_name 
val x1714 = x1710.l_quantity 
val x1715 = Record1744(x1713, x1714, newId) 
x1715   })
 }
} ) ) } 
val x1723 = x1718.map{ case (x1719, x1720) => { 
  val x1721 = x1719.lbl 
  val x1722 = RecM_flat3(x1721, x1720, newId) 
  x1722 }} 
val M_flat3 = x1723
val x1724 = M_flat3
val x1725 = (x1604,x1631,x1644,x1674,x1687,x1724) 
x1725                                 
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
