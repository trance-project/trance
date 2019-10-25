
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
case class Record1723(s_name: String, s_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1727(c_name: String, c_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1749(p_name: String, cnt: Int, uniqueId: Long) extends CaseClassRecord
case class Query3Out(p_name: String, suppliers: List[Record1723], customers: List[Record1727], uniqueId: Long) extends CaseClassRecord
object Query5Calc {
 def main(args: Array[String]){
    var start0 = System.currentTimeMillis()
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
val PS = TPCHLoader.loadPartSupp[PartSupp].toList
val S = TPCHLoader.loadSupplier[Supplier].toList
    val Query5 = { val x1719 = P.flatMap(x1682 => { 
    val x1683 = x1682.p_name 
    val x1697 = PS.flatMap(x1684 => { 
      if({val x1685 = x1684.ps_partkey 
      val x1686 = x1682.p_partkey 
      val x1687 = x1685 == x1686 
      x1687   }) {  val x1696 = S.flatMap(x1688 => { 
          if({val x1689 = x1688.s_suppkey 
          val x1690 = x1684.ps_suppkey 
          val x1691 = x1689 == x1690 
          x1691   }) {  val x1692 = x1688.s_name 
            val x1693 = x1688.s_nationkey 
            val x1694 = Record1723(x1692, x1693, newId) 
            val x1695 = List(x1694) 
            x1695} else {  Nil}}) 
        x1696} else {  Nil}}) 
    val x1716 = L.flatMap(x1698 => { 
      if({val x1699 = x1698.l_partkey 
      val x1700 = x1682.p_partkey 
      val x1701 = x1699 == x1700 
      x1701   }) {  val x1715 = O.flatMap(x1702 => { 
          if({val x1703 = x1702.o_orderkey 
          val x1704 = x1698.l_orderkey 
          val x1705 = x1703 == x1704 
          x1705   }) {  val x1714 = C.flatMap(x1706 => { 
              if({val x1707 = x1706.c_custkey 
              val x1708 = x1702.o_custkey 
              val x1709 = x1707 == x1708 
              x1709   }) {  val x1710 = x1706.c_name 
                val x1711 = x1706.c_nationkey 
                val x1712 = Record1727(x1710, x1711, newId) 
                val x1713 = List(x1712) 
                x1713} else {  Nil}}) 
            x1714} else {  Nil}}) 
        x1715} else {  Nil}}) 
    val x1717 = Query3Out(x1683, x1697, x1716, newId) 
    val x1718 = List(x1717) 
    x1718}) 
x1719  }
    var end0 = System.currentTimeMillis() - start0
    
    def f(){
      val x1745 = Query5.flatMap(x1731 => { 
    val x1732 = x1731.p_name 
    val x1733 = x1731.customers 
    val x1742 = x1733.foldLeft(0)((acc1747, x1734) => 
      if({val x1735 = x1731.suppliers 
      val x1740 = x1735.foldLeft(0)((acc1748, x1736) => 
        if({val x1737 = x1734.c_nationkey 
        val x1738 = x1736.s_nationkey 
        val x1739 = x1737 == x1738 
        x1739   }) {  acc1748 + {1}} else {  acc1748}) 
      val x1741 = x1740 == 0 
      x1741   }) {  acc1747 + {1}} else {  acc1747}) 
    val x1743 = Record1749(x1732, x1742, newId) 
    val x1744 = List(x1743) 
    x1744}) 
x1745 
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
