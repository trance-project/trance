
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
case class Record1916(p__F: Part, S__F: Int, PS__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record1917(p__F: Part, C__F: Int, L__F: Int, O__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record1918(p_name: String, suppliers: Record1916, customers: Record1917, uniqueId: Long) extends CaseClassRecord
case class Record1928(s_name: String, s_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1941(c_name: String, c_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1999(Query5__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record2000(lbl: Record1999, uniqueId: Long) extends CaseClassRecord
case class Input_Query5__DFlat2002(p_name: String, suppliers: Int, customers: Int, uniqueId: Long) extends CaseClassRecord
case class Input_Query5__DDict2002(suppliers: (List[(Int, List[Record1928])], Unit), customers: (List[(Int, List[Record1941])], Unit), uniqueId: Long) extends CaseClassRecord
case class Record2013(p_name: String, cnt: Int, uniqueId: Long) extends CaseClassRecord
case class Record2014(_1: Record1999, _2: List[Record2013], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx1(lbl: Q3Flat, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx3(lbl: Record1917, uniqueId: Long) extends CaseClassRecord
case class RecM_flat1(_1: Q3Flat, _2: List[Record1918], uniqueId: Long) extends CaseClassRecord
case class RecM_flat3(_1: Record1917, _2: List[Record1941], uniqueId: Long) extends CaseClassRecord
case class RecM_flat2(_1: Record1916, _2: List[Record1928], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx2(lbl: Record1916, uniqueId: Long) extends CaseClassRecord
object ShredQuery5Calc {
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
val PS__F = 5
val PS__D = (List((PS__F, TPCHLoader.loadPartSupp[PartSupp].toList)), ())
val S__F = 6
val S__D = (List((S__F, TPCHLoader.loadSupplier[Supplier].toList)), ())
    val ShredQuery5 = { val x1771 = Q3Flat(O__F, C__F, PS__F, S__F, L__F, P__F, newId) 
val x1772 = RecM_ctx1(x1771, newId) 
val x1773 = List(x1772) 
val M_ctx1 = x1773
val x1774 = M_ctx1
val x1800 = M_ctx1.flatMap(x1775 => { 
    val x1776 = x1775.lbl 
    val x1777 = P__D._1 
    val x1797 = x1777.flatMap(x1778 => { 
      if({val x1779 = x1775.lbl 
      val x1780 = x1779.P__F 
      val x1781 = x1778._1 
      val x1782 = x1780 == x1781 
      x1782    }) {  val x1783 = x1778._2 
        val x1796 = x1783.flatMap(x1784 => { 
            val x1785 = x1784.p_name 
            val x1786 = x1775.lbl 
            val x1787 = x1786.S__F 
            val x1788 = x1786.PS__F 
            val x1789 = Record1916(x1784, x1787, x1788, newId) 
            val x1790 = x1786.C__F 
            val x1791 = x1786.L__F 
            val x1792 = x1786.O__F 
            val x1793 = Record1917(x1784, x1790, x1791, x1792, newId) 
            val x1794 = Record1918(x1785, x1789, x1793, newId) 
            val x1795 = List(x1794) 
            x1795}) 
        x1796} else {  Nil}}) 
    val x1798 = RecM_flat1(x1776, x1797, newId) 
    val x1799 = List(x1798) 
    x1799}) 
val M_flat1 = x1800
val x1801 = M_flat1
val x1809 = M_flat1.flatMap(x1802 => { 
    val x1803 = x1802._2 
    val x1808 = x1803.flatMap(x1804 => { 
        val x1805 = x1804.suppliers 
        val x1806 = RecM_ctx2(x1805, newId) 
        val x1807 = List(x1806) 
        x1807}) 
    x1808}) 
val x1810 = x1809.distinct 
val M_ctx2 = x1810
val x1811 = M_ctx2
val x1848 = M_ctx2.flatMap(x1812 => { 
    val x1813 = x1812.lbl 
    val x1814 = PS__D._1 
    val x1845 = x1814.flatMap(x1815 => { 
      if({val x1816 = x1812.lbl 
      val x1817 = x1816.PS__F 
      val x1818 = x1815._1 
      val x1819 = x1817 == x1818 
      x1819    }) {  val x1820 = x1815._2 
        val x1844 = x1820.flatMap(x1821 => { 
          if({val x1822 = x1821.ps_partkey 
          val x1823 = x1812.lbl 
          val x1824 = x1823.p__F 
          val x1825 = x1824.p_partkey 
          val x1826 = x1822 == x1825 
          x1826     }) {  val x1827 = S__D._1 
            val x1843 = x1827.flatMap(x1828 => { 
              if({val x1829 = x1812.lbl 
              val x1830 = x1829.S__F 
              val x1831 = x1828._1 
              val x1832 = x1830 == x1831 
              x1832    }) {  val x1833 = x1828._2 
                val x1842 = x1833.flatMap(x1834 => { 
                  if({val x1835 = x1834.s_suppkey 
                  val x1836 = x1821.ps_suppkey 
                  val x1837 = x1835 == x1836 
                  x1837   }) {  val x1838 = x1834.s_name 
                    val x1839 = x1834.s_nationkey 
                    val x1840 = Record1928(x1838, x1839, newId) 
                    val x1841 = List(x1840) 
                    x1841} else {  Nil}}) 
                x1842} else {  Nil}}) 
            x1843} else {  Nil}}) 
        x1844} else {  Nil}}) 
    val x1846 = RecM_flat2(x1813, x1845, newId) 
    val x1847 = List(x1846) 
    x1847}) 
val M_flat2 = x1848
val x1849 = M_flat2
val x1857 = M_flat1.flatMap(x1850 => { 
    val x1851 = x1850._2 
    val x1856 = x1851.flatMap(x1852 => { 
        val x1853 = x1852.customers 
        val x1854 = RecM_ctx3(x1853, newId) 
        val x1855 = List(x1854) 
        x1855}) 
    x1856}) 
val x1858 = x1857.distinct 
val M_ctx3 = x1858
val x1859 = M_ctx3
val x1909 = M_ctx3.flatMap(x1860 => { 
    val x1861 = x1860.lbl 
    val x1862 = L__D._1 
    val x1906 = x1862.flatMap(x1863 => { 
      if({val x1864 = x1860.lbl 
      val x1865 = x1864.L__F 
      val x1866 = x1863._1 
      val x1867 = x1865 == x1866 
      x1867    }) {  val x1868 = x1863._2 
        val x1905 = x1868.flatMap(x1869 => { 
          if({val x1870 = x1869.l_partkey 
          val x1871 = x1860.lbl 
          val x1872 = x1871.p__F 
          val x1873 = x1872.p_partkey 
          val x1874 = x1870 == x1873 
          x1874     }) {  val x1875 = O__D._1 
            val x1904 = x1875.flatMap(x1876 => { 
              if({val x1877 = x1860.lbl 
              val x1878 = x1877.O__F 
              val x1879 = x1876._1 
              val x1880 = x1878 == x1879 
              x1880    }) {  val x1881 = x1876._2 
                val x1903 = x1881.flatMap(x1882 => { 
                  if({val x1883 = x1882.o_orderkey 
                  val x1884 = x1869.l_orderkey 
                  val x1885 = x1883 == x1884 
                  x1885   }) {  val x1886 = C__D._1 
                    val x1902 = x1886.flatMap(x1887 => { 
                      if({val x1888 = x1860.lbl 
                      val x1889 = x1888.C__F 
                      val x1890 = x1887._1 
                      val x1891 = x1889 == x1890 
                      x1891    }) {  val x1892 = x1887._2 
                        val x1901 = x1892.flatMap(x1893 => { 
                          if({val x1894 = x1893.c_custkey 
                          val x1895 = x1882.o_custkey 
                          val x1896 = x1894 == x1895 
                          x1896   }) {  val x1897 = x1893.c_name 
                            val x1898 = x1893.c_nationkey 
                            val x1899 = Record1941(x1897, x1898, newId) 
                            val x1900 = List(x1899) 
                            x1900} else {  Nil}}) 
                        x1901} else {  Nil}}) 
                    x1902} else {  Nil}}) 
                x1903} else {  Nil}}) 
            x1904} else {  Nil}}) 
        x1905} else {  Nil}}) 
    val x1907 = RecM_flat3(x1861, x1906, newId) 
    val x1908 = List(x1907) 
    x1908}) 
val M_flat3 = x1909
val x1910 = M_flat3
val x1911 = (x1774,x1801,x1811,x1849,x1859,x1910) 
x1911            }
    var end0 = System.currentTimeMillis() - start0
    
case class Input_Q3_Dict1(suppliers: (List[RecM_flat2], Unit), customers: (List[RecM_flat3], Unit))
val Query5__F = ShredQuery5._1.head.lbl
val Query5__D = (ShredQuery5._2, Input_Q3_Dict1((ShredQuery5._4, Unit), (ShredQuery5._6, Unit)))
    def f(){
      val x1949 = Record1999(Query5__F, newId) 
val x1950 = Record2000(x1949, newId) 
val x1951 = List(x1950) 
val M_ctx1 = x1951
val x1952 = M_ctx1
val x1996 = M_ctx1.flatMap(x1953 => { 
    val x1954 = x1953.lbl 
    val x1955 = Query5__D._1 
    val x1993 = x1955.flatMap(x1956 => { 
      if({val x1957 = x1953.lbl 
      val x1958 = x1957.Query5__F 
      val x1959 = x1956._1 
      val x1960 = x1958 == x1959 
      x1960    }) {  val x1961 = x1956._2 
        val x1992 = x1961.flatMap(x1962 => { 
            val x1963 = x1962.p_name 
            val x1964 = Query5__D._2 
            val x1965 = x1964.customers 
            val x1966 = x1965._1 
            val x1989 = x1966.foldLeft(0)((acc2008, x1967) => 
              if({val x1968 = x1962.customers 
              val x1969 = x1967._1 
              val x1970 = x1968 == x1969 
              x1970   }) {  acc2008 + {val x1971 = x1967._2 
                val x1988 = x1971.foldLeft(0)((acc2009, x1972) => 
                  if({val x1973 = Query5__D._2 
                  val x1974 = x1973.suppliers 
                  val x1975 = x1974._1 
                  val x1986 = x1975.foldLeft(0)((acc2011, x1976) => 
                    if({val x1977 = x1962.suppliers 
                    val x1978 = x1976._1 
                    val x1979 = x1977 == x1978 
                    x1979   }) {  acc2011 + {val x1980 = x1976._2 
                      val x1985 = x1980.foldLeft(0)((acc2012, x1981) => 
                        if({val x1982 = x1972.c_nationkey 
                        val x1983 = x1981.s_nationkey 
                        val x1984 = x1982 == x1983 
                        x1984   }) {  acc2012 + {1}} else {  acc2012}) 
                      x1985  }} else {  acc2011}) 
                  val x1987 = x1986 == 0 
                  x1987     }) {  acc2009 + {1}} else {  acc2009}) 
                x1988  }} else {  acc2008}) 
            val x1990 = Record2013(x1963, x1989, newId) 
            val x1991 = List(x1990) 
            x1991}) 
        x1992} else {  Nil}}) 
    val x1994 = Record2014(x1954, x1993, newId) 
    val x1995 = List(x1994) 
    x1995}) 
val M_flat1 = x1996
val x1997 = M_flat1
val x1998 = (x1952,x1997) 
x1998     
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
