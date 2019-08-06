
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
case class Record2101(s_name: String, s_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record2107(c_name: String, c_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record2151(p_name: String, cnt: Int, uniqueId: Long) extends CaseClassRecord
case class Query3Out(p_name: String, suppliers: List[Record2101], customers: List[Record2107], uniqueId: Long) extends CaseClassRecord
object Query5 {
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
    val Query5 = { val x2026 = P 
val x2028 = PS 
val x2033 = { val hm2097 = x2026.groupBy{ case x2029 => {val x2031 = x2029.p_partkey 
x2031 } }
x2028.flatMap(x2030 => hm2097.get({val x2032 = x2030.ps_partkey 
x2032 }) match {
 case Some(a) => a.map(v => (v, x2030))
 case _ => Nil
}) } 
val x2035 = S 
val x2041 = { val hm2098 = x2033.groupBy{ case (x2036, x2037) => {val x2039 = x2037.ps_suppkey 
x2039 } }
x2035.flatMap(x2038 => hm2098.get({val x2040 = x2038.s_suppkey 
x2040 }) match {
 case Some(a) => a.map(v => (v, x2038))
 case _ => Nil
}) } 
val x2051 = { val grps2099 = x2041.groupBy{ case ((x2042, x2043), x2044) => { val x2045 = (x2042) 
x2045  } }
 grps2099.toList.map(x2049 => (x2049._1, x2049._2.flatMap{ 
   case ((x2042, x2043), null) =>  Nil
   case ((x2042, x2043), x2044) => {val x2050 = (x2043,x2044) 
x2050 } match {
   case (null,_) => Nil
   case (x2043,x2044) => List({val x2046 = x2044.s_name 
val x2047 = x2044.s_nationkey 
val x2048 = Record2101(x2046, x2047, newId) 
x2048   })
 }
} ) ) } 
val x2053 = L 
val x2059 = { val hm2102 = x2051.groupBy{ case (x2054, x2055) => {val x2057 = x2054.p_partkey 
x2057 } }
x2053.flatMap(x2056 => hm2102.get({val x2058 = x2056.l_partkey 
x2058 }) match {
 case Some(a) => a.map(v => (v, x2056))
 case _ => Nil
}) } 
val x2061 = O 
val x2068 = { val hm2103 = x2059.groupBy{ case ((x2062, x2063), x2064) => {val x2066 = x2064.l_orderkey 
x2066 } }
x2061.flatMap(x2065 => hm2103.get({val x2067 = x2065.o_orderkey 
x2067 }) match {
 case Some(a) => a.map(v => (v, x2065))
 case _ => Nil
}) } 
val x2070 = C 
val x2078 = { val hm2104 = x2068.groupBy{ case (((x2071, x2072), x2073), x2074) => {val x2076 = x2074.o_custkey 
x2076 } }
x2070.flatMap(x2075 => hm2104.get({val x2077 = x2075.c_custkey 
x2077 }) match {
 case Some(a) => a.map(v => (v, x2075))
 case _ => Nil
}) } 
val x2090 = { val grps2105 = x2078.groupBy{ case ((((x2079, x2080), x2081), x2082), x2083) => { val x2084 = (x2079,x2080) 
x2084  } }
 grps2105.toList.map(x2088 => (x2088._1, x2088._2.flatMap{ 
   case ((((x2079, x2080), x2081), x2082), null) =>  Nil
   case ((((x2079, x2080), x2081), x2082), x2083) => {val x2089 = (x2082,x2081,x2083) 
x2089 } match {
   case (null,null,_) => Nil
   case (x2082,x2081,x2083) => List({val x2085 = x2083.c_name 
val x2086 = x2083.c_nationkey 
val x2087 = Record2107(x2085, x2086, newId) 
x2087   })
 }
} ) ) } 
val x2096 = x2090.map{ case ((x2091, x2092), x2093) => { 
  val x2094 = x2091.p_name 
  val x2095 = Query3Out(x2094, x2092, x2093, newId) 
  x2095 }} 
x2096               }
    var end0 = System.currentTimeMillis() - start0
    
    def f(){
      val x2114 = Query5 
val x2118 = x2114.flatMap{ case x2115 => 
  val x2116 = x2115.customers 
  x2116.flatMap(x2117 => {
    List((x2115, x2117))
})} 
val x2126 = x2118.flatMap{ case (x2119, x2120) => 
  val x2121 = x2119.suppliers 
  x2121.flatMap(x2122 => {
  if({val x2123 = x2120.c_nationkey 
  val x2124 = x2122.s_nationkey 
  val x2125 = x2123 == x2124 
  x2125   }) {  List(((x2119, x2120), x2122))} else {  List(((x2119, x2120), null))}
})} 
val x2133 = { val grps2147 = x2126.groupBy{ case ((x2127, x2128), x2129) => { val x2130 = (x2127,x2128) 
x2130  } }
 grps2147.toList.map(x2131 => (x2131._1, x2131._2.foldLeft(0){ 
 case (acc2148, ((x2127, x2128), x2129)) => {val x2132 = (x2129) 
x2132 } match {
   case (null) => acc2148
   case _ => acc2148 + {1}
 }
} ) ) } 
val x2141 = { val grps2149 = x2133.groupBy{ case ((x2134, x2135), x2136) => { val x2137 = (x2134) 
x2137  } }
 grps2149.toList.map(x2138 => (x2138._1, x2138._2.foldLeft(0){ 
 case (acc2150, ((x2134, x2135), x2136)) => {val x2140 = (x2135) 
x2140 } match {
   case (null) => acc2150
   case _ => acc2150 + {1}
 }
} ) ) } 
val x2146 = x2141.map{ case (x2142, x2143) => { 
  val x2144 = x2142.p_name 
  val x2145 = Record2151(x2144, x2143, newId) 
  x2145 }} 
x2146.foreach(println(_)) 
    }
    var time = List[Long]()
    for (i <- 1 to 1) {
     var start = System.currentTimeMillis()
      f
      var end = System.currentTimeMillis() - start
      time = time :+ end
    }
    val avg = (time.sum/5)
    println(end0+","+avg)
 }
}
