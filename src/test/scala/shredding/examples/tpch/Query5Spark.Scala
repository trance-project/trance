
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
case class Record924(s_name: String, s_nationkey: Int, uniqueId: Long)
case class Record926(c_name: String, c_nationkey: Int, uniqueId: Long)
case class Record968(p_name: String, cnt: Int, uniqueId: Long)
case class Query3Out(p_name: String, suppliers: List[Record924], customers: List[Record926], uniqueId: Long)
object Query5Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("Query5Spark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   
val tpch = TPCHLoader(spark)
val C = tpch.loadCustomers
C.cache
C.count
val O = tpch.loadOrders
O.cache
O.count
val L = tpch.loadLineitem
L.cache
L.count
val P = tpch.loadPart
P.cache
P.count
val PS = tpch.loadPartSupp
PS.cache
PS.count
val S = tpch.loadSupplier
S.cache
S.count
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
   val Query5 = { val x852 = P 
val x854 = PS 
val x859 = { val out1 = x852.map{ case x855 => ({val x857 = x855.p_partkey 
x857}, x855) }
  val out2 = x854.map{ case x856 => ({val x858 = x856.ps_partkey 
x858}, x856) }
  out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x861 = S 
val x867 = { val out1 = x859.map{ case (x862, x863) => ({val x865 = x863.ps_suppkey 
x865}, (x862, x863)) }
  val out2 = x861.map{ case x864 => ({val x866 = x864.s_suppkey 
x866}, x864) }
  out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x877 = x867.map{ case ((x868, x869), x870) => val x876 = (x869,x870) 
x876 match {
   case (_,null) => ({val x871 = (x868) 
x871}, Nil) => ({val x871 = (x868) 
x871}, Nil) 
   case x875 => ({val x871 = (x868) 
x871}, List({val x872 = x870.s_name 
val x873 = x870.s_nationkey 
val x874 = Record924(x872, x873, newId) 
x874}))
 }
}.foldByKey(Nil){ case (acc923, x875) => acc923 ++ x875 } 
val x879 = L 
val x885 = { val out1 = x877.map{ case (x880, x881) => ({val x883 = x880.p_partkey 
x883}, (x880, x881)) }
  val out2 = x879.map{ case x882 => ({val x884 = x882.l_partkey 
x884}, x882) }
  out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x887 = O 
val x894 = { val out1 = x885.map{ case ((x888, x889), x890) => ({val x892 = x890.l_orderkey 
x892}, ((x888, x889), x890)) }
  val out2 = x887.map{ case x891 => ({val x893 = x891.o_orderkey 
x893}, x891) }
  out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x896 = C 
val x904 = { val out1 = x894.map{ case (((x897, x898), x899), x900) => ({val x902 = x900.o_custkey 
x902}, (((x897, x898), x899), x900)) }
  val out2 = x896.map{ case x901 => ({val x903 = x901.c_custkey 
x903}, x901) }
  out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x916 = x904.map{ case ((((x905, x906), x907), x908), x909) => val x915 = (x907,x908,x909) 
x915 match {
   case (_,null,_,_) => ({val x910 = (x905,x906) 
x910}, Nil)
case (_,_,null) => ({val x910 = (x905,x906) 
x910}, Nil) => ({val x910 = (x905,x906) 
x910}, Nil) 
   case x914 => ({val x910 = (x905,x906) 
x910}, List({val x911 = x909.c_name 
val x912 = x909.c_nationkey 
val x913 = Record926(x911, x912, newId) 
x913}))
 }
}.foldByKey(Nil){ case (acc925, x914) => acc925 ++ x914 } 
val x922 = x916.map{ case ((x917, x918), x919) => 
   val x920 = x917.p_name 
val x921 = Query3Out(x920, x918, x919, newId) 
x921 
} 
x922 } 
 Query5.cache 
 Query5.count
   var start0 = System.currentTimeMillis()
   def f() {
     val x933 = Query5 
val x937 = x933.flatMap{ case x934 => x934 match {
   case null => List((x934, null))
   case _ => 
   {val x935 = x934.customers 
x935} match {
     case Nil => List((x934, null))
     case lst => lst.map{ case x936 => (x934, x936) }
  }
 }} 
val x945 = x937.flatMap{ case (x938, x939) => (x938, x939) match {
   case (_, null) => List(((x938, x939), null))
   case _ => 
   {val x940 = x938.suppliers 
x940} match {
     case Nil => List(((x938, x939), null))
     case lst => lst.map{ case x941 => if ({val x942 = x939.c_nationkey 
val x943 = x941.s_nationkey 
val x944 = x942 == x943 
x944}) { ((x938, x939), x941) } else { ((x938, x939), null) } }
  }
 }} 
val x952 = x945.map{ case ((x946, x947), x948) => val x951 = (x948) 
x951 match {
   case (null) => ({val x949 = (x946,x947) 
x949}, 0) => ({val x949 = (x946,x947) 
x949}, 0)
   case x950 => ({val x949 = (x946,x947) 
x949}, {1})
 }
}.foldByKey(0){ case (acc966, x950) => acc966 + x950 } 
val x960 = x952.map{ case ((x953, x954), x955) => val x959 = (x954) 
x959 match {
   case x957 if {val x958 = x955 == 0 
x958} => ({val x956 = (x953) 
x956}, {1})
   case x957 => ({val x956 = (x953) 
x956}, 0)
 }
}.foldByKey(0){ case (acc967, x957) => acc967 + x957 } 
val x965 = x960.map{ case (x961, x962) => 
   val x963 = x961.p_name 
val x964 = Record968(x963, x962, newId) 
x964 
} 
x965.count
   }
   f
   var end0 = System.currentTimeMillis() - start0
   println("Query5Spark"+sf+","+Config.datapath+","+end0)
 }
}
