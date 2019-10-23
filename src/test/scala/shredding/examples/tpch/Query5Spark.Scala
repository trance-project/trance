
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record1016(p_name: String, p_partkey: Int)
case class Record1017(ps_suppkey: Int, ps_partkey: Int)
case class Record1018(s_name: String, s_nationkey: Int, s_suppkey: Int)
case class Record1020(s_name: String, s_nationkey: Int)
case class Record1021(l_orderkey: Int, l_partkey: Int)
case class Record1022(o_custkey: Int, o_orderkey: Int)
case class Record1023(c_name: String, c_nationkey: Int, c_custkey: Int)
case class Record1025(c_name: String, c_nationkey: Int)
case class Record1068(p_name: String, cnt: Int)
case class Query3Out(p_name: String, suppliers: Iterable[Record1020], customers: Iterable[Record1025])
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
   val Query5 = { val x928 = P.map(x924 => { val x925 = x924.p_name 
val x926 = x924.p_partkey 
val x927 = Record1016(x925, x926) 
x927 }) 
val x933 = PS.map(x929 => { val x930 = x929.ps_suppkey 
val x931 = x929.ps_partkey 
val x932 = Record1017(x930, x931) 
x932 }) 
val x938 = { val out1 = x928.map{ case x934 => ({val x936 = x934.p_partkey 
x936}, x934) }
  val out2 = x933.map{ case x935 => ({val x937 = x935.ps_partkey 
x937}, x935) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x944 = S.map(x939 => { val x940 = x939.s_name 
val x941 = x939.s_nationkey 
val x942 = x939.s_suppkey 
val x943 = Record1018(x940, x941, x942) 
x943 }) 
val x950 = { val out1 = x938.map{ case (x945, x946) => ({val x948 = x946.ps_suppkey 
x948}, (x945, x946)) }
  val out2 = x944.map{ case x947 => ({val x949 = x947.s_suppkey 
x949}, x947) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x960 = x950.flatMap{ case ((x951, x952), x953) => val x959 = (x952,x953) 
x959 match {
   case (_,null) => Nil 
   case x958 => List(({val x954 = (x951) 
x954}, {val x955 = x953.s_name 
val x956 = x953.s_nationkey 
val x957 = Record1020(x955, x956) 
x957}))
 }
}.groupByKey() 
val x965 = L.map(x961 => { val x962 = x961.l_orderkey 
val x963 = x961.l_partkey 
val x964 = Record1021(x962, x963) 
x964 }) 
val x971 = { val out1 = x960.map{ case (x966, x967) => ({val x969 = x966.p_partkey 
x969}, (x966, x967)) }
  val out2 = x965.map{ case x968 => ({val x970 = x968.l_partkey 
x970}, x968) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x976 = O.map(x972 => { val x973 = x972.o_custkey 
val x974 = x972.o_orderkey 
val x975 = Record1022(x973, x974) 
x975 }) 
val x983 = { val out1 = x971.map{ case ((x977, x978), x979) => ({val x981 = x979.l_orderkey 
x981}, ((x977, x978), x979)) }
  val out2 = x976.map{ case x980 => ({val x982 = x980.o_orderkey 
x982}, x980) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x989 = C.map(x984 => { val x985 = x984.c_name 
val x986 = x984.c_nationkey 
val x987 = x984.c_custkey 
val x988 = Record1023(x985, x986, x987) 
x988 }) 
val x997 = { val out1 = x983.map{ case (((x990, x991), x992), x993) => ({val x995 = x993.o_custkey 
x995}, (((x990, x991), x992), x993)) }
  val out2 = x989.map{ case x994 => ({val x996 = x994.c_custkey 
x996}, x994) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x1009 = x997.flatMap{ case ((((x998, x999), x1000), x1001), x1002) => val x1008 = (x1001,x1000,x1002) 
x1008 match {
   case (_,null,_) => Nil
case (_,_,null) => Nil 
   case x1007 => List(({val x1003 = (x998,x999) 
x1003}, {val x1004 = x1002.c_name 
val x1005 = x1002.c_nationkey 
val x1006 = Record1025(x1004, x1005) 
x1006}))
 }
}.groupByKey() 
val x1015 = x1009.map{ case ((x1010, x1011), x1012) => 
   val x1013 = x1010.p_name 
val x1014 = Query3Out(x1013, x1011, x1012) 
x1014 
} 
x1015 } 
 Query5.cache 
 Query5.count
   var start0 = System.currentTimeMillis()
   def f() {
     val x1032 = Query5 
val x1036 = x1032.flatMap{ case x1033 => x1033 match {
   case null => List((x1033, null))
   case _ => 
   {val x1034 = x1033.customers 
x1034} match {
     case Nil => List((x1033, null))
     case lst => lst.map{ case x1035 => (x1033, x1035) }
  }
 }} 
val x1041 = x1036.flatMap{ case (x1037, x1038) => (x1037, x1038) match {
   case (_, null) => List(((x1037, x1038), null))
   case _ => 
   {val x1039 = x1037.suppliers 
x1039} match {
     case Nil => List(((x1037, x1038), null))
     case lst => lst.map{ case x1040 => ((x1037, x1038), x1040) }
  }
 }} 
val x1052 = x1041.flatMap{ case ((x1042, x1043), x1044) => val x1051 = (x1044) 
x1051 match {
   case (null) => Nil
   case x1050 => List(({val x1045 = (x1042,x1043) 
x1045}, {val x1046 = x1043.c_nationkey 
val x1047 = x1044.s_nationkey 
val x1048 = x1046 == x1047 
val x1049 = 
 if ({x1048})
 {  1}
 else 0  
x1049}))
 }
}.reduceByKey(_ + _) 
val x1060 = x1052.flatMap{ case ((x1053, x1054), x1055) => val x1059 = (x1054) 
x1059 match {
   case x1057 if {val x1058 = x1055 == 0 
x1058} => List(({val x1056 = (x1053) 
x1056}, {1}))
   case x1057 => List(({val x1056 = (x1053) 
x1056}, 0))
 }
}.reduceByKey(_ + _) 
val x1065 = x1060.map{ case (x1061, x1062) => 
   val x1063 = x1061.p_name 
val x1064 = Record1068(x1063, x1062) 
x1064 
} 
x1065.count
   }
   f
   var end0 = System.currentTimeMillis() - start0
   println("Query5Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
