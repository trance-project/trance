
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record1043(ps_partkey: Int, ps_suppkey: Int)
case class Record1044(s_name: String, s_nationkey: Int, s_suppkey: Int)
case class Record1045(ps_partkey: Int, s_name: String, s_nationkey: Int)
case class Record1046(o_orderkey: Int, o_custkey: Int)
case class Record1047(c_name: String, c_nationkey: Int, c_custkey: Int)
case class Record1048(o_orderkey: Int, c_name: String, c_nationkey: Int)
case class Record1049(l_partkey: Int, l_orderkey: Int)
case class Record1050(l_partkey: Int, c_name: String, c_nationkey: Int)
case class Record1051(p_name: String, p_partkey: Int)
case class Record1052(p__Fp_partkey: Int)
case class Record1053(p_name: String, suppliers: Record1052, customers: Record1052)
case class Record1054(s_name: String, s_nationkey: Int)
case class Record1055(c_name: String, c_nationkey: Int)
case class Record1123(n_name: String, n_nationkey: Int)
case class Record1124(n__Fn_nationkey: Int)
case class Record1125(n_name: String, parts: Record1124)
case class Record1127(p_name: String)
object ShredQuery7FullSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery7FullSpark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val N__F = 6
val N__D_1 = tpch.loadNation
N__D_1.cache
N__D_1.count
val L__F = 3
val L__D_1 = tpch.loadLineitem
L__D_1.cache
L__D_1.count
val P__F = 4
val P__D_1 = tpch.loadPart
P__D_1.cache
P__D_1.count
val C__F = 1
val C__D_1 = tpch.loadCustomers
C__D_1.cache
C__D_1.count
val O__F = 2
val O__D_1 = tpch.loadOrders
O__D_1.cache
O__D_1.count
val PS__F = 5
val PS__D_1 = tpch.loadPartSupp
PS__D_1.cache
PS__D_1.count
val S__F = 6
val S__D_1 = tpch.loadSupplier
S__D_1.cache
S__D_1.count

   val x939 = PS__D_1.map(x935 => { val x936 = x935.ps_partkey 
val x937 = x935.ps_suppkey 
val x938 = Record1043(x936, x937) 
x938 }) 
val x940 = S__D_1 
val x946 = x940.map(x941 => { val x942 = x941.s_name 
val x943 = x941.s_nationkey 
val x944 = x941.s_suppkey 
val x945 = Record1044(x942, x943, x944) 
x945 }) 
val x951 = { val out1 = x939.map{ case x947 => ({val x949 = x947.ps_suppkey 
x949}, x947) }
  val out2 = x946.map{ case x948 => ({val x950 = x948.s_suppkey 
x950}, x948) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x958 = x951.map{ case (x952, x953) => 
   val x954 = x952.ps_partkey 
val x955 = x953.s_name 
val x956 = x953.s_nationkey 
val x957 = Record1045(x954, x955, x956) 
x957 
} 
val partsuppliers__D_1 = x958
val x959 = partsuppliers__D_1
//partsuppliers__D_1.collect.foreach(println(_))
val x964 = O__D_1.map(x960 => { val x961 = x960.o_orderkey 
val x962 = x960.o_custkey 
val x963 = Record1046(x961, x962) 
x963 }) 
val x965 = C__D_1 
val x971 = x965.map(x966 => { val x967 = x966.c_name 
val x968 = x966.c_nationkey 
val x969 = x966.c_custkey 
val x970 = Record1047(x967, x968, x969) 
x970 }) 
val x976 = { val out1 = x964.map{ case x972 => ({val x974 = x972.o_custkey 
x974}, x972) }
  val out2 = x971.map{ case x973 => ({val x975 = x973.c_custkey 
x975}, x973) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x983 = x976.map{ case (x977, x978) => 
   val x979 = x977.o_orderkey 
val x980 = x978.c_name 
val x981 = x978.c_nationkey 
val x982 = Record1048(x979, x980, x981) 
x982 
} 
val custorders__D_1 = x983
val x984 = custorders__D_1
//custorders__D_1.collect.foreach(println(_))
val x986 = custorders__D_1 
val x987 = L__D_1 
val x992 = x987.map(x988 => { val x989 = x988.l_partkey 
val x990 = x988.l_orderkey 
val x991 = Record1049(x989, x990) 
x991 }) 
val x997 = { val out1 = x986.map{ case x993 => ({val x995 = x993.o_orderkey 
x995}, x993) }
  val out2 = x992.map{ case x994 => ({val x996 = x994.l_orderkey 
x996}, x994) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1004 = x997.map{ case (x998, x999) => 
   val x1000 = x999.l_partkey 
val x1001 = x998.c_name 
val x1002 = x998.c_nationkey 
val x1003 = Record1050(x1000, x1001, x1002) 
x1003 
} 
val cparts__D_1 = x1004
val x1005 = cparts__D_1
//cparts__D_1.collect.foreach(println(_))
val x1010 = P__D_1.map(x1006 => { val x1007 = x1006.p_name 
val x1008 = x1006.p_partkey 
val x1009 = Record1051(x1007, x1008) 
x1009 }) 
val x1016 = x1010.map{ case x1011 => 
   val x1012 = x1011.p_name 
val x1013 = x1011.p_partkey 
val x1014 = Record1052(x1013) 
val x1015 = Record1053(x1012, x1014, x1014) 
x1015 
} 
val M_flat1 = x1016
val x1017 = M_flat1
//M_flat1.collect.foreach(println(_))
val x1019 = partsuppliers__D_1 
val x1028 = x1019.map{ case x1020 => 
   val x1021 = x1020.ps_partkey 
val x1022 = Record1052(x1021) 
val x1023 = x1020.s_name 
val x1024 = x1020.s_nationkey 
val x1025 = Record1054(x1023, x1024) 
val x1026 = List(x1025) 
val x1027 = (x1022, x1026) 
x1027 
}//.groupByLabel() 
val M_flat2 = x1028
val x1029 = M_flat2
//M_flat2.collect.foreach(println(_))
val x1031 = cparts__D_1 
val x1040 = x1031.map{ case x1032 => 
   val x1033 = x1032.l_partkey 
val x1034 = Record1052(x1033) 
val x1035 = x1032.c_name 
val x1036 = x1032.c_nationkey 
val x1037 = Record1055(x1035, x1036) 
val x1038 = List(x1037) 
val x1039 = (x1034, x1038) 
x1039 
}//.groupByLabel() 
val M_flat3 = x1040
val x1041 = M_flat3
//M_flat3.collect.foreach(println(_))
val Query3__D_1 = M_flat1
Query3__D_1.cache
Query3__D_1.count
val Query3__D_2suppliers_1 = M_flat2
Query3__D_2suppliers_1.cache
Query3__D_2suppliers_1.count
val Query3__D_2customers_1 = M_flat3
Query3__D_2customers_1.cache
Query3__D_2customers_1.count
def f = { 
 val x1070 = N__D_1.map(x1066 => { val x1067 = x1066.n_name 
val x1068 = x1066.n_nationkey 
val x1069 = Record1123(x1067, x1068) 
x1069 }) 
val x1076 = x1070.map{ case x1071 => 
   val x1072 = x1071.n_name 
val x1073 = x1071.n_nationkey 
val x1074 = Record1124(x1073) 
val x1075 = Record1125(x1072, x1074) 
x1075 
} 
val M_flat1 = x1076
val x1077 = M_flat1
M_flat1.count
//M_flat1.collect.foreach(println(_))
val x1079 = Query3__D_1 
val x1081 = Query3__D_2suppliers_1 
val x1084 = x1081 
val x1088 = { val out1 = x1079.map{ case x1085 => ({val x1087 = x1085.suppliers 
x1087}, x1085) }
  val out2 = x1084.flatMapValues(identity)
  out1.lookup(out2)
} 
val x1089 = Query3__D_2customers_1 
val x1092 = x1089 
val x1097 = { val out1 = x1088.map{ case (a, null) => (null, (a, null)); case (x1093, x1094) => ({val x1096 = x1093.customers 
x1096}, (x1093, x1094)) }
  val out2 = x1092.flatMapValues(identity)
  out1.outerLookup(out2)
} 
val x1108 = x1097.flatMap{ case ((x1098, x1099), x1100) => val x1107 = (x1100) 
x1107 match {
   case (null) => Nil
   case x1106 => List(({val x1101 = (x1098,x1099) 
x1101}, {val x1102 = x1100.c_nationkey 
val x1103 = x1099.s_nationkey 
val x1104 = x1102 == x1103 
val x1105 = 
 if ({x1104})
 {  1}
 else 0  
x1105}))
 }
}.reduceByKey(_ + _) 
val x1120 = x1108.map{ case ((x1109, x1110), x1111) => 
   val x1112 = x1110.s_nationkey 
val x1113 = Record1124(x1112) 
val x1114 = x1111 == 0 
val x1115 = x1109.p_name 
val x1116 = Record1127(x1115) 
val x1117 = List(x1116) 
val x1118 = 
 if ({x1114})
 {  x1117}
 else Nil  
val x1119 = (x1113, x1118) 
x1119 
}.groupByLabel() 
val M_flat2 = x1120
val x1121 = M_flat2
M_flat2.collect.foreach(println(_))
x1121.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery7FullSpark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
