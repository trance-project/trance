
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record1102(p_name: String, p_partkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1103(ps_suppkey: Int, ps_partkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1104(s_name: String, s_nationkey: Int, s_suppkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1106(s_name: String, s_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1107(l_orderkey: Int, l_partkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1108(o_custkey: Int, o_orderkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1109(c_name: String, c_nationkey: Int, c_custkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1111(c_name: String, c_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1153(p_name: String, cnt: Int, uniqueId: Long) extends CaseClassRecord
case class Query3Out(p_name: String, suppliers: Iterable[Record1106], customers: Iterable[Record1111], uniqueId: Long) extends CaseClassRecord
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
   val Query5 = { val x1014 = P.map(x1010 => { val x1011 = x1010.p_name 
val x1012 = x1010.p_partkey 
val x1013 = Record1102(x1011, x1012, newId) 
x1013 }) 
val x1019 = PS.map(x1015 => { val x1016 = x1015.ps_suppkey 
val x1017 = x1015.ps_partkey 
val x1018 = Record1103(x1016, x1017, newId) 
x1018 }) 
val x1024 = { val out1 = x1014.map{ case x1020 => ({val x1022 = x1020.p_partkey 
x1022}, x1020) }
  val out2 = x1019.map{ case x1021 => ({val x1023 = x1021.ps_partkey 
x1023}, x1021) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x1030 = S.map(x1025 => { val x1026 = x1025.s_name 
val x1027 = x1025.s_nationkey 
val x1028 = x1025.s_suppkey 
val x1029 = Record1104(x1026, x1027, x1028, newId) 
x1029 }) 
val x1036 = { val out1 = x1024.map{ case (x1031, x1032) => ({val x1034 = x1032.ps_suppkey 
x1034}, (x1031, x1032)) }
  val out2 = x1030.map{ case x1033 => ({val x1035 = x1033.s_suppkey 
x1035}, x1033) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x1046 = x1036.map{ case ((x1037, x1038), x1039) => val x1045 = (x1038,x1039) 
x1045 match {
   case (_,null) => ({val x1040 = (x1037) 
x1040}, null) 
   case x1044 => ({val x1040 = (x1037) 
x1040}, {val x1041 = x1039.s_name 
val x1042 = x1039.s_nationkey 
val x1043 = Record1106(x1041, x1042, newId) 
x1043})
 }
}.groupByKey() 
val x1051 = L.map(x1047 => { val x1048 = x1047.l_orderkey 
val x1049 = x1047.l_partkey 
val x1050 = Record1107(x1048, x1049, newId) 
x1050 }) 
val x1057 = { val out1 = x1046.map{ case (x1052, x1053) => ({val x1055 = x1052.p_partkey 
x1055}, (x1052, x1053)) }
  val out2 = x1051.map{ case x1054 => ({val x1056 = x1054.l_partkey 
x1056}, x1054) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x1062 = O.map(x1058 => { val x1059 = x1058.o_custkey 
val x1060 = x1058.o_orderkey 
val x1061 = Record1108(x1059, x1060, newId) 
x1061 }) 
val x1069 = { val out1 = x1057.map{ case ((x1063, x1064), x1065) => ({val x1067 = x1065.l_orderkey 
x1067}, ((x1063, x1064), x1065)) }
  val out2 = x1062.map{ case x1066 => ({val x1068 = x1066.o_orderkey 
x1068}, x1066) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x1075 = C.map(x1070 => { val x1071 = x1070.c_name 
val x1072 = x1070.c_nationkey 
val x1073 = x1070.c_custkey 
val x1074 = Record1109(x1071, x1072, x1073, newId) 
x1074 }) 
val x1083 = { val out1 = x1069.map{ case (((x1076, x1077), x1078), x1079) => ({val x1081 = x1079.o_custkey 
x1081}, (((x1076, x1077), x1078), x1079)) }
  val out2 = x1075.map{ case x1080 => ({val x1082 = x1080.c_custkey 
x1082}, x1080) }
  out1.join(out2).map{ case (k,v) => v }
  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x1095 = x1083.map{ case ((((x1084, x1085), x1086), x1087), x1088) => val x1094 = (x1086,x1088,x1087) 
x1094 match {
   case (_,null,_) => ({val x1089 = (x1084,x1085) 
x1089}, null)
case (_,_,null) => ({val x1089 = (x1084,x1085) 
x1089}, null) 
   case x1093 => ({val x1089 = (x1084,x1085) 
x1089}, {val x1090 = x1088.c_name 
val x1091 = x1088.c_nationkey 
val x1092 = Record1111(x1090, x1091, newId) 
x1092})
 }
}.groupByKey() 
val x1101 = x1095.map{ case ((x1096, x1097), x1098) => 
   val x1099 = x1096.p_name 
val x1100 = Query3Out(x1099, x1097, x1098, newId) 
x1100 
} 
x1101 } 
 Query5.cache 
 Query5.count
   var start0 = System.currentTimeMillis()
   def f() {
     val x1118 = Query5 
val x1122 = x1118.flatMap{ case x1119 => x1119 match {
   case null => List((x1119, null))
   case _ => 
   {val x1120 = x1119.customers 
x1120} match {
     case Nil => List((x1119, null))
     case lst => lst.map{ case x1121 => (x1119, x1121) }
  }
 }} 
val x1130 = x1122.flatMap{ case (x1123, x1124) => (x1123, x1124) match {
   case (_, null) => List(((x1123, x1124), null))
   case _ => 
   {val x1125 = x1123.suppliers 
x1125} match {
     case Nil => List(((x1123, x1124), null))
     case lst => lst.map{ case x1126 => if ({val x1127 = x1124.c_nationkey 
val x1128 = x1126.s_nationkey 
val x1129 = x1127 == x1128 
x1129}) { ((x1123, x1124), x1126) } else { ((x1123, x1124), null) } }
  }
 }} 
val x1137 = x1130.map{ case ((x1131, x1132), x1133) => val x1136 = (x1133) 
x1136 match {
   case (null) => ({val x1134 = (x1131,x1132) 
x1134}, 0)
   case x1135 => ({val x1134 = (x1131,x1132) 
x1134}, {1})
 }
}.reduceByKey(_ + _) 
val x1145 = x1137.map{ case ((x1138, x1139), x1140) => val x1144 = (x1139) 
x1144 match {
   case x1142 if {val x1143 = x1140 == 0 
x1143} => ({val x1141 = (x1138) 
x1141}, {1})
   case x1142 => ({val x1141 = (x1138) 
x1141}, 0)
 }
}.reduceByKey(_ + _) 
val x1150 = x1145.map{ case (x1146, x1147) => 
   val x1148 = x1146.p_name 
val x1149 = Record1153(x1148, x1147, newId) 
x1149 
} 
x1150.count
   }
   f
   var end0 = System.currentTimeMillis() - start0
   println("Query5Spark"+sf+","+Config.datapath+","+end0)
 }
}
