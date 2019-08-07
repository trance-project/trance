
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
case class Record1141(p__F: Part, S__F: Int, PS__F: Int, uniqueId: Long)
case class Record1142(p__F: Part, C__F: Int, L__F: Int, O__F: Int, uniqueId: Long)
case class Record1143(p_name: String, suppliers: Record1141, customers: Record1142, uniqueId: Long)
case class Record1147(s_name: String, s_nationkey: Int, uniqueId: Long)
case class Record1152(c_name: String, c_nationkey: Int, uniqueId: Long)
case class Record1230(lbl: Q5Flat, uniqueId: Long)
case class Input_Query5__D(p_name: String, suppliers: Int, customers: Int, uniqueId: Long)
case class Record1235(p_name: String, cnt: Int, uniqueId: Long)
case class Record1236(_1: Q5Flat, _2: List[Record1235], uniqueId: Long)
case class RecM_ctx1(lbl: Q3Flat, uniqueId: Long)
case class RecM_ctx3(lbl: Record1142, uniqueId: Long)
case class RecM_flat1(_1: Q3Flat, _2: List[Record1143], uniqueId: Long)
case class RecM_flat3(_1: Record1142, _2: List[Record1152], uniqueId: Long)
case class RecM_flat2(_1: Record1141, _2: List[Record1147], uniqueId: Long)
case class RecM_ctx2(lbl: Record1141, uniqueId: Long)
object ShredQuery5Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery5Spark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   
val tpch = TPCHLoader(spark)
val C__F = 1
val C__D_1 = tpch.loadCustomers()
C__D_1.cache
C__D_1.count
val O__F = 2
val O__D_1 = tpch.loadOrders()
O__D_1.cache
O__D_1.count
val L__F = 3
val L__D_1 = tpch.loadLineitem()
L__D_1.cache
L__D_1.count
val P__F = 4
val P__D_1 = tpch.loadPart()
P__D_1.cache
P__D_1.count
val PS__F = 5
val PS__D_1 = tpch.loadPartSupp
PS__D_1.cache
PS__D_1.count
val S__F = 6
val S__D_1 = tpch.loadSupplier
S__D_1.cache
S__D_1.count
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
   val x993 = Q3Flat(O__F, C__F, PS__F, S__F, L__F, P__F, newId) 
val x994 = RecM_ctx1(x993, newId) 
val x995 = List(x994) 
val M_ctx1 = spark.sparkContext.parallelize(x995)
//println("M_ctx1")
val x996 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x998 = M_ctx1 
val x999 = P__D_1 
val x1001 = x999 
val x1004 = { val out1 = x998.map{ case x1002 => ({true}, x1002) }
  val out2 = x1001.map{ case x1003 => ({true}, x1003) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1020 = x1004.map{ case (x1005, x1006) => val x1019 = (x1006) 
x1019 match {
   case (null) => ({val x1007 = (x1005) 
x1007}, Nil) => ({val x1007 = (x1005) 
x1007}, Nil) 
   case x1018 => ({val x1007 = (x1005) 
x1007}, List({val x1008 = x1006.p_name 
val x1009 = x1005.lbl 
val x1010 = x1009.S__F 
val x1011 = x1009.PS__F 
val x1012 = Record1141(x1006, x1010, x1011, newId) 
val x1013 = x1009.C__F 
val x1014 = x1009.L__F 
val x1015 = x1009.O__F 
val x1016 = Record1142(x1006, x1013, x1014, x1015, newId) 
val x1017 = Record1143(x1008, x1012, x1016, newId) 
x1017}))
 }
}.foldByKey(Nil){ case (acc1140, x1018) => acc1140 ++ x1018 } 
val x1025 = x1020.map{ case (x1021, x1022) => 
   val x1023 = x1021.lbl 
val x1024 = RecM_flat1(x1023, x1022, newId) 
x1024 
} 
val M_flat1 = x1025
//println("M_flat1")
val x1026 = M_flat1
//M_flat1.collect.foreach(println(_))
val x1028 = M_flat1 
val x1032 = x1028.flatMap{ case x1029 => x1029 match {
   case null => List((x1029, null))
   case _ =>
   val x1030 = x1029._2 
x1030 match {
     case x1031 => x1031.map{ case v2 => (x1029, v2) }
  }
 }} 
val x1037 = x1032.map{ case (x1033, x1034) => 
   val x1035 = x1034.suppliers 
val x1036 = RecM_ctx2(x1035, newId) 
x1036 
} 
val x1038 = x1037.distinct 
val M_ctx2 = x1038
//println("M_ctx2")
val x1039 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x1041 = M_ctx2 
val x1042 = PS__D_1 
val x1044 = x1042 
val x1051 = { val out1 = x1041.map{ case x1045 => ({val x1047 = x1045.lbl 
val x1048 = x1047.p__F 
val x1049 = x1048.p_partkey 
x1049}, x1045) }
  val out2 = x1044.map{ case x1046 => ({val x1050 = x1046.ps_partkey 
x1050}, x1046) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1052 = S__D_1 
val x1054 = x1052 
val x1060 = { val out1 = x1051.map{ case (x1055, x1056) => ({val x1058 = x1056.ps_suppkey 
x1058}, (x1055, x1056)) }
  val out2 = x1054.map{ case x1057 => ({val x1059 = x1057.s_suppkey 
x1059}, x1057) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1070 = x1060.map{ case ((x1061, x1062), x1063) => val x1069 = (x1062,x1063) 
x1069 match {
   case (_,null) => ({val x1064 = (x1061) 
x1064}, Nil) => ({val x1064 = (x1061) 
x1064}, Nil) 
   case x1068 => ({val x1064 = (x1061) 
x1064}, List({val x1065 = x1063.s_name 
val x1066 = x1063.s_nationkey 
val x1067 = Record1147(x1065, x1066, newId) 
x1067}))
 }
}.foldByKey(Nil){ case (acc1146, x1068) => acc1146 ++ x1068 } 
val x1075 = x1070.map{ case (x1071, x1072) => 
   val x1073 = x1071.lbl 
val x1074 = RecM_flat2(x1073, x1072, newId) 
x1074 
} 
val M_flat2 = x1075
//println("M_flat2")
val x1076 = M_flat2
//M_flat2.collect.foreach(println(_))
val x1078 = M_flat1 
val x1082 = x1078.flatMap{ case x1079 => x1079 match {
   case null => List((x1079, null))
   case _ =>
   val x1080 = x1079._2 
x1080 match {
     case x1081 => x1081.map{ case v2 => (x1079, v2) }
  }
 }} 
val x1087 = x1082.map{ case (x1083, x1084) => 
   val x1085 = x1084.customers 
val x1086 = RecM_ctx3(x1085, newId) 
x1086 
} 
val x1088 = x1087.distinct 
val M_ctx3 = x1088
//println("M_ctx3")
val x1089 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x1091 = M_ctx3 
val x1092 = L__D_1 
val x1094 = x1092 
val x1101 = { val out1 = x1091.map{ case x1095 => ({val x1097 = x1095.lbl 
val x1098 = x1097.p__F 
val x1099 = x1098.p_partkey 
x1099}, x1095) }
  val out2 = x1094.map{ case x1096 => ({val x1100 = x1096.l_partkey 
x1100}, x1096) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1102 = O__D_1 
val x1104 = x1102 
val x1110 = { val out1 = x1101.map{ case (x1105, x1106) => ({val x1108 = x1106.l_orderkey 
x1108}, (x1105, x1106)) }
  val out2 = x1104.map{ case x1107 => ({val x1109 = x1107.o_orderkey 
x1109}, x1107) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1111 = C__D_1 
val x1113 = x1111 
val x1120 = { val out1 = x1110.map{ case ((x1114, x1115), x1116) => ({val x1118 = x1116.o_custkey 
x1118}, ((x1114, x1115), x1116)) }
  val out2 = x1113.map{ case x1117 => ({val x1119 = x1117.c_custkey 
x1119}, x1117) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1131 = x1120.map{ case (((x1121, x1122), x1123), x1124) => val x1130 = (x1122,x1123,x1124) 
x1130 match {
   case (_,null,_,_) => ({val x1125 = (x1121) 
x1125}, Nil)
case (_,_,null) => ({val x1125 = (x1121) 
x1125}, Nil) => ({val x1125 = (x1121) 
x1125}, Nil) 
   case x1129 => ({val x1125 = (x1121) 
x1125}, List({val x1126 = x1124.c_name 
val x1127 = x1124.c_nationkey 
val x1128 = Record1152(x1126, x1127, newId) 
x1128}))
 }
}.foldByKey(Nil){ case (acc1151, x1129) => acc1151 ++ x1129 } 
val x1136 = x1131.map{ case (x1132, x1133) => 
   val x1134 = x1132.lbl 
val x1135 = RecM_flat3(x1134, x1133, newId) 
x1135 
} 
val M_flat3 = x1136
//println("M_flat3")
val x1137 = M_flat3
//M_flat3.collect.foreach(println(_))
val res = x1137 
 
val Query5__F = M_ctx1.collect.take(1)(0).lbl
val Query5__D_1 = M_flat1.flatMap(r => r._2)
Query5__D_1.cache
Query5__D_1.count
val Query5__D_2suppliers_1 = M_flat2.map(r => (r._1, r._2))
Query5__D_2suppliers_1.cache
Query5__D_2suppliers_1.count
val Query5__D_2customers_1 = M_flat3.map(r => (r._1, r._2))
Query5__D_2customers_1.cache
Query5__D_2customers_1.count
  
   var start0 = System.currentTimeMillis()
   def f() {
     val x1163 = Q5Flat(Query5__F, newId) 
val x1164 = Record1230(x1163, newId) 
val x1165 = List(x1164) 
val M_ctx1 = spark.sparkContext.parallelize(x1165)
//println("M_ctx1")
val x1166 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x1168 = M_ctx1 
val x1169 = Query5__D_1 
val x1171 = x1169 
val x1174 = { val out1 = x1168.map{ case x1172 => ({true}, x1172) }
  val out2 = x1171.map{ case x1173 => ({true}, x1173) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1176 = Query5__D_2customers_1 
val x1179 = x1176 
val x1184 = { val out1 = x1174.map{ case (a, null) => (null, (a, null)); case (x1180, x1181) => ({val x1183 = x1181.customers 
x1183}, (x1180, x1181)) }
  val out2 = x1179.flatMap(x1182 => x1182._2.map{case v2 => (x1182._1, v2)})
  out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x1185 = Query5__D_2suppliers_1 
val x1188 = x1185 
val x1196 = { val out1 = x1184.map{ case (a, null) => (null, (a, null)); case ((x1189, x1190), x1191) => (({val x1193 = x1190.suppliers 
x1193}, {val x1195 = x1191.c_nationkey 
x1195}), ((x1189, x1190), x1191)) }
  val out2 = x1188.flatMap(v2 => v2._2.map{case x1192 => ((v2._1, {val x1194 = x1192.s_nationkey 
x1194}), x1192)})
  out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
} 
val x1204 = x1196.map{ case (((x1197, x1198), x1199), x1200) => val x1203 = (x1200) 
x1203 match {
   case (null) => ({val x1201 = (x1197,x1198,x1199) 
x1201}, 0) => ({val x1201 = (x1197,x1198,x1199) 
x1201}, 0)
   case x1202 => ({val x1201 = (x1197,x1198,x1199) 
x1201}, {1})
 }
}.foldByKey(0){ case (acc1232, x1202) => acc1232 + x1202 } 
val x1213 = x1204.map{ case ((x1205, x1206, x1207), x1208) => val x1212 = (x1207) 
x1212 match {
   case x1210 if {val x1211 = x1208 == 0 
x1211} => ({val x1209 = (x1205,x1206) 
x1209}, {1})
   case x1210 => ({val x1209 = (x1205,x1206) 
x1209}, 0)
 }
}.foldByKey(0){ case (acc1233, x1210) => acc1233 + x1210 } 
val x1222 = x1213.map{ case ((x1214, x1215), x1216) => val x1221 = (x1215,x1216) 
x1221 match {
   case (_,null) => ({val x1217 = (x1214) 
x1217}, Nil) => ({val x1217 = (x1214) 
x1217}, Nil) 
   case x1220 => ({val x1217 = (x1214) 
x1217}, List({val x1218 = x1215.p_name 
val x1219 = Record1235(x1218, x1216, newId) 
x1219}))
 }
}.foldByKey(Nil){ case (acc1234, x1220) => acc1234 ++ x1220 } 
val x1227 = x1222.map{ case (x1223, x1224) => 
   val x1225 = x1223.lbl 
val x1226 = Record1236(x1225, x1224, newId) 
x1226 
} 
val M_flat1 = x1227
//println("M_flat1")
val x1228 = M_flat1
//M_flat1.collect.foreach(println(_))
val res = x1228.count
   }
   f
   var end0 = System.currentTimeMillis() - start0
   println("ShredQuery5Spark"+sf+","+Config.datapath+","+end0)
 }
}
