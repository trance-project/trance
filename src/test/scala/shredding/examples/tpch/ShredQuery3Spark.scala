
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record1227(lbl: Unit)
case class Record1228(ps_partkey: Int, ps_suppkey: Int)
case class Record1229(s_name: String, s_nationkey: Int, s_suppkey: Int)
case class Record1231(ps_partkey: Int, s_name: String, s_nationkey: Int)
case class Record1232(_1: Record1227, _2: (Iterable[Record1231]))
case class Record1233(o_orderkey: Int, o_custkey: Int)
case class Record1234(c_name: String, c_nationkey: Int, c_custkey: Int)
case class Record1236(o_orderkey: Int, c_name: String, c_nationkey: Int)
case class Record1237(_1: Record1227, _2: (Iterable[Record1236]))
case class Record1238(l_partkey: Int, l_orderkey: Int)
case class Record1240(l_partkey: Int, c_name: String, c_nationkey: Int)
case class Record1241(_1: Record1227, _2: (Iterable[Record1240]))
case class Record1242(p_name: String, p_partkey: Int)
case class Record1244(p__Fp_partkey: Int)
case class Record1245(p_name: String, suppliers: Record1244, customers: Record1244)
case class Record1246(_1: Record1227, _2: (Iterable[Record1245]))
case class Record1247(lbl: Record1244)
case class Record1249(s_name: String, s_nationkey: Int)
case class Record1250(_1: Record1247, _2: (Iterable[Record1249]))
case class Record1252(c_name: String, c_nationkey: Int)
case class Record1253(_1: Record1247, _2: (Iterable[Record1252]))
object ShredQuery3Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery3Spark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
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

   def f = { 
 val x992 = () 
val x993 = Record1227(x992) 
val partsuppliers__F = x993
val x994 = partsuppliers__F
//partsuppliers__F.collect.foreach(println(_))
val x995 = List(partsuppliers__F) 
val x997 = x995 
val x998 = PS__D_1 
val x1003 = x998.map(x999 => { val x1000 = x999.ps_partkey 
val x1001 = x999.ps_suppkey 
val x1002 = Record1228(x1000, x1001) 
x1002 }) 
val x1006 = x1003.map{ case c => (x997.head, c) } 
val x1007 = S__D_1 
val x1013 = x1007.map(x1008 => { val x1009 = x1008.s_name 
val x1010 = x1008.s_nationkey 
val x1011 = x1008.s_suppkey 
val x1012 = Record1229(x1009, x1010, x1011) 
x1012 }) 
val x1019 = { val out1 = x1006.map{ case (x1014, x1015) => ({val x1017 = x1015.ps_suppkey 
x1017}, (x1014, x1015)) }
  val out2 = x1013.map{ case x1016 => ({val x1018 = x1016.s_suppkey 
x1018}, x1016) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x1030 = x1019.map{ case ((x1020, x1021), x1022) => val x1029 = (x1021,x1022) 
x1029 match {
   case (_,null) => ({val x1023 = (x1020) 
x1023}, null) 
   case x1028 => ({val x1023 = (x1020) 
x1023}, {val x1024 = x1021.ps_partkey 
val x1025 = x1022.s_name 
val x1026 = x1022.s_nationkey 
val x1027 = Record1231(x1024, x1025, x1026) 
x1027})
 }
}.groupByLabel() 
val x1035 = x1030.map{ case (x1031, x1032) => 
   val x1033 = (x1032) 
val x1034 = Record1232(x1031, x1033) 
x1034 
} 
val partsuppliers__D_1 = x1035.flatMap{r => r._2}
val x1036 = partsuppliers__D_1
//partsuppliers__D_1.collect.foreach(println(_))
val custorders__F = x993
val x1037 = custorders__F
//custorders__F.collect.foreach(println(_))
val x1038 = List(custorders__F) 
val x1040 = x1038 
val x1041 = O__D_1 
val x1046 = x1041.map(x1042 => { val x1043 = x1042.o_orderkey 
val x1044 = x1042.o_custkey 
val x1045 = Record1233(x1043, x1044) 
x1045 }) 
val x1049 = x1046.map{ case c => (x1040.head, c) } 
val x1050 = C__D_1 
val x1056 = x1050.map(x1051 => { val x1052 = x1051.c_name 
val x1053 = x1051.c_nationkey 
val x1054 = x1051.c_custkey 
val x1055 = Record1234(x1052, x1053, x1054) 
x1055 }) 
val x1062 = { val out1 = x1049.map{ case (x1057, x1058) => ({val x1060 = x1058.o_custkey 
x1060}, (x1057, x1058)) }
  val out2 = x1056.map{ case x1059 => ({val x1061 = x1059.c_custkey 
x1061}, x1059) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x1073 = x1062.map{ case ((x1063, x1064), x1065) => val x1072 = (x1064,x1065) 
x1072 match {
   case (_,null) => ({val x1066 = (x1063) 
x1066}, null) 
   case x1071 => ({val x1066 = (x1063) 
x1066}, {val x1067 = x1064.o_orderkey 
val x1068 = x1065.c_name 
val x1069 = x1065.c_nationkey 
val x1070 = Record1236(x1067, x1068, x1069) 
x1070})
 }
}.groupByLabel() 
val x1078 = x1073.map{ case (x1074, x1075) => 
   val x1076 = (x1075) 
val x1077 = Record1237(x1074, x1076) 
x1077 
} 
val custorders__D_1 = x1078.flatMap{ r => r._2 }
val x1079 = custorders__D_1
//custorders__D_1.collect.foreach(println(_))
val cparts__F = x993
val x1080 = cparts__F
//cparts__F.collect.foreach(println(_))
val x1081 = List(cparts__F) 
val x1083 = x1081 
val x1084 = custorders__D_1 
val x1086 = x1084 
val x1089 = x1086.map{ case c => (x1083.head, c) } 
val x1090 = L__D_1 
val x1095 = x1090.map(x1091 => { val x1092 = x1091.l_partkey 
val x1093 = x1091.l_orderkey 
val x1094 = Record1238(x1092, x1093) 
x1094 }) 
val x1101 = { val out1 = x1089.map{ case (x1096, x1097) => ({val x1099 = x1097.o_orderkey 
x1099}, (x1096, x1097)) }
  val out2 = x1095.map{ case x1098 => ({val x1100 = x1098.l_orderkey 
x1100}, x1098) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1112 = x1101.map{ case ((x1102, x1103), x1104) => val x1111 = (x1103,x1104) 
x1111 match {
   case (_,null) => ({val x1105 = (x1102) 
x1105}, null) 
   case x1110 => ({val x1105 = (x1102) 
x1105}, {val x1106 = x1104.l_partkey 
val x1107 = x1103.c_name 
val x1108 = x1103.c_nationkey 
val x1109 = Record1240(x1106, x1107, x1108) 
x1109})
 }
}.groupByLabel() 
val x1117 = x1112.map{ case (x1113, x1114) => 
   val x1115 = (x1114) 
val x1116 = Record1241(x1113, x1115) 
x1116 
} 
val cparts__D_1 = x1117.flatMap{ r => r._2 }
val x1118 = cparts__D_1
//cparts__D_1.collect.foreach(println(_))
val x1119 = List(x993) 
val M_ctx1 = x1119
val x1120 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x1122 = M_ctx1 
val x1123 = P__D_1 
val x1128 = x1123.map(x1124 => { val x1125 = x1124.p_name 
val x1126 = x1124.p_partkey 
val x1127 = Record1242(x1125, x1126) 
x1127 }) 
val x1131 = x1128.map{ case c => (x1122.head, c) } 
val x1141 = x1131.map{ case (x1132, x1133) => val x1140 = (x1133) 
x1140 match {
   case (null) => ({val x1134 = (x1132) 
x1134}, null) 
   case x1139 => ({val x1134 = (x1132) 
x1134}, {val x1135 = x1133.p_name 
val x1136 = x1133.p_partkey 
val x1137 = Record1244(x1136) 
val x1138 = Record1245(x1135, x1137, x1137) 
x1138})
 }
}.groupByLabel() 
val x1146 = x1141.map{ case (x1142, x1143) => 
   val x1144 = (x1143) 
val x1145 = Record1246(x1142, x1144) 
x1145 
} 
val M_flat1 = x1146
val x1147 = M_flat1
//M_flat1.collect.foreach(println(_))
val x1149 = M_flat1 
val x1153 = x1149.flatMap{ case x1150 => x1150 match {
   case null => List((x1150, null))
   case _ =>
   val x1151 = x1150._2 
x1151 match {
     case x1152 => x1152.map{ case v2 => (x1150, v2) }
  }
 }} 
val x1158 = x1153.map{ case (x1154, x1155) => 
   val x1156 = x1155.suppliers 
val x1157 = Record1247(x1156) 
x1157 
} 
val x1159 = x1158.distinct 
val M_ctx2 = x1159
val x1160 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x1162 = M_ctx2 
val x1163 = partsuppliers__D_1
val x1165 = x1163 
val x1171 = { val out1 = x1162.map{ case x1166 => ({val x1168 = x1166.lbl 
val x1169 = x1168.p__Fp_partkey 
x1169}, x1166) }
  val out2 = x1165.map{ case x1167 => ({val x1170 = x1167.ps_partkey 
x1170}, x1167) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1180 = x1171.map{ case (x1172, x1173) => val x1179 = (x1173) 
x1179 match {
   case (null) => ({val x1174 = (x1172) 
x1174}, null) 
   case x1178 => ({val x1174 = (x1172) 
x1174}, {val x1175 = x1173.s_name 
val x1176 = x1173.s_nationkey 
val x1177 = Record1249(x1175, x1176) 
x1177})
 }
}.groupByLabel() 
val x1185 = x1180.map{ case (x1181, x1182) => 
   val x1183 = (x1182) 
val x1184 = Record1250(x1181, x1183) 
x1184 
} 
val M_flat2 = x1185
val x1186 = M_flat2
//M_flat2.collect.foreach(println(_))
val x1188 = M_flat1 
val x1192 = x1188.flatMap{ case x1189 => x1189 match {
   case null => List((x1189, null))
   case _ =>
   val x1190 = x1189._2 
x1190 match {
     case x1191 => x1191.map{ case v2 => (x1189, v2) }
  }
 }} 
val x1197 = x1192.map{ case (x1193, x1194) => 
   val x1195 = x1194.customers 
val x1196 = Record1247(x1195) 
x1196 
} 
val x1198 = x1197.distinct 
val M_ctx3 = x1198
val x1199 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x1201 = M_ctx3 
val x1202 = cparts__D_1
val x1204 = x1202 
val x1210 = { val out1 = x1201.map{ case x1205 => ({val x1207 = x1205.lbl 
val x1208 = x1207.p__Fp_partkey 
x1208}, x1205) }
  val out2 = x1204.map{ case x1206 => ({val x1209 = x1206.l_partkey 
x1209}, x1206) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1219 = x1210.map{ case (x1211, x1212) => val x1218 = (x1212) 
x1218 match {
   case (null) => ({val x1213 = (x1211) 
x1213}, null) 
   case x1217 => ({val x1213 = (x1211) 
x1213}, {val x1214 = x1212.c_name 
val x1215 = x1212.c_nationkey 
val x1216 = Record1252(x1214, x1215) 
x1216})
 }
}.groupByLabel() 
val x1224 = x1219.map{ case (x1220, x1221) => 
   val x1222 = (x1221) 
val x1223 = Record1253(x1220, x1222) 
x1223 
} 
val M_flat3 = x1224
val x1225 = M_flat3
//M_flat3.collect.foreach(println(_))
x1225.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis()
   println("ShredQuery3Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
