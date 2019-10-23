
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record1251(lbl: Unit)
case class Record1252(p_name: String, p_partkey: Int)
case class Record1254(p__Fp_partkey: Int)
case class Record1255(p_name: String, suppliers: Record1254, customers: Record1254)
case class Record1256(_1: Record1251, _2: (Iterable[Record1255]))
case class Record1257(lbl: Record1254)
case class Record1258(ps_suppkey: Int, ps_partkey: Int)
case class Record1259(s_name: String, s_nationkey: Int, s_suppkey: Int)
case class Record1261(s_name: String, s_nationkey: Int)
case class Record1262(_1: Record1257, _2: (Iterable[Record1261]))
case class Record1263(l_orderkey: Int, l_partkey: Int)
case class Record1264(o_custkey: Int, o_orderkey: Int)
case class Record1265(c_name: String, c_nationkey: Int, c_custkey: Int)
case class Record1267(c_name: String, c_nationkey: Int)
case class Record1268(_1: Record1257, _2: (Iterable[Record1267]))
object ShredQuery3Spark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery3Spark"+sf)
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
   var start0 = System.currentTimeMillis()
   val x1093 = () 
val x1094 = Record1251(x1093) 
val x1095 = List(x1094) 
val M_ctx1 = x1095
val x1098 = M_ctx1 
val x1099 = P__D_1 
val x1104 = x1099.map(x1100 => { val x1101 = x1100.p_name 
val x1102 = x1100.p_partkey 
val x1103 = Record1252(x1101, x1102) 
x1103 }) 
val x1107 = x1104.map{ case c => (x1098.head, c) } 
val x1117 = x1107.flatMap{ case (x1108, x1109) => val x1116 = (x1109) 
x1116 match {
   case (null) => Nil 
   case x1115 => List(({val x1110 = (x1108) 
x1110}, {val x1111 = x1109.p_name 
val x1112 = x1109.p_partkey 
val x1113 = Record1254(x1112) 
val x1114 = Record1255(x1111, x1113, x1113) 
x1114}))
 }
}.groupByLabel() 
val x1122 = x1117.map{ case (x1118, x1119) => 
   val x1120 = (x1119) 
val x1121 = Record1256(x1118, x1120) 
x1121 
} 
val M_flat1 = x1122
//println("M_flat1")
val x1123 = M_flat1
//M_flat1.collect.foreach(println(_))
val x1125 = M_flat1 
val x1129 = x1125.flatMap{ case x1126 => x1126 match {
   case null => List((x1126, null))
   case _ =>
   val x1127 = x1126._2 
x1127 match {
     case x1128 => x1128.map{ case v2 => (x1126, v2) }
  }
 }} 
val x1134 = x1129.map{ case (x1130, x1131) => 
   val x1132 = x1131.suppliers 
val x1133 = Record1257(x1132) 
x1133 
} 
val x1135 = x1134.distinct 
val M_ctx2 = x1135
//println("M_ctx2")
val x1136 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x1138 = M_ctx2 
val x1139 = PS__D_1 
val x1144 = x1139.map(x1140 => { val x1141 = x1140.ps_suppkey 
val x1142 = x1140.ps_partkey 
val x1143 = Record1258(x1141, x1142) 
x1143 }) 
val x1150 = { val out1 = x1138.map{ case x1145 => ({val x1147 = x1145.lbl 
val x1148 = x1147.p__Fp_partkey 
x1148}, x1145) }
  val out2 = x1144.map{ case x1146 => ({val x1149 = x1146.ps_partkey 
x1149}, x1146) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1151 = S__D_1 
val x1157 = x1151.map(x1152 => { val x1153 = x1152.s_name 
val x1154 = x1152.s_nationkey 
val x1155 = x1152.s_suppkey 
val x1156 = Record1259(x1153, x1154, x1155) 
x1156 }) 
val x1163 = { val out1 = x1150.map{ case (x1158, x1159) => ({val x1161 = x1159.ps_suppkey 
x1161}, (x1158, x1159)) }
  val out2 = x1157.map{ case x1160 => ({val x1162 = x1160.s_suppkey 
x1162}, x1160) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1173 = x1163.flatMap{ case ((x1164, x1165), x1166) => val x1172 = (x1165,x1166) 
x1172 match {
   case (_,null) => Nil 
   case x1171 => List(({val x1167 = (x1164) 
x1167}, {val x1168 = x1166.s_name 
val x1169 = x1166.s_nationkey 
val x1170 = Record1261(x1168, x1169) 
x1170}))
 }
}.groupByLabel() 
val x1178 = x1173.map{ case (x1174, x1175) => 
   val x1176 = (x1175) 
val x1177 = Record1262(x1174, x1176) 
x1177 
} 
val M_flat2 = x1178
//println("M_flat2")
val x1179 = M_flat2
//M_flat2.collect.foreach(println(_))
val x1181 = M_flat1 
val x1185 = x1181.flatMap{ case x1182 => x1182 match {
   case null => List((x1182, null))
   case _ =>
   val x1183 = x1182._2 
x1183 match {
     case x1184 => x1184.map{ case v2 => (x1182, v2) }
  }
 }} 
val x1190 = x1185.map{ case (x1186, x1187) => 
   val x1188 = x1187.customers 
val x1189 = Record1257(x1188) 
x1189 
} 
val x1191 = x1190.distinct 
val M_ctx3 = x1191
//println("M_ctx3")
val x1192 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x1194 = M_ctx3 
val x1195 = L__D_1 
val x1200 = x1195.map(x1196 => { val x1197 = x1196.l_orderkey 
val x1198 = x1196.l_partkey 
val x1199 = Record1263(x1197, x1198) 
x1199 }) 
val x1206 = { val out1 = x1194.map{ case x1201 => ({val x1203 = x1201.lbl 
val x1204 = x1203.p__Fp_partkey 
x1204}, x1201) }
  val out2 = x1200.map{ case x1202 => ({val x1205 = x1202.l_partkey 
x1205}, x1202) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1207 = O__D_1 
val x1212 = x1207.map(x1208 => { val x1209 = x1208.o_custkey 
val x1210 = x1208.o_orderkey 
val x1211 = Record1264(x1209, x1210) 
x1211 }) 
val x1218 = { val out1 = x1206.map{ case (x1213, x1214) => ({val x1216 = x1214.l_orderkey 
x1216}, (x1213, x1214)) }
  val out2 = x1212.map{ case x1215 => ({val x1217 = x1215.o_orderkey 
x1217}, x1215) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1219 = C__D_1 
val x1225 = x1219.map(x1220 => { val x1221 = x1220.c_name 
val x1222 = x1220.c_nationkey 
val x1223 = x1220.c_custkey 
val x1224 = Record1265(x1221, x1222, x1223) 
x1224 }) 
val x1232 = { val out1 = x1218.map{ case ((x1226, x1227), x1228) => ({val x1230 = x1228.o_custkey 
x1230}, ((x1226, x1227), x1228)) }
  val out2 = x1225.map{ case x1229 => ({val x1231 = x1229.c_custkey 
x1231}, x1229) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1243 = x1232.flatMap{ case (((x1233, x1234), x1235), x1236) => val x1242 = (x1234,x1235,x1236) 
x1242 match {
   case (_,null,_) => Nil
case (_,_,null) => Nil 
   case x1241 => List(({val x1237 = (x1233) 
x1237}, {val x1238 = x1236.c_name 
val x1239 = x1236.c_nationkey 
val x1240 = Record1267(x1238, x1239) 
x1240}))
 }
}.groupByLabel() 
val x1248 = x1243.map{ case (x1244, x1245) => 
   val x1246 = (x1245) 
val x1247 = Record1268(x1244, x1246) 
x1247 
} 
val M_flat3 = x1248
//println("M_flat3")
val x1249 = M_flat3
//M_flat3.collect.foreach(println(_))
val res = x1249.count
   var end0 = System.currentTimeMillis() - start0
   println("ShredQuery3Spark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
