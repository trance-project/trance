
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record1186(c_name: String, c_custkey: Int)
case class Record1187(o_orderkey: Int, o_custkey: Int)
case class Record1188(o_orderkey: Int, c_name: String)
case class Record1189(s_name: String, s_suppkey: Int)
case class Record1190(s__Fs_suppkey: Int)
case class Record1191(s_name: String, customers2: Record1190)
case class Record1192(l_suppkey: Int, l_orderkey: Int)
case class Record1194(c_name2: String)
//case class Record1195(l__Fl_suppkey: Int)
case class Record1196(_1: Record1190, _2: Iterable[Record1194])
case class Record1235(c_name: String)
case class Record1236(c__Fc_name: String)
case class Record1237(c_name: String, suppliers: Record1236)
case class Record1238(co2__Fc_name2: String)
case class Record1239(s_name: String)
case class Record1240(_1: Record1238, _2: Iterable[Record1239])
object ShredQuery6FullSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6FullSpark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
val L__F = 3
val L__D_1 = tpch.loadLineitem
L__D_1.cache
L__D_1.count
val C__F = 1
val C__D_1 = tpch.loadCustomers
C__D_1.cache
C__D_1.count
val O__F = 2
val O__D_1 = tpch.loadOrders
O__D_1.cache
O__D_1.count
val S__F = 6
val S__D_1 = tpch.loadSupplier
S__D_1.cache
S__D_1.count

   val x1126 = C__D_1.map(x1122 => { val x1123 = x1122.c_name 
val x1124 = x1122.c_custkey 
val x1125 = Record1186(x1123, x1124) 
x1125 }) 
val x1127 = O__D_1 
val x1132 = x1127.map(x1128 => { val x1129 = x1128.o_orderkey 
val x1130 = x1128.o_custkey 
val x1131 = Record1187(x1129, x1130) 
x1131 }) 
val x1137 = { val out1 = x1126.map{ case x1133 => ({val x1135 = x1133.c_custkey 
x1135}, x1133) }
  val out2 = x1132.map{ case x1134 => ({val x1136 = x1134.o_custkey 
x1136}, x1134) }
  out1.joinSkewLeft(out2).map{ case (k,v) => v }
} 
val x1143 = x1137.map{ case (x1138, x1139) => 
   val x1140 = x1139.o_orderkey 
val x1141 = x1138.c_name 
val x1142 = Record1188(x1140, x1141) 
x1142 
} 
val resultInner__D_1 = x1143
val x1144 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val x1149 = S__D_1.map(x1145 => { val x1146 = x1145.s_name 
val x1147 = x1145.s_suppkey 
val x1148 = Record1189(x1146, x1147) 
x1148 }) 
val x1155 = x1149.map{ case x1150 => 
   val x1151 = x1150.s_name 
val x1152 = x1150.s_suppkey 
val x1153 = Record1190(x1152) 
val x1154 = Record1191(x1151, x1153) 
x1154 
} 
val M_flat1 = x1155
val x1156 = M_flat1
//M_flat1.collect.foreach(println(_))
val x1161 = L__D_1.map(x1157 => { val x1158 = x1157.l_suppkey 
val x1159 = x1157.l_orderkey 
val x1160 = Record1192(x1158, x1159) 
x1160 }) 
val x1162 = resultInner__D_1 
val x1164 = x1162 
val x1169 = { val out1 = x1161.map{ case x1165 => ({val x1167 = x1165.l_orderkey 
x1167}, x1165) }
  val out2 = x1164.map{ case x1166 => ({val x1168 = x1166.o_orderkey 
x1168}, x1166) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1177 = x1169.flatMap{ case (x1170, x1171) => val x1176 = (x1171) 
x1176 match {
   case (null) => Nil 
   case x1175 => List(({val x1172 = (x1170) 
x1172}, {val x1173 = x1171.c_name 
val x1174 = Record1194(x1173) 
x1174}))
 }
}.groupByKey() 
val x1183 = x1177.map{ case (x1178, x1179) => 
   val x1180 = x1178.l_suppkey 
val x1181 = Record1190(x1180) 
val x1182 = Record1196(x1181, x1179) 
x1182 
} 
val M_flat2 = x1183
val x1184 = M_flat2
//M_flat2.collect.foreach(println(_))
val Query2Full__D_1 = M_flat1
Query2Full__D_1.cache
Query2Full__D_1.count
val Query2Full__D_2customers2_1 = M_flat2
Query2Full__D_2customers2_1.cache
Query2Full__D_2customers2_1.count
def f = { 
 val x1206 = C__D_1.map(x1203 => { val x1204 = x1203.c_name 
val x1205 = Record1235(x1204) 
x1205 }) 
val x1211 = x1206.map{ case x1207 => 
   val x1208 = x1207.c_name 
val x1209 = Record1236(x1208) 
val x1210 = Record1237(x1208, x1209) 
x1210 
} 
val M_flat1 = x1211
val x1212 = M_flat1
M_flat1.count
//M_flat1.collect.foreach(println(_))
val x1214 = Query2Full__D_1 
val x1216 = Query2Full__D_2customers2_1 
val x1219 = x1216 
val x1223 = { val out1 = x1214.map{ case x1220 => ({val x1222 = x1220.customers2 
x1222}, x1220) }
  val out2 = x1219.flatMap(x1221 => x1221._2.map{case v2 => (x1221._1, v2)})
  out2.joinSkewLeft(out1).map{ case (k, v) => v }
} 
val x1232 = x1223.map{ case (x1225, x1224) => 
   val x1226 = x1225.c_name2 
val x1227 = Record1238(x1226) 
val x1228 = x1224.s_name 
val x1229 = Record1239(x1228) 
val x1230 = List(x1229) 
val x1231 = Record1240(x1227, x1230) 
x1231 
} 
val M_flat2 = x1232
val x1233 = M_flat2
//M_flat2.collect.foreach(println(_))
x1233.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery6FullSpark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
