
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record1168(s_name: String, s_suppkey: Int)
case class Record1169(s__Fs_suppkey: Int)
case class Record1170(s_name: String, customers2: Record1169)
case class Record1171(l_suppkey: Int, l_orderkey: Int)
case class Record1172(o_custkey: Int, o_orderkey: Int)
case class Record1173(c_name: String, c_custkey: Int)
case class Record1175(c_name2: String)
//case class Record1176(l__Fl_suppkey: Int)
case class Record1177(_1: Record1169, _2: Iterable[Record1175])
case class Record1216(c_name: String)
case class Record1217(c__Fc_name: String)
case class Record1218(c_name: String, suppliers: Record1217)
case class Record1219(co2__Fc_name2: String)
case class Record1220(s_name: String)
case class Record1221(_1: Record1219, _2: Iterable[Record1220])
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

   val x1115 = S__D_1.map(x1111 => { val x1112 = x1111.s_name 
val x1113 = x1111.s_suppkey 
val x1114 = Record1168(x1112, x1113) 
x1114 }) 
val x1121 = x1115.map{ case x1116 => 
   val x1117 = x1116.s_name 
val x1118 = x1116.s_suppkey 
val x1119 = Record1169(x1118) 
val x1120 = Record1170(x1117, x1119) 
x1120 
} 
val M_flat1 = x1121
val x1122 = M_flat1
//M_flat1.collect.foreach(println(_))
val x1127 = L__D_1.map(x1123 => { val x1124 = x1123.l_suppkey 
val x1125 = x1123.l_orderkey 
val x1126 = Record1171(x1124, x1125) 
x1126 }) 
val x1128 = O__D_1 
val x1133 = x1128.map(x1129 => { val x1130 = x1129.o_custkey 
val x1131 = x1129.o_orderkey 
val x1132 = Record1172(x1130, x1131) 
x1132 }) 
val x1138 = { val out1 = x1127.map{ case x1134 => ({val x1136 = x1134.l_orderkey 
x1136}, x1134) }
  val out2 = x1133.map{ case x1135 => ({val x1137 = x1135.o_orderkey 
x1137}, x1135) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1139 = C__D_1 
val x1144 = x1139.map(x1140 => { val x1141 = x1140.c_name 
val x1142 = x1140.c_custkey 
val x1143 = Record1173(x1141, x1142) 
x1143 }) 
val x1150 = { val out1 = x1138.map{ case (x1145, x1146) => ({val x1148 = x1146.o_custkey 
x1148}, (x1145, x1146)) }
  val out2 = x1144.map{ case x1147 => ({val x1149 = x1147.c_custkey 
x1149}, x1147) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1159 = x1150.flatMap{ case ((x1151, x1152), x1153) => val x1158 = (x1152,x1153) 
x1158 match {
   case (_,null) => Nil 
   case x1157 => List(({val x1154 = (x1151) 
x1154}, {val x1155 = x1153.c_name 
val x1156 = Record1175(x1155) 
x1156}))
 }
}.groupByKey() 
val x1165 = x1159.map{ case (x1160, x1161) => 
   val x1162 = x1160.l_suppkey 
val x1163 = Record1169(x1162) 
val x1164 = Record1177(x1163, x1161) 
x1164 
} 
val M_flat2 = x1165
val x1166 = M_flat2
//M_flat2.collect.foreach(println(_))
val Query2Full__D_1 = M_flat1
Query2Full__D_1.cache
Query2Full__D_1.count
val Query2Full__D_2customers2_1 = M_flat2
Query2Full__D_2customers2_1.cache
Query2Full__D_2customers2_1.count
def f = { 
 val x1187 = C__D_1.map(x1184 => { val x1185 = x1184.c_name 
val x1186 = Record1216(x1185) 
x1186 }) 
val x1192 = x1187.map{ case x1188 => 
   val x1189 = x1188.c_name 
val x1190 = Record1217(x1189) 
val x1191 = Record1218(x1189, x1190) 
x1191 
} 
val M_flat1 = x1192
val x1193 = M_flat1
//M_flat1.collect.foreach(println(_))
val x1195 = Query2Full__D_1 
val x1197 = Query2Full__D_2customers2_1 
val x1200 = x1197 
val x1204 = { val out1 = x1195.map{ case x1201 => ({val x1203 = x1201.customers2 
x1203}, x1201) }
  val out2 = x1200.flatMap(x1202 => x1202._2.map{case v2 => (x1202._1, v2)})
  out1.join(out2).map{ case (k, v) => v }
} 
val x1213 = x1204.map{ case (x1205, x1206) => 
   val x1207 = x1206.c_name2 
val x1208 = Record1219(x1207) 
val x1209 = x1205.s_name 
val x1210 = Record1220(x1209) 
val x1211 = List(x1210) 
val x1212 = Record1221(x1208, x1211) 
x1212 
} 
val M_flat2 = x1213
val x1214 = M_flat2
M_flat2.collect.foreach(println(_))
x1214.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery6FullSpark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
