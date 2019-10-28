
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record1139(s_name: String, s_suppkey: Int)
case class Record1140(s__Fs_suppkey: Int)
case class Record1141(s_name: String, customers2: Record1140)
case class Record1142(c_name: String, c_custkey: Int)
case class Record1143(o_orderkey: Int, o_custkey: Int)
case class Record1144(l_suppkey: Int, l_orderkey: Int)
case class Record1145(c_name2: String)
case class Record1183(c_name: String)
case class Record1184(c__Fc_name: String)
case class Record1185(c_name: String, suppliers: Record1184)
case class Record1186(s_name: String)
case class Record1187(_1: String, _2: Iterable[Record1186])
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

   val Query2Full = {
 val x1093 = S__D_1.map(x1089 => { val x1090 = x1089.s_name 
val x1091 = x1089.s_suppkey 
val x1092 = Record1139(x1090, x1091) 
x1092 }) 
val x1099 = x1093.map{ case x1094 => 
   val x1095 = x1094.s_name 
val x1096 = x1094.s_suppkey 
val x1097 = Record1140(x1096) 
val x1098 = Record1141(x1095, x1097) 
x1098 
} 
val M_flat1 = x1099
val x1100 = M_flat1
//M_flat1.collect.foreach(println(_))
val x1105 = C__D_1.map(x1101 => { val x1102 = x1101.c_name 
val x1103 = x1101.c_custkey 
val x1104 = Record1142(x1102, x1103) 
x1104 }) 
val x1106 = O__D_1 
val x1111 = x1106.map(x1107 => { val x1108 = x1107.o_orderkey 
val x1109 = x1107.o_custkey 
val x1110 = Record1143(x1108, x1109) 
x1110 }) 
val x1116 = { val out1 = x1105.map{ case x1112 => ({val x1114 = x1112.c_custkey 
x1114}, x1112) }
  val out2 = x1111.map{ case x1113 => ({val x1115 = x1113.o_custkey 
x1115}, x1113) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1117 = L__D_1 
val x1122 = x1117.map(x1118 => { val x1119 = x1118.l_suppkey 
val x1120 = x1118.l_orderkey 
val x1121 = Record1144(x1119, x1120) 
x1121 }) 
val x1130 = { val out1 = x1116.map{ case (x1123, x1124) => ({val x1126 = x1124.o_orderkey 
x1126}, (x1123, x1124)) }
  val out2 = x1122.map{ case x1125 => ({val x1127 = x1125.l_suppkey 
val x1128 = x1125.l_orderkey 
val x1129 = (x1127,x1128) 
x1129}, x1125) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1136 = x1130.map{ case ((x1131, x1132), x1133) => 
   val x1134 = x1131.c_name 
val x1135 = Record1145(x1134) 
x1135 
} 
val M_flat2 = x1136
val x1137 = M_flat2
//M_flat2.collect.foreach(println(_))
x1137
}
Query2Full.cache
Query2Full.count
def f = { 
 val x1155 = C__D_1.map(x1152 => { val x1153 = x1152.c_name 
val x1154 = Record1183(x1153) 
x1154 }) 
val x1160 = x1155.map{ case x1156 => 
   val x1157 = x1156.c_name 
val x1158 = Record1184(x1157) 
val x1159 = Record1185(x1157, x1158) 
x1159 
} 
val M_flat1 = x1160
val x1161 = M_flat1
//M_flat1.collect.foreach(println(_))
val x1163 = Query2Full__D_1 
val x1165 = Query2Full__D_2customers2_1 
val x1168 = x1165 
val x1172 = { val out1 = x1163.map{ case x1169 => ({val x1171 = x1169.customers2 
x1171}, x1169) }
  val out2 = x1168.flatMap(x1170 => x1170._2.map{case v2 => (x1170._1.lbl, v2)})
  out1.join(out2).map{ case (k, v) => v }
} 
val x1180 = x1172.map{ case (x1173, x1174) => 
   val x1175 = x1174.c_name2 
val x1176 = x1173.s_name 
val x1177 = Record1186(x1176) 
val x1178 = List(x1177) 
val x1179 = Record1187(x1175, x1178) 
x1179 
} 
val M_flat2 = x1180
val x1181 = M_flat2
//M_flat2.collect.foreach(println(_))
x1181.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery6FullSpark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
