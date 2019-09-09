
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record1345(p_name: String, p_partkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1347(p__F: Record1345, S__F: Int, PS__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record1348(p__F: Record1345, C__F: Int, L__F: Int, O__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record1349(p_name: String, suppliers: Record1347, customers: Record1348, uniqueId: Long) extends CaseClassRecord
case class Record1351(ps_suppkey: Int, ps_partkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1353(s_name: String, s_nationkey: Int, s_suppkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1355(s_name: String, s_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1357(l_orderkey: Int, l_partkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1359(o_custkey: Int, o_orderkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1361(c_name: String, c_nationkey: Int, c_custkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record1363(c_name: String, c_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx1(lbl: Q3Flat, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx3(lbl: Record1348, uniqueId: Long) extends CaseClassRecord
case class RecM_flat1(_1: Q3Flat, _2: Iterable[Record1349], uniqueId: Long) extends CaseClassRecord
case class RecM_flat3(_1: Record1348, _2: Iterable[Record1363], uniqueId: Long) extends CaseClassRecord
case class RecM_flat2(_1: Record1347, _2: Iterable[Record1355], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx2(lbl: Record1347, uniqueId: Long) extends CaseClassRecord
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
   val x1178 = Q3Flat(O__F, C__F, PS__F, S__F, L__F, P__F, newId) 
val x1179 = RecM_ctx1(x1178, newId) 
val x1180 = List(x1179) 
val M_ctx1 = x1180.head
val x1183 = M_ctx1 
val x1184 = P__D_1 
val x1189 = x1184.map(x1185 => { val x1186 = x1185.p_name 
val x1187 = x1185.p_partkey 
val x1188 = Record1345(x1186, x1187, newId) 
x1188 }) 
val x1192 = x1189.map{ case c => (x1183, c) } 
val x1208 = x1192.map{ case (x1193, x1194) => val x1207 = (x1194) 
x1207 match {
   case (null) => ({val x1195 = (x1193) 
x1195}, null) 
   case x1206 => ({val x1195 = (x1193) 
x1195}, {val x1196 = x1194.p_name 
val x1197 = x1193.lbl 
val x1198 = x1197.S__F 
val x1199 = x1197.PS__F 
val x1200 = Record1347(x1194, x1198, x1199, newId) 
val x1201 = x1197.C__F 
val x1202 = x1197.L__F 
val x1203 = x1197.O__F 
val x1204 = Record1348(x1194, x1201, x1202, x1203, newId) 
val x1205 = Record1349(x1196, x1200, x1204, newId) 
x1205})
 }
}.groupByLabel() 
val x1213 = x1208.map{ case (x1209, x1210) => 
   val x1211 = x1209.lbl 
val x1212 = RecM_flat1(x1211, x1210, newId) 
x1212 
} 
val M_flat1 = x1213
//println("M_flat1")
val x1214 = M_flat1
//M_flat1.collect.foreach(println(_))
val x1216 = M_flat1 
val x1220 = x1216.flatMap{ case x1217 => x1217 match {
   case null => List((x1217, null))
   case _ =>
   val x1218 = x1217._2 
x1218 match {
     case x1219 => x1219.map{ case v2 => (x1217, v2) }
  }
 }} 
val x1225 = x1220.map{ case (x1221, x1222) => 
   val x1223 = x1222.suppliers 
val x1224 = RecM_ctx2(x1223, newId) 
x1224 
} 
val x1226 = x1225.distinct 
val M_ctx2 = x1226
//println("M_ctx2")
val x1227 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x1229 = M_ctx2 
val x1230 = PS__D_1 
val x1235 = x1230.map(x1231 => { val x1232 = x1231.ps_suppkey 
val x1233 = x1231.ps_partkey 
val x1234 = Record1351(x1232, x1233, newId) 
x1234 }) 
val x1242 = { val out1 = x1229.map{ case x1236 => ({val x1238 = x1236.lbl 
val x1239 = x1238.p__F 
val x1240 = x1239.p_partkey 
x1240}, x1236) }
  val out2 = x1235.map{ case x1237 => ({val x1241 = x1237.ps_partkey 
x1241}, x1237) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1243 = S__D_1 
val x1249 = x1243.map(x1244 => { val x1245 = x1244.s_name 
val x1246 = x1244.s_nationkey 
val x1247 = x1244.s_suppkey 
val x1248 = Record1353(x1245, x1246, x1247, newId) 
x1248 }) 
val x1255 = { val out1 = x1242.map{ case (x1250, x1251) => ({val x1253 = x1251.ps_suppkey 
x1253}, (x1250, x1251)) }
  val out2 = x1249.map{ case x1252 => ({val x1254 = x1252.s_suppkey 
x1254}, x1252) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1265 = x1255.map{ case ((x1256, x1257), x1258) => val x1264 = (x1257,x1258) 
x1264 match {
   case (_,null) => ({val x1259 = (x1256) 
x1259}, null) 
   case x1263 => ({val x1259 = (x1256) 
x1259}, {val x1260 = x1258.s_name 
val x1261 = x1258.s_nationkey 
val x1262 = Record1355(x1260, x1261, newId) 
x1262})
 }
}.groupByLabel() 
val x1270 = x1265.map{ case (x1266, x1267) => 
   val x1268 = x1266.lbl 
val x1269 = RecM_flat2(x1268, x1267, newId) 
x1269 
} 
val M_flat2 = x1270
//println("M_flat2")
val x1271 = M_flat2
//M_flat2.collect.foreach(println(_))
val x1273 = M_flat1 
val x1277 = x1273.flatMap{ case x1274 => x1274 match {
   case null => List((x1274, null))
   case _ =>
   val x1275 = x1274._2 
x1275 match {
     case x1276 => x1276.map{ case v2 => (x1274, v2) }
  }
 }} 
val x1282 = x1277.map{ case (x1278, x1279) => 
   val x1280 = x1279.customers 
val x1281 = RecM_ctx3(x1280, newId) 
x1281 
} 
val x1283 = x1282.distinct 
val M_ctx3 = x1283
//println("M_ctx3")
val x1284 = M_ctx3
//M_ctx3.collect.foreach(println(_))
val x1286 = M_ctx3 
val x1287 = L__D_1 
val x1292 = x1287.map(x1288 => { val x1289 = x1288.l_orderkey 
val x1290 = x1288.l_partkey 
val x1291 = Record1357(x1289, x1290, newId) 
x1291 }) 
val x1299 = { val out1 = x1286.map{ case x1293 => ({val x1295 = x1293.lbl 
val x1296 = x1295.p__F 
val x1297 = x1296.p_partkey 
x1297}, x1293) }
  val out2 = x1292.map{ case x1294 => ({val x1298 = x1294.l_partkey 
x1298}, x1294) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1300 = O__D_1 
val x1305 = x1300.map(x1301 => { val x1302 = x1301.o_custkey 
val x1303 = x1301.o_orderkey 
val x1304 = Record1359(x1302, x1303, newId) 
x1304 }) 
val x1311 = { val out1 = x1299.map{ case (x1306, x1307) => ({val x1309 = x1307.l_orderkey 
x1309}, (x1306, x1307)) }
  val out2 = x1305.map{ case x1308 => ({val x1310 = x1308.o_orderkey 
x1310}, x1308) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1312 = C__D_1 
val x1318 = x1312.map(x1313 => { val x1314 = x1313.c_name 
val x1315 = x1313.c_nationkey 
val x1316 = x1313.c_custkey 
val x1317 = Record1361(x1314, x1315, x1316, newId) 
x1317 }) 
val x1325 = { val out1 = x1311.map{ case ((x1319, x1320), x1321) => ({val x1323 = x1321.o_custkey 
x1323}, ((x1319, x1320), x1321)) }
  val out2 = x1318.map{ case x1322 => ({val x1324 = x1322.c_custkey 
x1324}, x1322) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x1336 = x1325.map{ case (((x1326, x1327), x1328), x1329) => val x1335 = (x1327,x1328,x1329) 
x1335 match {
   case (_,null,_) => ({val x1330 = (x1326) 
x1330}, null)
case (_,_,null) => ({val x1330 = (x1326) 
x1330}, null) 
   case x1334 => ({val x1330 = (x1326) 
x1330}, {val x1331 = x1329.c_name 
val x1332 = x1329.c_nationkey 
val x1333 = Record1363(x1331, x1332, newId) 
x1333})
 }
}.groupByLabel() 
val x1341 = x1336.map{ case (x1337, x1338) => 
   val x1339 = x1337.lbl 
val x1340 = RecM_flat3(x1339, x1338, newId) 
x1340 
} 
val M_flat3 = x1341
//println("M_flat3")
val x1342 = M_flat3
//M_flat3.collect.foreach(println(_))
val res = x1342.count
   var end0 = System.currentTimeMillis() - start0
   println("ShredQuery3Spark"+sf+","+Config.datapath+","+end0)
 }
}
