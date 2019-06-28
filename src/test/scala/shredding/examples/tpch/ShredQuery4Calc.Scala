
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record1207(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record1208(c_name: String, c_orders: Record1207)
case class Record1215(o__F: Orders, P__F: Int, L__F: Int)
case class Record1216(o_orderdate: String, o_parts: Record1215)
case class Record1226(p_name: String, l_qty: Double)
case class Record1310(lbl: Q1Flat2)
case class Input_Q1__DFlat1312(c_name: String, c_orders: Int)
case class Flat1313(o_orderdate: String, o_parts: Int)
case class Dict1313(o_parts: (List[(Int, List[Record1226])], Unit))
case class Input_Q1__DDict1312(c_orders: (List[(Int, List[Flat1313])], Dict1313))
case class Record1326(l_qty: Double)
case class Record1328(c_name: String, p_name: String, month: String, t_qty: Int)
case class Record1329(_1: Q1Flat2, _2: List[Record1328])
case class RecM_flat2(_1: Record1207, _2: List[Record1216])
case class RecM_flat3(_1: Record1215, _2: List[Record1226])
case class RecM_flat1(_1: Q1Flat, _2: List[Record1208])
case class RecM_ctx3(lbl: Record1215)
case class RecM_ctx2(lbl: Record1207)
case class RecM_ctx1(lbl: Q1Flat)

case class Input_Q1_Dict2(o_parts: (List[RecM_flat3], Unit))
case class Input_Q1_Dict1(c_orders: (List[RecM_flat2], Input_Q1_Dict2))
object ShredQuery4Calc {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
val C__F = 1
val C__D = (List((C__F, TPCHLoader.loadCustomer[Customer].toList)), ())
val O__F = 2
val O__D = (List((O__F, TPCHLoader.loadOrders[Orders].toList)), ())
val L__F = 3
val L__D = (List((L__F, TPCHLoader.loadLineitem[Lineitem].toList)), ())
val P__F = 4
val P__D = (List((P__F, TPCHLoader.loadPart[Part].toList)), ())
    val Q1 = { val x1088 = Q1Flat(P__F, C__F, L__F, O__F)
val x1089 = RecM_ctx1(x1088)
val x1090 = List(x1089)
val M_ctx1 = x1090
val x1091 = M_ctx1
val x1114 = M_ctx1.flatMap(x1092 => { 
    val x1093 = x1092.lbl
    val x1094 = C__D._1
    val x1111 = x1094.flatMap(x1095 => { 
      if({val x1096 = x1092.lbl
      val x1097 = x1096.C__F
      val x1098 = x1095._1
      val x1099 = x1097 == x1098
      x1099}) {  val x1100 = x1095._2
        val x1110 = x1100.flatMap(x1101 => { 
            val x1102 = x1101.c_name
            val x1103 = x1092.lbl
            val x1104 = x1103.P__F
            val x1105 = x1103.L__F
            val x1106 = x1103.O__F
            val x1107 = Record1207(x1104, x1105, x1106, x1101)
            val x1108 = Record1208(x1102, x1107)
            val x1109 = List(x1108)
            x1109})
        x1110} else {  Nil}})
    val x1112 = RecM_flat1(x1093, x1111)
    val x1113 = List(x1112)
    x1113})
val M_flat1 = x1114
val x1115 = M_flat1
val x1123 = M_flat1.flatMap(x1116 => { 
    val x1117 = x1116._2
    val x1122 = x1117.flatMap(x1118 => { 
        val x1119 = x1118.c_orders
        val x1120 = RecM_ctx2(x1119)
        val x1121 = List(x1120)
        x1121})
    x1122})
val x1124 = x1123.distinct
val M_ctx2 = x1124
val x1125 = M_ctx2
val x1152 = M_ctx2.flatMap(x1126 => { 
    val x1127 = x1126.lbl
    val x1128 = O__D._1
    val x1149 = x1128.flatMap(x1129 => { 
      if({val x1130 = x1126.lbl
      val x1131 = x1130.O__F
      val x1132 = x1129._1
      val x1133 = x1131 == x1132
      x1133}) {  val x1134 = x1129._2
        val x1148 = x1134.flatMap(x1135 => { 
          if({val x1136 = x1135.o_custkey
          val x1137 = x1126.lbl
          val x1138 = x1137.c__F
          val x1139 = x1138.c_custkey
          val x1140 = x1136 == x1139
          x1140}) {  val x1141 = x1135.o_orderdate
            val x1142 = x1126.lbl
            val x1143 = x1142.P__F
            val x1144 = x1142.L__F
            val x1145 = Record1215(x1135, x1143, x1144)
            val x1146 = Record1216(x1141, x1145)
            val x1147 = List(x1146)
            x1147} else {  Nil}})
        x1148} else {  Nil}})
    val x1150 = RecM_flat2(x1127, x1149)
    val x1151 = List(x1150)
    x1151})
val M_flat2 = x1152
val x1153 = M_flat2
val x1161 = M_flat2.flatMap(x1154 => { 
    val x1155 = x1154._2
    val x1160 = x1155.flatMap(x1156 => { 
        val x1157 = x1156.o_parts
        val x1158 = RecM_ctx3(x1157)
        val x1159 = List(x1158)
        x1159})
    x1160})
val x1162 = x1161.distinct
val M_ctx3 = x1162
val x1163 = M_ctx3
val x1200 = M_ctx3.flatMap(x1164 => { 
    val x1165 = x1164.lbl
    val x1166 = L__D._1
    val x1197 = x1166.flatMap(x1167 => { 
      if({val x1168 = x1164.lbl
      val x1169 = x1168.L__F
      val x1170 = x1167._1
      val x1171 = x1169 == x1170
      x1171}) {  val x1172 = x1167._2
        val x1196 = x1172.flatMap(x1173 => { 
          if({val x1174 = x1173.l_orderkey
          val x1175 = x1164.lbl
          val x1176 = x1175.o__F
          val x1177 = x1176.o_orderkey
          val x1178 = x1174 == x1177
          x1178}) {  val x1179 = P__D._1
            val x1195 = x1179.flatMap(x1180 => { 
              if({val x1181 = x1164.lbl
              val x1182 = x1181.P__F
              val x1183 = x1180._1
              val x1184 = x1182 == x1183
              x1184}) {  val x1185 = x1180._2
                val x1194 = x1185.flatMap(x1186 => { 
                  if({val x1187 = x1173.l_partkey
                  val x1188 = x1186.p_partkey
                  val x1189 = x1187 == x1188
                  x1189}) {  val x1190 = x1186.p_name
                    val x1191 = x1173.l_quantity
                    val x1192 = Record1226(x1190, x1191)
                    val x1193 = List(x1192)
                    x1193} else {  Nil}})
                x1194} else {  Nil}})
            x1195} else {  Nil}})
        x1196} else {  Nil}})
    val x1198 = RecM_flat3(x1165, x1197)
    val x1199 = List(x1198)
    x1199})
val M_flat3 = x1200
val x1201 = M_flat3
val x1202 = (x1091,x1115,x1125,x1153,x1163,x1201)
x1202 }
    var end0 = System.currentTimeMillis() - start0
    
val Q1__F = Q1._1.head.lbl
val Q1__D = (Q1._2, Input_Q1_Dict1((Q1._4, Input_Q1_Dict2((Q1._6, Unit)))))
    def f(){
      val x1238 = Q1Flat2(Q1__F)
val x1239 = Record1310(x1238)
val x1240 = List(x1239)
val M_ctx1 = x1240
val x1241 = M_ctx1
val x1307 = M_ctx1.flatMap(x1242 => { 
    val x1243 = x1242.lbl
    val x1244 = Q1__D._1
    val x1304 = x1244.flatMap(x1245 => { 
      if({val x1246 = x1242.lbl
      val x1247 = x1246.Q1__F
      val x1248 = x1245._1
      val x1249 = x1247 == x1248
      x1249}) {  val x1250 = x1245._2
        val x1303 = x1250.flatMap(x1251 => { 
            val x1252 = Q1__D._2
            val x1253 = x1252.c_orders
            val x1254 = x1253._1
            val x1302 = x1254.flatMap(x1255 => { 
              if({val x1256 = x1251.c_orders
              val x1257 = x1255._1
              val x1258 = x1256 == x1257
              x1258}) {  val x1259 = x1255._2
                val x1301 = x1259.flatMap(x1260 => { 
                    val x1261 = Q1__D._2
                    val x1262 = x1261.c_orders
                    val x1263 = x1262._2
                    val x1264 = x1263.o_parts
                    val x1265 = x1264._1
                    val x1300 = x1265.flatMap(x1266 => { 
                      if({val x1267 = x1260.o_parts
                      val x1268 = x1266._1
                      val x1269 = x1267 == x1268
                      x1269}) {  val x1270 = x1266._2
                        val x1299 = x1270.flatMap(x1271 => { 
                            val x1272 = x1251.c_name
                            val x1273 = x1271.p_name
                            val x1274 = x1260.o_orderdate
                            val x1275 = Q1__D._2
                            val x1276 = x1275.c_orders
                            val x1277 = x1276._2
                            val x1278 = x1277.o_parts
                            val x1279 = x1278._1
                            val x1296 = x1279.foldLeft(0)((acc1324, x1280) => 
                              if({val x1281 = x1260.o_parts
                              val x1282 = x1280._1
                              val x1283 = x1281 == x1282
                              x1283}) {  acc1324 + {val x1284 = x1280._2
                                val x1295 = x1284.foldLeft(0)((acc1325, x1285) => 
                                  if({val x1286 = x1285.p_name
                                  val x1287 = x1271.p_name
                                  val x1288 = x1286 == x1287
                                  x1288}) {  acc1325 + {val x1289 = x1271.l_qty
                                    val x1290 = Record1326(x1289)
                                    val x1291 = x1285.l_qty
                                    val x1292 = (1 to x1291.asInstanceOf[Int]).map(v => x1290)
                                    val x1294 = x1292.foldLeft(0)((acc1327, x1293) => 
                                        acc1327 + {1})
                                    x1294}} else {  acc1325})
                                x1295}} else {  acc1324})
                            val x1297 = Record1328(x1272, x1273, x1274, x1296)
                            val x1298 = List(x1297)
                            x1298})
                        x1299} else {  Nil}})
                    x1300})
                x1301} else {  Nil}})
            x1302})
        x1303} else {  Nil}})
    val x1305 = Record1329(x1243, x1304)
    val x1306 = List(x1305)
    x1306})
val M_flat1 = x1307
val x1308 = M_flat1
val x1309 = (x1241,x1308)
x1309
    }
    var time = List[Long]()
    for (i <- 1 to 5) {  
     var start = System.currentTimeMillis()
      f
      var end = System.currentTimeMillis() - start
      time = time :+ end
    }
    val avg = (time.sum/5)
    println(end0+","+avg)
 }
}
