
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record1166(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record1167(c_name: String, c_orders: Record1166)
case class Record1174(o__F: Orders, P__F: Int, L__F: Int)
case class Record1175(o_orderdate: String, o_parts: Record1174)
case class Record1185(p_name: String, l_qty: Double)
case class Record1262(lbl: Q1Flat2)
case class Input_Query4__DFlat1264(c_name: String, c_orders: Int)
case class Flat1265(o_orderdate: String, o_parts: Int)
case class Dict1265(o_parts: (List[(Int, List[Record1185])], Unit))
case class Input_Query4__DDict1264(c_orders: (List[(Int, List[Flat1265])], Dict1265))
case class Record1278(c_name: String, p_name: String, month: String, t_qty: Double)
case class Record1279(_1: Q1Flat2, _2: List[Record1278])
case class RecM_flat2(_1: Record1166, _2: List[Record1175])
case class RecM_flat3(_1: Record1174, _2: List[Record1185])
case class RecM_flat1(_1: Q1Flat, _2: List[Record1167])
case class RecM_ctx3(lbl: Record1174)
case class RecM_ctx2(lbl: Record1166)
case class RecM_ctx1(lbl: Q1Flat)
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
    val ShredQuery4 = { val x1047 = Q1Flat(P__F, C__F, L__F, O__F)
val x1048 = RecM_ctx1(x1047)
val x1049 = List(x1048)
val M_ctx1 = x1049
val x1050 = M_ctx1
val x1073 = M_ctx1.flatMap(x1051 => { 
    val x1052 = x1051.lbl
    val x1053 = C__D._1
    val x1070 = x1053.flatMap(x1054 => { 
      if({val x1055 = x1051.lbl
      val x1056 = x1055.C__F
      val x1057 = x1054._1
      val x1058 = x1056 == x1057
      x1058}) {  val x1059 = x1054._2
        val x1069 = x1059.flatMap(x1060 => { 
            val x1061 = x1060.c_name
            val x1062 = x1051.lbl
            val x1063 = x1062.P__F
            val x1064 = x1062.L__F
            val x1065 = x1062.O__F
            val x1066 = Record1166(x1063, x1064, x1065, x1060)
            val x1067 = Record1167(x1061, x1066)
            val x1068 = List(x1067)
            x1068})
        x1069} else {  Nil}})
    val x1071 = RecM_flat1(x1052, x1070)
    val x1072 = List(x1071)
    x1072})
val M_flat1 = x1073
val x1074 = M_flat1
val x1082 = M_flat1.flatMap(x1075 => { 
    val x1076 = x1075._2
    val x1081 = x1076.flatMap(x1077 => { 
        val x1078 = x1077.c_orders
        val x1079 = RecM_ctx2(x1078)
        val x1080 = List(x1079)
        x1080})
    x1081})
val x1083 = x1082.distinct
val M_ctx2 = x1083
val x1084 = M_ctx2
val x1111 = M_ctx2.flatMap(x1085 => { 
    val x1086 = x1085.lbl
    val x1087 = O__D._1
    val x1108 = x1087.flatMap(x1088 => { 
      if({val x1089 = x1085.lbl
      val x1090 = x1089.O__F
      val x1091 = x1088._1
      val x1092 = x1090 == x1091
      x1092}) {  val x1093 = x1088._2
        val x1107 = x1093.flatMap(x1094 => { 
          if({val x1095 = x1094.o_custkey
          val x1096 = x1085.lbl
          val x1097 = x1096.c__F
          val x1098 = x1097.c_custkey
          val x1099 = x1095 == x1098
          x1099}) {  val x1100 = x1094.o_orderdate
            val x1101 = x1085.lbl
            val x1102 = x1101.P__F
            val x1103 = x1101.L__F
            val x1104 = Record1174(x1094, x1102, x1103)
            val x1105 = Record1175(x1100, x1104)
            val x1106 = List(x1105)
            x1106} else {  Nil}})
        x1107} else {  Nil}})
    val x1109 = RecM_flat2(x1086, x1108)
    val x1110 = List(x1109)
    x1110})
val M_flat2 = x1111
val x1112 = M_flat2
val x1120 = M_flat2.flatMap(x1113 => { 
    val x1114 = x1113._2
    val x1119 = x1114.flatMap(x1115 => { 
        val x1116 = x1115.o_parts
        val x1117 = RecM_ctx3(x1116)
        val x1118 = List(x1117)
        x1118})
    x1119})
val x1121 = x1120.distinct
val M_ctx3 = x1121
val x1122 = M_ctx3
val x1159 = M_ctx3.flatMap(x1123 => { 
    val x1124 = x1123.lbl
    val x1125 = L__D._1
    val x1156 = x1125.flatMap(x1126 => { 
      if({val x1127 = x1123.lbl
      val x1128 = x1127.L__F
      val x1129 = x1126._1
      val x1130 = x1128 == x1129
      x1130}) {  val x1131 = x1126._2
        val x1155 = x1131.flatMap(x1132 => { 
          if({val x1133 = x1132.l_orderkey
          val x1134 = x1123.lbl
          val x1135 = x1134.o__F
          val x1136 = x1135.o_orderkey
          val x1137 = x1133 == x1136
          x1137}) {  val x1138 = P__D._1
            val x1154 = x1138.flatMap(x1139 => { 
              if({val x1140 = x1123.lbl
              val x1141 = x1140.P__F
              val x1142 = x1139._1
              val x1143 = x1141 == x1142
              x1143}) {  val x1144 = x1139._2
                val x1153 = x1144.flatMap(x1145 => { 
                  if({val x1146 = x1132.l_partkey
                  val x1147 = x1145.p_partkey
                  val x1148 = x1146 == x1147
                  x1148}) {  val x1149 = x1145.p_name
                    val x1150 = x1132.l_quantity
                    val x1151 = Record1185(x1149, x1150)
                    val x1152 = List(x1151)
                    x1152} else {  Nil}})
                x1153} else {  Nil}})
            x1154} else {  Nil}})
        x1155} else {  Nil}})
    val x1157 = RecM_flat3(x1124, x1156)
    val x1158 = List(x1157)
    x1158})
val M_flat3 = x1159
val x1160 = M_flat3
val x1161 = (x1050,x1074,x1084,x1112,x1122,x1160)
x1161 }
    var end0 = System.currentTimeMillis() - start0
    
case class Input_Q1_Dict2(o_parts: (List[RecM_flat3], Unit))
case class Input_Q1_Dict1(c_orders: (List[RecM_flat2], Input_Q1_Dict2))
val Query4__F = ShredQuery4._1.head.lbl
val Query4__D = (ShredQuery4._2, Input_Q1_Dict1((ShredQuery4._4, Input_Q1_Dict2((ShredQuery4._6, Unit)))))
    def f(){
      val x1195 = Q1Flat2(Query4__F)
val x1196 = Record1262(x1195)
val x1197 = List(x1196)
val M_ctx1 = x1197
val x1198 = M_ctx1
val x1259 = M_ctx1.flatMap(x1199 => { 
    val x1200 = x1199.lbl
    val x1201 = Query4__D._1
    val x1256 = x1201.flatMap(x1202 => { 
      if({val x1203 = x1199.lbl
      val x1204 = x1203.Query4__F
      val x1205 = x1202._1
      val x1206 = x1204 == x1205
      x1206}) {  val x1207 = x1202._2
        val x1255 = x1207.flatMap(x1208 => { 
            val x1209 = Query4__D._2
            val x1210 = x1209.c_orders
            val x1211 = x1210._1
            val x1254 = x1211.flatMap(x1212 => { 
              if({val x1213 = x1208.c_orders
              val x1214 = x1212._1
              val x1215 = x1213 == x1214
              x1215}) {  val x1216 = x1212._2
                val x1253 = x1216.flatMap(x1217 => { 
                    val x1218 = Query4__D._2
                    val x1219 = x1218.c_orders
                    val x1220 = x1219._2
                    val x1221 = x1220.o_parts
                    val x1222 = x1221._1
                    val x1252 = x1222.flatMap(x1223 => { 
                      if({val x1224 = x1217.o_parts
                      val x1225 = x1223._1
                      val x1226 = x1224 == x1225
                      x1226}) {  val x1227 = x1223._2
                        val x1251 = x1227.flatMap(x1228 => { 
                            val x1229 = x1208.c_name
                            val x1230 = x1228.p_name
                            val x1231 = x1217.o_orderdate
                            val x1232 = Query4__D._2
                            val x1233 = x1232.c_orders
                            val x1234 = x1233._2
                            val x1235 = x1234.o_parts
                            val x1236 = x1235._1
                            val x1248 = x1236.foldLeft(0.0)((acc1276, x1237) => 
                              if({val x1238 = x1217.o_parts
                              val x1239 = x1237._1
                              val x1240 = x1238 == x1239
                              x1240}) {  acc1276 + {val x1241 = x1237._2
                                val x1247 = x1241.foldLeft(0.0)((acc1277, x1242) => 
                                  if({val x1243 = x1242.p_name
                                  val x1244 = x1228.p_name
                                  val x1245 = x1243 == x1244
                                  x1245}) {  acc1277 + {val x1246 = x1242.l_qty
                                    x1246}} else {  acc1277})
                                x1247}} else {  acc1276})
                            val x1249 = Record1278(x1229, x1230, x1231, x1248)
                            val x1250 = List(x1249)
                            x1250})
                        x1251} else {  Nil}})
                    x1252})
                x1253} else {  Nil}})
            x1254})
        x1255} else {  Nil}})
    val x1257 = Record1279(x1200, x1256)
    val x1258 = List(x1257)
    x1258})
val M_flat1 = x1259
val x1260 = M_flat1
val x1261 = (x1198,x1260)
x1261
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
