
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record1162(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record1163(c_name: String, c_orders: Record1162)
case class Record1170(o__F: Orders, P__F: Int, L__F: Int)
case class Record1171(o_orderdate: String, o_parts: Record1170)
case class Record1181(p_name: String, l_qty: Double)
case class Record1258(lbl: Q1Flat2)
case class Input_Query4__DFlat1260(c_name: String, c_orders: Int)
case class Flat1261(o_orderdate: String, o_parts: Int)
case class Dict1261(o_parts: (List[(Int, List[Record1181])], Unit))
case class Input_Query4__DDict1260(c_orders: (List[(Int, List[Flat1261])], Dict1261))
case class Record1274(c_name: String, p_name: String, month: String, t_qty: Double)
case class Record1275(_1: Q1Flat2, _2: List[Record1274])
case class RecM_flat2(_1: Record1162, _2: List[Record1171])
case class RecM_flat3(_1: Record1170, _2: List[Record1181])
case class RecM_flat1(_1: Q1Flat, _2: List[Record1163])
case class RecM_ctx3(lbl: Record1170)
case class RecM_ctx2(lbl: Record1162)
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
    val ShredQuery4 = { val x1043 = Q1Flat(P__F, C__F, L__F, O__F)
val x1044 = RecM_ctx1(x1043)
val x1045 = List(x1044)
val M_ctx1 = x1045
val x1046 = M_ctx1
val x1069 = M_ctx1.flatMap(x1047 => { 
    val x1048 = x1047.lbl
    val x1049 = C__D._1
    val x1066 = x1049.flatMap(x1050 => { 
      if({val x1051 = x1047.lbl
      val x1052 = x1051.C__F
      val x1053 = x1050._1
      val x1054 = x1052 == x1053
      x1054}) {  val x1055 = x1050._2
        val x1065 = x1055.flatMap(x1056 => { 
            val x1057 = x1056.c_name
            val x1058 = x1047.lbl
            val x1059 = x1058.P__F
            val x1060 = x1058.L__F
            val x1061 = x1058.O__F
            val x1062 = Record1162(x1059, x1060, x1061, x1056)
            val x1063 = Record1163(x1057, x1062)
            val x1064 = List(x1063)
            x1064})
        x1065} else {  Nil}})
    val x1067 = RecM_flat1(x1048, x1066)
    val x1068 = List(x1067)
    x1068})
val M_flat1 = x1069
val x1070 = M_flat1
val x1078 = M_flat1.flatMap(x1071 => { 
    val x1072 = x1071._2
    val x1077 = x1072.flatMap(x1073 => { 
        val x1074 = x1073.c_orders
        val x1075 = RecM_ctx2(x1074)
        val x1076 = List(x1075)
        x1076})
    x1077})
val x1079 = x1078.distinct
val M_ctx2 = x1079
val x1080 = M_ctx2
val x1107 = M_ctx2.flatMap(x1081 => { 
    val x1082 = x1081.lbl
    val x1083 = O__D._1
    val x1104 = x1083.flatMap(x1084 => { 
      if({val x1085 = x1081.lbl
      val x1086 = x1085.O__F
      val x1087 = x1084._1
      val x1088 = x1086 == x1087
      x1088}) {  val x1089 = x1084._2
        val x1103 = x1089.flatMap(x1090 => { 
          if({val x1091 = x1090.o_custkey
          val x1092 = x1081.lbl
          val x1093 = x1092.c__F
          val x1094 = x1093.c_custkey
          val x1095 = x1091 == x1094
          x1095}) {  val x1096 = x1090.o_orderdate
            val x1097 = x1081.lbl
            val x1098 = x1097.P__F
            val x1099 = x1097.L__F
            val x1100 = Record1170(x1090, x1098, x1099)
            val x1101 = Record1171(x1096, x1100)
            val x1102 = List(x1101)
            x1102} else {  Nil}})
        x1103} else {  Nil}})
    val x1105 = RecM_flat2(x1082, x1104)
    val x1106 = List(x1105)
    x1106})
val M_flat2 = x1107
val x1108 = M_flat2
val x1116 = M_flat2.flatMap(x1109 => { 
    val x1110 = x1109._2
    val x1115 = x1110.flatMap(x1111 => { 
        val x1112 = x1111.o_parts
        val x1113 = RecM_ctx3(x1112)
        val x1114 = List(x1113)
        x1114})
    x1115})
val x1117 = x1116.distinct
val M_ctx3 = x1117
val x1118 = M_ctx3
val x1155 = M_ctx3.flatMap(x1119 => { 
    val x1120 = x1119.lbl
    val x1121 = L__D._1
    val x1152 = x1121.flatMap(x1122 => { 
      if({val x1123 = x1119.lbl
      val x1124 = x1123.L__F
      val x1125 = x1122._1
      val x1126 = x1124 == x1125
      x1126}) {  val x1127 = x1122._2
        val x1151 = x1127.flatMap(x1128 => { 
          if({val x1129 = x1128.l_orderkey
          val x1130 = x1119.lbl
          val x1131 = x1130.o__F
          val x1132 = x1131.o_orderkey
          val x1133 = x1129 == x1132
          x1133}) {  val x1134 = P__D._1
            val x1150 = x1134.flatMap(x1135 => { 
              if({val x1136 = x1119.lbl
              val x1137 = x1136.P__F
              val x1138 = x1135._1
              val x1139 = x1137 == x1138
              x1139}) {  val x1140 = x1135._2
                val x1149 = x1140.flatMap(x1141 => { 
                  if({val x1142 = x1128.l_partkey
                  val x1143 = x1141.p_partkey
                  val x1144 = x1142 == x1143
                  x1144}) {  val x1145 = x1141.p_name
                    val x1146 = x1128.l_quantity
                    val x1147 = Record1181(x1145, x1146)
                    val x1148 = List(x1147)
                    x1148} else {  Nil}})
                x1149} else {  Nil}})
            x1150} else {  Nil}})
        x1151} else {  Nil}})
    val x1153 = RecM_flat3(x1120, x1152)
    val x1154 = List(x1153)
    x1154})
val M_flat3 = x1155
val x1156 = M_flat3
val x1157 = (x1046,x1070,x1080,x1108,x1118,x1156)
x1157 }
    var end0 = System.currentTimeMillis() - start0
    
case class Input_Q1_Dict2(o_parts: (List[RecM_flat3], Unit))
case class Input_Q1_Dict1(c_orders: (List[RecM_flat2], Input_Q1_Dict2))
val Query4__F = ShredQuery4._1.head.lbl
val Query4__D = (ShredQuery4._2, Input_Q1_Dict1((ShredQuery4._4, Input_Q1_Dict2((ShredQuery4._6, Unit)))))
    def f(){
      val x1191 = Q1Flat2(Query4__F)
val x1192 = Record1258(x1191)
val x1193 = List(x1192)
val M_ctx1 = x1193
val x1194 = M_ctx1
val x1255 = M_ctx1.flatMap(x1195 => { 
    val x1196 = x1195.lbl
    val x1197 = Query4__D._1
    val x1252 = x1197.flatMap(x1198 => { 
      if({val x1199 = x1195.lbl
      val x1200 = x1199.Query4__F
      val x1201 = x1198._1
      val x1202 = x1200 == x1201
      x1202}) {  val x1203 = x1198._2
        val x1251 = x1203.flatMap(x1204 => { 
            val x1205 = Query4__D._2
            val x1206 = x1205.c_orders
            val x1207 = x1206._1
            val x1250 = x1207.flatMap(x1208 => { 
              if({val x1209 = x1204.c_orders
              val x1210 = x1208._1
              val x1211 = x1209 == x1210
              x1211}) {  val x1212 = x1208._2
                val x1249 = x1212.flatMap(x1213 => { 
                    val x1214 = Query4__D._2
                    val x1215 = x1214.c_orders
                    val x1216 = x1215._2
                    val x1217 = x1216.o_parts
                    val x1218 = x1217._1
                    val x1248 = x1218.flatMap(x1219 => { 
                      if({val x1220 = x1213.o_parts
                      val x1221 = x1219._1
                      val x1222 = x1220 == x1221
                      x1222}) {  val x1223 = x1219._2
                        val x1247 = x1223.flatMap(x1224 => { 
                            val x1225 = x1204.c_name
                            val x1226 = x1224.p_name
                            val x1227 = x1213.o_orderdate
                            val x1228 = Query4__D._2
                            val x1229 = x1228.c_orders
                            val x1230 = x1229._2
                            val x1231 = x1230.o_parts
                            val x1232 = x1231._1
                            val x1244 = x1232.foldLeft(0.0)((acc1272, x1233) => 
                              if({val x1234 = x1213.o_parts
                              val x1235 = x1233._1
                              val x1236 = x1234 == x1235
                              x1236}) {  acc1272 + {val x1237 = x1233._2
                                val x1243 = x1237.foldLeft(0.0)((acc1273, x1238) => 
                                  if({val x1239 = x1238.p_name
                                  val x1240 = x1224.p_name
                                  val x1241 = x1239 == x1240
                                  x1241}) {  acc1273 + {val x1242 = x1238.l_qty
                                    x1242}} else {  acc1273})
                                x1243}} else {  acc1272})
                            val x1245 = Record1274(x1225, x1226, x1227, x1244)
                            val x1246 = List(x1245)
                            x1246})
                        x1247} else {  Nil}})
                    x1248})
                x1249} else {  Nil}})
            x1250})
        x1251} else {  Nil}})
    val x1253 = Record1275(x1196, x1252)
    val x1254 = List(x1253)
    x1254})
val M_flat1 = x1255
val x1256 = M_flat1
val x1257 = (x1194,x1256)
x1257
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
