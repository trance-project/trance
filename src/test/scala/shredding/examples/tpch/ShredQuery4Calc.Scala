
package experiments
/** Generated code **/
import shredding.examples.tpch._
object ShredQuery4Calc {
 case class Record1158(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record1159(c_name: String, c_orders: Record1158)
case class Record1166(o__F: Orders, P__F: Int, L__F: Int)
case class Record1167(o_orderdate: String, o_parts: Record1166)
case class Record1177(p_name: String, l_qty: Double)
case class Record1261(lbl: Q1Flat2)
case class Input_Q1__DFlat1263(c_name: String, c_orders: Int)
case class Flat1264(o_orderdate: String, o_parts: Int)
case class Dict1264(o_parts: (List[(Int, List[Record1177])], Unit))
case class Input_Q1__DDict1263(c_orders: (List[(Int, List[Flat1264])], Dict1264))
case class Record1277(l_qty: Double)
case class Record1279(c_name: String, p_name: String, month: String, t_qty: Int)
case class Record1280(_1: Q1Flat2, _2: List[Record1279])
case class RecM_flat2(_1: Record1158, _2: List[Record1167])
case class RecM_flat3(_1: Record1166, _2: List[Record1177])
case class RecM_flat1(_1: Q1Flat, _2: List[Record1159])
case class RecM_ctx3(lbl: Record1166)
case class RecM_ctx2(lbl: Record1158)
case class RecM_ctx1(lbl: Q1Flat)
case class Input_Q1_Dict2(o_parts: (List[RecM_flat3], Unit))
case class Input_Q1_Dict1(c_orders: (List[RecM_flat2], Input_Q1_Dict2))

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

    val Q1 = { 
      val x1039 = Q1Flat(P__F, C__F, L__F, O__F)
val x1040 = RecM_ctx1(x1039)
val x1041 = List(x1040)
val M_ctx1 = x1041
val x1042 = M_ctx1
val x1065 = M_ctx1.flatMap(x1043 => { 
    val x1044 = x1043.lbl
    val x1045 = C__D._1
    val x1062 = x1045.flatMap(x1046 => { 
      if({val x1047 = x1043.lbl
      val x1048 = x1047.C__F
      val x1049 = x1046._1
      val x1050 = x1048 == x1049
      x1050}) {  val x1051 = x1046._2
        val x1061 = x1051.flatMap(x1052 => { 
            val x1053 = x1052.c_name
            val x1054 = x1043.lbl
            val x1055 = x1054.P__F
            val x1056 = x1054.L__F
            val x1057 = x1054.O__F
            val x1058 = Record1158(x1055, x1056, x1057, x1052)
            val x1059 = Record1159(x1053, x1058)
            val x1060 = List(x1059)
            x1060})
        x1061} else {  Nil}})
    val x1063 = RecM_flat1(x1044, x1062)
    val x1064 = List(x1063)
    x1064})
val M_flat1 = x1065
val x1066 = M_flat1
val x1074 = M_flat1.flatMap(x1067 => { 
    val x1068 = x1067._2
    val x1073 = x1068.flatMap(x1069 => { 
        val x1070 = x1069.c_orders
        val x1071 = RecM_ctx2(x1070)
        val x1072 = List(x1071)
        x1072})
    x1073})
val x1075 = x1074.distinct
val M_ctx2 = x1075
val x1076 = M_ctx2
val x1103 = M_ctx2.flatMap(x1077 => { 
    val x1078 = x1077.lbl
    val x1079 = O__D._1
    val x1100 = x1079.flatMap(x1080 => { 
      if({val x1081 = x1077.lbl
      val x1082 = x1081.O__F
      val x1083 = x1080._1
      val x1084 = x1082 == x1083
      x1084}) {  val x1085 = x1080._2
        val x1099 = x1085.flatMap(x1086 => { 
          if({val x1087 = x1086.o_custkey
          val x1088 = x1077.lbl
          val x1089 = x1088.c__F
          val x1090 = x1089.c_custkey
          val x1091 = x1087 == x1090
          x1091}) {  val x1092 = x1086.o_orderdate
            val x1093 = x1077.lbl
            val x1094 = x1093.P__F
            val x1095 = x1093.L__F
            val x1096 = Record1166(x1086, x1094, x1095)
            val x1097 = Record1167(x1092, x1096)
            val x1098 = List(x1097)
            x1098} else {  Nil}})
        x1099} else {  Nil}})
    val x1101 = RecM_flat2(x1078, x1100)
    val x1102 = List(x1101)
    x1102})
val M_flat2 = x1103
val x1104 = M_flat2
val x1112 = M_flat2.flatMap(x1105 => { 
    val x1106 = x1105._2
    val x1111 = x1106.flatMap(x1107 => { 
        val x1108 = x1107.o_parts
        val x1109 = RecM_ctx3(x1108)
        val x1110 = List(x1109)
        x1110})
    x1111})
val x1113 = x1112.distinct
val M_ctx3 = x1113
val x1114 = M_ctx3
val x1151 = M_ctx3.flatMap(x1115 => { 
    val x1116 = x1115.lbl
    val x1117 = L__D._1
    val x1148 = x1117.flatMap(x1118 => { 
      if({val x1119 = x1115.lbl
      val x1120 = x1119.L__F
      val x1121 = x1118._1
      val x1122 = x1120 == x1121
      x1122}) {  val x1123 = x1118._2
        val x1147 = x1123.flatMap(x1124 => { 
          if({val x1125 = x1124.l_orderkey
          val x1126 = x1115.lbl
          val x1127 = x1126.o__F
          val x1128 = x1127.o_orderkey
          val x1129 = x1125 == x1128
          x1129}) {  val x1130 = P__D._1
            val x1146 = x1130.flatMap(x1131 => { 
              if({val x1132 = x1115.lbl
              val x1133 = x1132.P__F
              val x1134 = x1131._1
              val x1135 = x1133 == x1134
              x1135}) {  val x1136 = x1131._2
                val x1145 = x1136.flatMap(x1137 => { 
                  if({val x1138 = x1124.l_partkey
                  val x1139 = x1137.p_partkey
                  val x1140 = x1138 == x1139
                  x1140}) {  val x1141 = x1137.p_name
                    val x1142 = x1124.l_quantity
                    val x1143 = Record1177(x1141, x1142)
                    val x1144 = List(x1143)
                    x1144} else {  Nil}})
                x1145} else {  Nil}})
            x1146} else {  Nil}})
        x1147} else {  Nil}})
    val x1149 = RecM_flat3(x1116, x1148)
    val x1150 = List(x1149)
    x1150})
val M_flat3 = x1151
val x1152 = M_flat3
val x1153 = (x1042,x1066,x1076,x1104,x1114,x1152)
x1153 
    }
val Q1__F = Q1._1.head.lbl
val Q1__D = (Q1._2, Input_Q1_Dict1((Q1._4, Input_Q1_Dict2((Q1._6, Unit)))))
    var end0 = System.currentTimeMillis() - start0
    var time = List[Long]()
    for (i <- 1 to 5) {  
     var start = System.currentTimeMillis()
      f(Q1__F, Q1__D)
      var end = System.currentTimeMillis() - start
      time = time :+ end
    }
    val avg = (time.sum/5)
    println(end0+","+avg)
 }
 def f(Q1__F: Q1Flat, Q1__D: (List[RecM_flat1], Input_Q1_Dict1)){
  val x1189 = Q1Flat2(Q1__F)
val x1190 = Record1261(x1189)
val x1191 = List(x1190)
val M_ctx1 = x1191
val x1192 = M_ctx1
val x1258 = M_ctx1.flatMap(x1193 => { 
    val x1194 = x1193.lbl
    val x1195 = Q1__D._1
    val x1255 = x1195.flatMap(x1196 => { 
      if({val x1197 = x1193.lbl
      val x1198 = x1197.Q1__F
      val x1199 = x1196._1
      val x1200 = x1198 == x1199
      x1200}) {  val x1201 = x1196._2
        val x1254 = x1201.flatMap(x1202 => { 
            val x1203 = Q1__D._2
            val x1204 = x1203.c_orders
            val x1205 = x1204._1
            val x1253 = x1205.flatMap(x1206 => { 
              if({val x1207 = x1202.c_orders
              val x1208 = x1206._1
              val x1209 = x1207 == x1208
              x1209}) {  val x1210 = x1206._2
                val x1252 = x1210.flatMap(x1211 => { 
                    val x1212 = Q1__D._2
                    val x1213 = x1212.c_orders
                    val x1214 = x1213._2
                    val x1215 = x1214.o_parts
                    val x1216 = x1215._1
                    val x1251 = x1216.flatMap(x1217 => { 
                      if({val x1218 = x1211.o_parts
                      val x1219 = x1217._1
                      val x1220 = x1218 == x1219
                      x1220}) {  val x1221 = x1217._2
                        val x1250 = x1221.flatMap(x1222 => { 
                            val x1223 = x1202.c_name
                            val x1224 = x1222.p_name
                            val x1225 = x1211.o_orderdate
                            val x1226 = Q1__D._2
                            val x1227 = x1226.c_orders
                            val x1228 = x1227._2
                            val x1229 = x1228.o_parts
                            val x1230 = x1229._1
                            val x1247 = x1230.foldLeft(0)((acc1275, x1231) => 
                              if({val x1232 = x1211.o_parts
                              val x1233 = x1231._1
                              val x1234 = x1232 == x1233
                              x1234}) {  acc1275 + {val x1235 = x1231._2
                                val x1246 = x1235.foldLeft(0)((acc1276, x1236) => 
                                  if({val x1237 = x1236.p_name
                                  val x1238 = x1222.p_name
                                  val x1239 = x1237 == x1238
                                  x1239}) {  acc1276 + {val x1240 = x1222.l_qty
                                    val x1241 = Record1277(x1240)
                                    val x1242 = x1236.l_qty
                                    val x1243 = (1 to x1242.asInstanceOf[Int]).map(v => x1241)
                                    val x1245 = x1243.foldLeft(0)((acc1278, x1244) => 
                                        acc1278 + {1})
                                    x1245}} else {  acc1276})
                                x1246}} else {  acc1275})
                            val x1248 = Record1279(x1223, x1224, x1225, x1247)
                            val x1249 = List(x1248)
                            x1249})
                        x1250} else {  Nil}})
                    x1251})
                x1252} else {  Nil}})
            x1253})
        x1254} else {  Nil}})
    val x1256 = Record1280(x1194, x1255)
    val x1257 = List(x1256)
    x1257})
val M_flat1 = x1258
val x1259 = M_flat1
val x1260 = (x1192,x1259)
x1260
 }
}
