
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record1154(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record1155(c_name: String, c_orders: Record1154)
case class Record1162(o__F: Orders, P__F: Int, L__F: Int)
case class Record1163(o_orderdate: String, o_parts: Record1162)
case class Record1173(p_name: String, l_qty: Double)
case class Record1257(lbl: Q1Flat2)
case class Input_Q1__DFlat1259(c_name: String, c_orders: Int)
case class Flat1260(o_orderdate: String, o_parts: Int)
case class Dict1260(o_parts: (List[(Int, List[Record1173])], Unit))
case class Input_Q1__DDict1259(c_orders: (List[(Int, List[Flat1260])], Dict1260))
case class Record1273(l_qty: Double)
case class Record1275(c_name: String, p_name: String, month: String, t_qty: Int)
case class Record1276(_1: Q1Flat2, _2: List[Record1275])
case class RecM_flat2(_1: Record1154, _2: List[Record1163])
case class RecM_flat3(_1: Record1162, _2: List[Record1173])
case class RecM_flat1(_1: Q1Flat, _2: List[Record1155])
case class RecM_ctx3(lbl: Record1162)
case class RecM_ctx2(lbl: Record1154)
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
    val Q1 = { val x1035 = Q1Flat(P__F, C__F, L__F, O__F)
val x1036 = RecM_ctx1(x1035)
val x1037 = List(x1036)
val M_ctx1 = x1037
val x1038 = M_ctx1
val x1061 = M_ctx1.flatMap(x1039 => { 
    val x1040 = x1039.lbl
    val x1041 = C__D._1
    val x1058 = x1041.flatMap(x1042 => { 
      if({val x1043 = x1039.lbl
      val x1044 = x1043.C__F
      val x1045 = x1042._1
      val x1046 = x1044 == x1045
      x1046}) {  val x1047 = x1042._2
        val x1057 = x1047.flatMap(x1048 => { 
            val x1049 = x1048.c_name
            val x1050 = x1039.lbl
            val x1051 = x1050.P__F
            val x1052 = x1050.L__F
            val x1053 = x1050.O__F
            val x1054 = Record1154(x1051, x1052, x1053, x1048)
            val x1055 = Record1155(x1049, x1054)
            val x1056 = List(x1055)
            x1056})
        x1057} else {  Nil}})
    val x1059 = RecM_flat1(x1040, x1058)
    val x1060 = List(x1059)
    x1060})
val M_flat1 = x1061
val x1062 = M_flat1
val x1070 = M_flat1.flatMap(x1063 => { 
    val x1064 = x1063._2
    val x1069 = x1064.flatMap(x1065 => { 
        val x1066 = x1065.c_orders
        val x1067 = RecM_ctx2(x1066)
        val x1068 = List(x1067)
        x1068})
    x1069})
val x1071 = x1070.distinct
val M_ctx2 = x1071
val x1072 = M_ctx2
val x1099 = M_ctx2.flatMap(x1073 => { 
    val x1074 = x1073.lbl
    val x1075 = O__D._1
    val x1096 = x1075.flatMap(x1076 => { 
      if({val x1077 = x1073.lbl
      val x1078 = x1077.O__F
      val x1079 = x1076._1
      val x1080 = x1078 == x1079
      x1080}) {  val x1081 = x1076._2
        val x1095 = x1081.flatMap(x1082 => { 
          if({val x1083 = x1082.o_custkey
          val x1084 = x1073.lbl
          val x1085 = x1084.c__F
          val x1086 = x1085.c_custkey
          val x1087 = x1083 == x1086
          x1087}) {  val x1088 = x1082.o_orderdate
            val x1089 = x1073.lbl
            val x1090 = x1089.P__F
            val x1091 = x1089.L__F
            val x1092 = Record1162(x1082, x1090, x1091)
            val x1093 = Record1163(x1088, x1092)
            val x1094 = List(x1093)
            x1094} else {  Nil}})
        x1095} else {  Nil}})
    val x1097 = RecM_flat2(x1074, x1096)
    val x1098 = List(x1097)
    x1098})
val M_flat2 = x1099
val x1100 = M_flat2
val x1108 = M_flat2.flatMap(x1101 => { 
    val x1102 = x1101._2
    val x1107 = x1102.flatMap(x1103 => { 
        val x1104 = x1103.o_parts
        val x1105 = RecM_ctx3(x1104)
        val x1106 = List(x1105)
        x1106})
    x1107})
val x1109 = x1108.distinct
val M_ctx3 = x1109
val x1110 = M_ctx3
val x1147 = M_ctx3.flatMap(x1111 => { 
    val x1112 = x1111.lbl
    val x1113 = L__D._1
    val x1144 = x1113.flatMap(x1114 => { 
      if({val x1115 = x1111.lbl
      val x1116 = x1115.L__F
      val x1117 = x1114._1
      val x1118 = x1116 == x1117
      x1118}) {  val x1119 = x1114._2
        val x1143 = x1119.flatMap(x1120 => { 
          if({val x1121 = x1120.l_orderkey
          val x1122 = x1111.lbl
          val x1123 = x1122.o__F
          val x1124 = x1123.o_orderkey
          val x1125 = x1121 == x1124
          x1125}) {  val x1126 = P__D._1
            val x1142 = x1126.flatMap(x1127 => { 
              if({val x1128 = x1111.lbl
              val x1129 = x1128.P__F
              val x1130 = x1127._1
              val x1131 = x1129 == x1130
              x1131}) {  val x1132 = x1127._2
                val x1141 = x1132.flatMap(x1133 => { 
                  if({val x1134 = x1120.l_partkey
                  val x1135 = x1133.p_partkey
                  val x1136 = x1134 == x1135
                  x1136}) {  val x1137 = x1133.p_name
                    val x1138 = x1120.l_quantity
                    val x1139 = Record1173(x1137, x1138)
                    val x1140 = List(x1139)
                    x1140} else {  Nil}})
                x1141} else {  Nil}})
            x1142} else {  Nil}})
        x1143} else {  Nil}})
    val x1145 = RecM_flat3(x1112, x1144)
    val x1146 = List(x1145)
    x1146})
val M_flat3 = x1147
val x1148 = M_flat3
val x1149 = (x1038,x1062,x1072,x1100,x1110,x1148)
x1149 }
    var end0 = System.currentTimeMillis() - start0
    
val Q1__F = Q1._1.head.lbl
val Q1__D = (Q1._2, Input_Q1_Dict1((Q1._4, Input_Q1_Dict2((Q1._6, Unit)))))
    def f(){
      val x1185 = Q1Flat2(Q1__F)
val x1186 = Record1257(x1185)
val x1187 = List(x1186)
val M_ctx1 = x1187
val x1188 = M_ctx1
val x1254 = M_ctx1.flatMap(x1189 => { 
    val x1190 = x1189.lbl
    val x1191 = Q1__D._1
    val x1251 = x1191.flatMap(x1192 => { 
      if({val x1193 = x1189.lbl
      val x1194 = x1193.Q1__F
      val x1195 = x1192._1
      val x1196 = x1194 == x1195
      x1196}) {  val x1197 = x1192._2
        val x1250 = x1197.flatMap(x1198 => { 
            val x1199 = Q1__D._2
            val x1200 = x1199.c_orders
            val x1201 = x1200._1
            val x1249 = x1201.flatMap(x1202 => { 
              if({val x1203 = x1198.c_orders
              val x1204 = x1202._1
              val x1205 = x1203 == x1204
              x1205}) {  val x1206 = x1202._2
                val x1248 = x1206.flatMap(x1207 => { 
                    val x1208 = Q1__D._2
                    val x1209 = x1208.c_orders
                    val x1210 = x1209._2
                    val x1211 = x1210.o_parts
                    val x1212 = x1211._1
                    val x1247 = x1212.flatMap(x1213 => { 
                      if({val x1214 = x1207.o_parts
                      val x1215 = x1213._1
                      val x1216 = x1214 == x1215
                      x1216}) {  val x1217 = x1213._2
                        val x1246 = x1217.flatMap(x1218 => { 
                            val x1219 = x1198.c_name
                            val x1220 = x1218.p_name
                            val x1221 = x1207.o_orderdate
                            val x1222 = Q1__D._2
                            val x1223 = x1222.c_orders
                            val x1224 = x1223._2
                            val x1225 = x1224.o_parts
                            val x1226 = x1225._1
                            val x1243 = x1226.foldLeft(0)((acc1271, x1227) => 
                              if({val x1228 = x1207.o_parts
                              val x1229 = x1227._1
                              val x1230 = x1228 == x1229
                              x1230}) {  acc1271 + {val x1231 = x1227._2
                                val x1242 = x1231.foldLeft(0)((acc1272, x1232) => 
                                  if({val x1233 = x1232.p_name
                                  val x1234 = x1218.p_name
                                  val x1235 = x1233 == x1234
                                  x1235}) {  acc1272 + {val x1236 = x1218.l_qty
                                    val x1237 = Record1273(x1236)
                                    val x1238 = x1232.l_qty
                                    val x1239 = (1 to x1238.asInstanceOf[Int]).map(v => x1237)
                                    val x1241 = x1239.foldLeft(0)((acc1274, x1240) => 
                                        acc1274 + {1})
                                    x1241}} else {  acc1272})
                                x1242}} else {  acc1271})
                            val x1244 = Record1275(x1219, x1220, x1221, x1243)
                            val x1245 = List(x1244)
                            x1245})
                        x1246} else {  Nil}})
                    x1247})
                x1248} else {  Nil}})
            x1249})
        x1250} else {  Nil}})
    val x1252 = Record1276(x1190, x1251)
    val x1253 = List(x1252)
    x1253})
val M_flat1 = x1254
val x1255 = M_flat1
val x1256 = (x1188,x1255)
x1256
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
