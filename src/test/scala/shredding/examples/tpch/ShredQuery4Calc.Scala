
package experiments
/** Generated code **/
import shredding.examples.tpch._
object ShredQuery4Calc {
 case class Record1153(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record1154(c_name: String, c_orders: Record1153)
case class Record1161(o__F: Orders, P__F: Int, L__F: Int)
case class Record1162(o_orderdate: String, o_parts: Record1161)
case class Record1172(p_name: String, l_qty: Double)
case class Record1256(Q1__F: Int)
case class Record1257(lbl: Record1256)
case class Input_Q1__DFlat1259(c_name: String, c_orders: Int)
case class Flat1260(o_orderdate: String, o_parts: Int)
case class Dict1260(o_parts: (List[(Int, List[Record1172])], Unit))
case class Input_Q1__DDict1259(c_orders: (List[(Int, List[Flat1260])], Dict1260))
case class Record1273(l_qty: Double)
case class Record1275(c_name: String, p_name: String, month: String, t_qty: Int)
case class Record1276(_1: Record1256, _2: List[Record1275])
case class RecM_flat2(_1: Record1153, _2: List[Record1162])
case class RecM_flat3(_1: Record1161, _2: List[Record1172])
case class RecM_flat1(_1: Q1Flat, _2: List[Record1154])
case class RecM_ctx3(lbl: Record1161)
case class RecM_ctx2(lbl: Record1153)
case class RecM_ctx1(lbl: Q1Flat)
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
    val foo = { val x1034 = Q1Flat(P__F, C__F, L__F, O__F)
val x1035 = RecM_ctx1(x1034)
val x1036 = List(x1035)
val M_ctx1 = x1036
val x1037 = M_ctx1
val x1060 = M_ctx1.flatMap(x1038 =>  
  {  val x1039 = x1038.lbl
    val x1040 = C__D._1
    val x1057 = x1040.flatMap(x1041 =>  
      if({val x1042 = x1038.lbl
      val x1043 = x1042.C__F
      val x1044 = x1041._1
      val x1045 = x1043 == x1044
      x1045}) {  val x1046 = x1041._2
        val x1056 = x1046.flatMap(x1047 =>  
          {  val x1048 = x1047.c_name
            val x1049 = x1038.lbl
            val x1050 = x1049.P__F
            val x1051 = x1049.L__F
            val x1052 = x1049.O__F
            val x1053 = Record1153(x1050, x1051, x1052, x1047)
            val x1054 = Record1154(x1048, x1053)
            val x1055 = List(x1054)
            x1055})
        x1056} else {  Nil})
    val x1058 = RecM_flat1(x1039, x1057)
    val x1059 = List(x1058)
    x1059})
val M_flat1 = x1060
val x1061 = M_flat1
val x1069 = M_flat1.flatMap(x1062 =>  
  {  val x1063 = x1062._2
    val x1068 = x1063.flatMap(x1064 =>  
      {  val x1065 = x1064.c_orders
        val x1066 = RecM_ctx2(x1065)
        val x1067 = List(x1066)
        x1067})
    x1068})
val x1070 = x1069.distinct
val M_ctx2 = x1070
val x1071 = M_ctx2
val x1098 = M_ctx2.flatMap(x1072 =>  
  {  val x1073 = x1072.lbl
    val x1074 = O__D._1
    val x1095 = x1074.flatMap(x1075 =>  
      if({val x1076 = x1072.lbl
      val x1077 = x1076.O__F
      val x1078 = x1075._1
      val x1079 = x1077 == x1078
      x1079}) {  val x1080 = x1075._2
        val x1094 = x1080.flatMap(x1081 =>  
          if({val x1082 = x1081.o_custkey
          val x1083 = x1072.lbl
          val x1084 = x1083.c__F
          val x1085 = x1084.c_custkey
          val x1086 = x1082 == x1085
          x1086}) {  val x1087 = x1081.o_orderdate
            val x1088 = x1072.lbl
            val x1089 = x1088.P__F
            val x1090 = x1088.L__F
            val x1091 = Record1161(x1081, x1089, x1090)
            val x1092 = Record1162(x1087, x1091)
            val x1093 = List(x1092)
            x1093} else {  Nil})
        x1094} else {  Nil})
    val x1096 = RecM_flat2(x1073, x1095)
    val x1097 = List(x1096)
    x1097})
val M_flat2 = x1098
val x1099 = M_flat2
val x1107 = M_flat2.flatMap(x1100 =>  
  {  val x1101 = x1100._2
    val x1106 = x1101.flatMap(x1102 =>  
      {  val x1103 = x1102.o_parts
        val x1104 = RecM_ctx3(x1103)
        val x1105 = List(x1104)
        x1105})
    x1106})
val x1108 = x1107.distinct
val M_ctx3 = x1108
val x1109 = M_ctx3
val x1146 = M_ctx3.flatMap(x1110 =>  
  {  val x1111 = x1110.lbl
    val x1112 = L__D._1
    val x1143 = x1112.flatMap(x1113 =>  
      if({val x1114 = x1110.lbl
      val x1115 = x1114.L__F
      val x1116 = x1113._1
      val x1117 = x1115 == x1116
      x1117}) {  val x1118 = x1113._2
        val x1142 = x1118.flatMap(x1119 =>  
          if({val x1120 = x1119.l_orderkey
          val x1121 = x1110.lbl
          val x1122 = x1121.o__F
          val x1123 = x1122.o_orderkey
          val x1124 = x1120 == x1123
          x1124}) {  val x1125 = P__D._1
            val x1141 = x1125.flatMap(x1126 =>  
              if({val x1127 = x1110.lbl
              val x1128 = x1127.P__F
              val x1129 = x1126._1
              val x1130 = x1128 == x1129
              x1130}) {  val x1131 = x1126._2
                val x1140 = x1131.flatMap(x1132 =>  
                  if({val x1133 = x1119.l_partkey
                  val x1134 = x1132.p_partkey
                  val x1135 = x1133 == x1134
                  x1135}) {  val x1136 = x1132.p_name
                    val x1137 = x1119.l_quantity
                    val x1138 = Record1172(x1136, x1137)
                    val x1139 = List(x1138)
                    x1139} else {  Nil})
                x1140} else {  Nil})
            x1141} else {  Nil})
        x1142} else {  Nil})
    val x1144 = RecM_flat3(x1111, x1143)
    val x1145 = List(x1144)
    x1145})
val M_flat3 = x1146
val x1147 = M_flat3
val x1148 = (x1037,x1061,x1071,x1099,x1109,x1147)
x1148 }
    var end0 = System.currentTimeMillis() - start0
    println("setup time: "+end0)
    var time = List[Long]()
    for (i <- 1 to 5) {  
     var start = System.currentTimeMillis()
      f(foo)
      var end = System.currentTimeMillis() - start
      time = time :+ end
    }
    val avg = (time.sum/5)
    println(avg)
 }
 def f(foo: List[fooOut]){
  val x1184 = Record1256(Q1__F)
val x1185 = Record1257(x1184)
val x1186 = List(x1185)
val M_ctx1 = x1186
val x1187 = M_ctx1
val x1253 = M_ctx1.flatMap(x1188 =>  
  {  val x1189 = x1188.lbl
    val x1190 = Q1__D._1
    val x1250 = x1190.flatMap(x1191 =>  
      if({val x1192 = x1188.lbl
      val x1193 = x1192.Q1__F
      val x1194 = x1191._1
      val x1195 = x1193 == x1194
      x1195}) {  val x1196 = x1191._2
        val x1249 = x1196.flatMap(x1197 =>  
          {  val x1198 = Q1__D._2
            val x1199 = x1198.c_orders
            val x1200 = x1199._1
            val x1248 = x1200.flatMap(x1201 =>  
              if({val x1202 = x1197.c_orders
              val x1203 = x1201._1
              val x1204 = x1202 == x1203
              x1204}) {  val x1205 = x1201._2
                val x1247 = x1205.flatMap(x1206 =>  
                  {  val x1207 = Q1__D._2
                    val x1208 = x1207.c_orders
                    val x1209 = x1208._2
                    val x1210 = x1209.o_parts
                    val x1211 = x1210._1
                    val x1246 = x1211.flatMap(x1212 =>  
                      if({val x1213 = x1206.o_parts
                      val x1214 = x1212._1
                      val x1215 = x1213 == x1214
                      x1215}) {  val x1216 = x1212._2
                        val x1245 = x1216.flatMap(x1217 =>  
                          {  val x1218 = x1197.c_name
                            val x1219 = x1217.p_name
                            val x1220 = x1206.o_orderdate
                            val x1221 = Q1__D._2
                            val x1222 = x1221.c_orders
                            val x1223 = x1222._2
                            val x1224 = x1223.o_parts
                            val x1225 = x1224._1
                            val x1242 = x1225.foldLeft(0)((acc1271, x1226) => 
                              if({val x1227 = x1206.o_parts
                              val x1228 = x1226._1
                              val x1229 = x1227 == x1228
                              x1229}) {  acc1271 + {val x1230 = x1226._2
                                val x1241 = x1230.foldLeft(0)((acc1272, x1231) => 
                                  if({val x1232 = x1231.p_name
                                  val x1233 = x1217.p_name
                                  val x1234 = x1232 == x1233
                                  x1234}) {  acc1272 + {val x1235 = x1217.l_qty
                                    val x1236 = Record1273(x1235)
                                    val x1237 = x1231.l_qty
                                    val x1238 = (1 to x1237.asInstanceOf[Int]).map(v => x1236)
                                    val x1240 = x1238.foldLeft(0)((acc1274, x1239) => 
                                      {  acc1274 + {1}})
                                    x1240}} else {  acc1272})
                                x1241}} else {  acc1271})
                            val x1243 = Record1275(x1218, x1219, x1220, x1242)
                            val x1244 = List(x1243)
                            x1244})
                        x1245} else {  Nil})
                    x1246})
                x1247} else {  Nil})
            x1248})
        x1249} else {  Nil})
    val x1251 = Record1276(x1189, x1250)
    val x1252 = List(x1251)
    x1252})
val M_flat1 = x1253
val x1254 = M_flat1
val x1255 = (x1187,x1254)
x1255
 }
}
