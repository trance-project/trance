
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record1166(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record1167(c_name: String, c_orders: Record1166)
case class Record1174(o__F: Orders, P__F: Int, L__F: Int)
case class Record1175(o_orderdate: String, o_parts: Record1174)
case class Record1185(p_name: String, l_qty: Double)
case class RecM_flat2(_1: Record1166, _2: List[Record1175])
case class RecM_flat3(_1: Record1174, _2: List[Record1185])
case class RecM_flat1(_1: Q1Flat, _2: List[Record1167])
case class RecM_ctx3(lbl: Record1174)
case class RecM_ctx2(lbl: Record1166)
case class RecM_ctx1(lbl: Q1Flat)
object ShredQuery1Calc {
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
    var end0 = System.currentTimeMillis() - start0
    def f(){
      val x1047 = Q1Flat(P__F, C__F, L__F, O__F)
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
x1161
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
