
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
    case class Record1172(P__F: Int, L__F: Int, O__F: Int, c__F: Customer, uniqueId: Long) extends CaseClassRecord
case class Record1173(c_name: String, c_orders: Record1172, uniqueId: Long) extends CaseClassRecord
case class Record1180(o__F: Orders, P__F: Int, L__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record1181(o_orderdate: String, o_parts: Record1180, uniqueId: Long) extends CaseClassRecord
case class Record1191(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class RecM_flat2(_1: Record1172, _2: List[Record1181], uniqueId: Long) extends CaseClassRecord
case class RecM_flat3(_1: Record1180, _2: List[Record1191], uniqueId: Long) extends CaseClassRecord
case class RecM_flat1(_1: Q1Flat, _2: List[Record1173], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx3(lbl: Record1180, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx2(lbl: Record1172, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx1(lbl: Q1Flat, uniqueId: Long) extends CaseClassRecord
object ShredQuery1Calc {
 def main(args: Array[String]){
    var start0 = System.currentTimeMillis()
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
    
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
      val x1053 = Q1Flat(P__F, C__F, L__F, O__F, newId) 
val x1054 = RecM_ctx1(x1053, newId) 
val x1055 = List(x1054) 
val M_ctx1 = x1055
val x1056 = M_ctx1
val x1079 = M_ctx1.flatMap(x1057 => { 
    val x1058 = x1057.lbl 
    val x1059 = C__D._1 
    val x1076 = x1059.flatMap(x1060 => { 
      if({val x1061 = x1057.lbl 
      val x1062 = x1061.C__F 
      val x1063 = x1060._1 
      val x1064 = x1062 == x1063 
      x1064    }) {  val x1065 = x1060._2 
        val x1075 = x1065.flatMap(x1066 => { 
            val x1067 = x1066.c_name 
            val x1068 = x1057.lbl 
            val x1069 = x1068.P__F 
            val x1070 = x1068.L__F 
            val x1071 = x1068.O__F 
            val x1072 = Record1172(x1069, x1070, x1071, x1066, newId) 
            val x1073 = Record1173(x1067, x1072, newId) 
            val x1074 = List(x1073) 
            x1074}) 
        x1075} else {  Nil}}) 
    val x1077 = RecM_flat1(x1058, x1076, newId) 
    val x1078 = List(x1077) 
    x1078}) 
val M_flat1 = x1079
val x1080 = M_flat1
val x1088 = M_flat1.flatMap(x1081 => { 
    val x1082 = x1081._2 
    val x1087 = x1082.flatMap(x1083 => { 
        val x1084 = x1083.c_orders 
        val x1085 = RecM_ctx2(x1084, newId) 
        val x1086 = List(x1085) 
        x1086}) 
    x1087}) 
val x1089 = x1088.distinct 
val M_ctx2 = x1089
val x1090 = M_ctx2
val x1117 = M_ctx2.flatMap(x1091 => { 
    val x1092 = x1091.lbl 
    val x1093 = O__D._1 
    val x1114 = x1093.flatMap(x1094 => { 
      if({val x1095 = x1091.lbl 
      val x1096 = x1095.O__F 
      val x1097 = x1094._1 
      val x1098 = x1096 == x1097 
      x1098    }) {  val x1099 = x1094._2 
        val x1113 = x1099.flatMap(x1100 => { 
          if({val x1101 = x1100.o_custkey 
          val x1102 = x1091.lbl 
          val x1103 = x1102.c__F 
          val x1104 = x1103.c_custkey 
          val x1105 = x1101 == x1104 
          x1105     }) {  val x1106 = x1100.o_orderdate 
            val x1107 = x1091.lbl 
            val x1108 = x1107.P__F 
            val x1109 = x1107.L__F 
            val x1110 = Record1180(x1100, x1108, x1109, newId) 
            val x1111 = Record1181(x1106, x1110, newId) 
            val x1112 = List(x1111) 
            x1112} else {  Nil}}) 
        x1113} else {  Nil}}) 
    val x1115 = RecM_flat2(x1092, x1114, newId) 
    val x1116 = List(x1115) 
    x1116}) 
val M_flat2 = x1117
val x1118 = M_flat2
val x1126 = M_flat2.flatMap(x1119 => { 
    val x1120 = x1119._2 
    val x1125 = x1120.flatMap(x1121 => { 
        val x1122 = x1121.o_parts 
        val x1123 = RecM_ctx3(x1122, newId) 
        val x1124 = List(x1123) 
        x1124}) 
    x1125}) 
val x1127 = x1126.distinct 
val M_ctx3 = x1127
val x1128 = M_ctx3
val x1165 = M_ctx3.flatMap(x1129 => { 
    val x1130 = x1129.lbl 
    val x1131 = L__D._1 
    val x1162 = x1131.flatMap(x1132 => { 
      if({val x1133 = x1129.lbl 
      val x1134 = x1133.L__F 
      val x1135 = x1132._1 
      val x1136 = x1134 == x1135 
      x1136    }) {  val x1137 = x1132._2 
        val x1161 = x1137.flatMap(x1138 => { 
          if({val x1139 = x1138.l_orderkey 
          val x1140 = x1129.lbl 
          val x1141 = x1140.o__F 
          val x1142 = x1141.o_orderkey 
          val x1143 = x1139 == x1142 
          x1143     }) {  val x1144 = P__D._1 
            val x1160 = x1144.flatMap(x1145 => { 
              if({val x1146 = x1129.lbl 
              val x1147 = x1146.P__F 
              val x1148 = x1145._1 
              val x1149 = x1147 == x1148 
              x1149    }) {  val x1150 = x1145._2 
                val x1159 = x1150.flatMap(x1151 => { 
                  if({val x1152 = x1138.l_partkey 
                  val x1153 = x1151.p_partkey 
                  val x1154 = x1152 == x1153 
                  x1154   }) {  val x1155 = x1151.p_name 
                    val x1156 = x1138.l_quantity 
                    val x1157 = Record1191(x1155, x1156, newId) 
                    val x1158 = List(x1157) 
                    x1158} else {  Nil}}) 
                x1159} else {  Nil}}) 
            x1160} else {  Nil}}) 
        x1161} else {  Nil}}) 
    val x1163 = RecM_flat3(x1130, x1162, newId) 
    val x1164 = List(x1163) 
    x1164}) 
val M_flat3 = x1165
val x1166 = M_flat3
val x1167 = (x1056,x1080,x1090,x1118,x1128,x1166) 
x1167           
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
