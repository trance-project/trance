
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record1061(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record1062(c_name: String, c_orders: Record1061)
case class Record1069(o__F: Orders, P__F: Int, L__F: Int)
case class Record1070(o_orderdate: String, o_parts: Record1069)
case class Record1079(p_name: String, l_qty: Double)
case class Record1160(lbl: Q1Flat2)
case class Input_Q1__DFlat1162(c_name: String, c_orders: Int)
case class Flat1163(o_orderdate: String, o_parts: Int)
case class Dict1163(o_parts: (List[(Int, List[Record1079])], Unit))
case class Input_Q1__DDict1162(c_orders: (List[(Int, List[Flat1163])], Dict1163))
case class Record1172(l_qty: Double)
case class Record1175(c_name: String, p_name: String, month: String, t_qty: Int)
case class Record1179(_1: Q1Flat2, _2: List[Record1175])
case class RecM_flat2(_1: Record1061, _2: List[Record1070])
case class RecM_flat3(_1: Record1069, _2: List[Record1079])
case class RecM_flat1(_1: Q1Flat, _2: List[Record1062])
case class RecM_ctx3(lbl: Record1069)
case class RecM_ctx2(lbl: Record1061)
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
    val Q1 = { val x954 = Q1Flat(P__F, C__F, L__F, O__F)
val x955 = RecM_ctx1(x954)
val x956 = List(x955)
val M_ctx1 = x956
val x957 = M_ctx1
val x977 = M_ctx1.flatMap(x958 => { 
    val x959 = x958.lbl
    val x960 = C__D._1
    val x974 = x960.flatMap(x961 => { 
      if({val x962 = x958.lbl
      val x963 = x962.C__F
      val x964 = C__D._1
      val x965 = x963 == x964
      x965}) {  val x966 = x961.c_name
        val x967 = x958.lbl
        val x968 = x967.P__F
        val x969 = x967.L__F
        val x970 = x967.O__F
        val x971 = Record1061(x968, x969, x970, x961)
        val x972 = Record1062(x966, x971)
        val x973 = List(x972)
        x973} else {  Nil}})
    val x975 = RecM_flat1(x959, x974)
    val x976 = List(x975)
    x976})
val M_flat1 = x977
val x978 = M_flat1
val x986 = M_flat1.flatMap(x979 => { 
    val x980 = x979._2
    val x985 = x980.flatMap(x981 => { 
        val x982 = x981.c_orders
        val x983 = RecM_ctx2(x982)
        val x984 = List(x983)
        x984})
    x985})
val x987 = x986.distinct
val M_ctx2 = x987
val x988 = M_ctx2
val x1012 = M_ctx2.flatMap(x989 => { 
    val x990 = x989.lbl
    val x991 = O__D._1
    val x1009 = x991.flatMap(x992 => { 
      if({val x993 = x989.lbl
      val x994 = x993.O__F
      val x995 = O__D._1
      val x996 = x994 == x995
      val x997 = x992.o_custkey
      val x998 = x993.c__F
      val x999 = x998.c_custkey
      val x1000 = x997 == x999
      val x1001 = x996 && x1000
      x1001}) {  val x1002 = x992.o_orderdate
        val x1003 = x989.lbl
        val x1004 = x1003.P__F
        val x1005 = x1003.L__F
        val x1006 = Record1069(x992, x1004, x1005)
        val x1007 = Record1070(x1002, x1006)
        val x1008 = List(x1007)
        x1008} else {  Nil}})
    val x1010 = RecM_flat2(x990, x1009)
    val x1011 = List(x1010)
    x1011})
val M_flat2 = x1012
val x1013 = M_flat2
val x1021 = M_flat2.flatMap(x1014 => { 
    val x1015 = x1014._2
    val x1020 = x1015.flatMap(x1016 => { 
        val x1017 = x1016.o_parts
        val x1018 = RecM_ctx3(x1017)
        val x1019 = List(x1018)
        x1019})
    x1020})
val x1022 = x1021.distinct
val M_ctx3 = x1022
val x1023 = M_ctx3
val x1055 = M_ctx3.flatMap(x1024 => { 
    val x1025 = x1024.lbl
    val x1026 = L__D._1
    val x1052 = x1026.flatMap(x1027 => { 
      if({val x1028 = x1024.lbl
      val x1029 = x1028.L__F
      val x1030 = L__D._1
      val x1031 = x1029 == x1030
      val x1032 = x1027.l_orderkey
      val x1033 = x1028.o__F
      val x1034 = x1033.o_orderkey
      val x1035 = x1032 == x1034
      val x1036 = x1031 && x1035
      x1036}) {  val x1037 = P__D._1
        val x1051 = x1037.flatMap(x1038 => { 
          if({val x1039 = x1024.lbl
          val x1040 = x1039.P__F
          val x1041 = P__D._1
          val x1042 = x1040 == x1041
          val x1043 = x1027.l_partkey
          val x1044 = x1038.p_partkey
          val x1045 = x1043 == x1044
          val x1046 = x1042 && x1045
          x1046}) {  val x1047 = x1038.p_name
            val x1048 = x1027.l_quantity
            val x1049 = Record1079(x1047, x1048)
            val x1050 = List(x1049)
            x1050} else {  Nil}})
        x1051} else {  Nil}})
    val x1053 = RecM_flat3(x1025, x1052)
    val x1054 = List(x1053)
    x1054})
val M_flat3 = x1055
val x1056 = M_flat3
val x1057 = (x957,x978,x988,x1013,x1023,x1056)
x1057 }
    var end0 = System.currentTimeMillis() - start0
    
val Q1__F = Q1._1.head.lbl
val Q1__D = (Q1._2, Input_Q1_Dict1((Q1._4, Input_Q1_Dict2((Q1._6, Unit)))))
    def f(){
      val x1089 = Q1Flat2(Q1__F)
val x1090 = Record1160(x1089)
val x1091 = List(x1090)
val M_ctx1 = x1091
val x1092 = M_ctx1
val x1157 = M_ctx1.flatMap(x1093 => { 
    val x1094 = x1093.lbl
    val x1095 = Q1__D._1
    val x1154 = x1095.flatMap(x1096 => { 
      if({val x1097 = x1093.lbl
      val x1098 = x1097.Q1__F
      val x1099 = Q1__D._1
      val x1100 = x1098 == x1099
      x1100}) {  val x1101 = Q1__D._2
        val x1102 = x1101.c_orders
        val x1103 = x1102._1
        val x1153 = x1103.flatMap(x1104 => { 
          if({val x1105 = x1096.c_orders
          val x1106 = Q1__D._2
          val x1107 = x1106.c_orders
          val x1108 = x1107._1
          val x1109 = x1105 == x1108
          x1109}) {  val x1110 = Q1__D._2
            val x1111 = x1110.c_orders
            val x1112 = x1111._2
            val x1113 = x1112.o_parts
            val x1114 = x1113._1
            val x1152 = x1114.flatMap(x1115 => { 
              if({val x1116 = x1104.o_parts
              val x1117 = Q1__D._2
              val x1118 = x1117.c_orders
              val x1119 = x1118._2
              val x1120 = x1119.o_parts
              val x1121 = x1120._1
              val x1122 = x1116 == x1121
              x1122}) {  val x1123 = x1096.c_name
                val x1124 = x1115.p_name
                val x1125 = x1104.o_orderdate
                val x1126 = Q1__D._2
                val x1127 = x1126.c_orders
                val x1128 = x1127._2
                val x1129 = x1128.o_parts
                val x1130 = x1129._1
                val x1149 = x1130.foldLeft(0)((acc1171, x1131) => 
                  if({val x1132 = x1104.o_parts
                  val x1133 = Q1__D._2
                  val x1134 = x1133.c_orders
                  val x1135 = x1134._2
                  val x1136 = x1135.o_parts
                  val x1137 = x1136._1
                  val x1138 = x1132 == x1137
                  val x1139 = x1131.p_name
                  val x1140 = x1115.p_name
                  val x1141 = x1139 == x1140
                  val x1142 = x1138 && x1141
                  x1142}) {  acc1171 + {val x1143 = x1115.l_qty
                    val x1144 = Record1172(x1143)
                    val x1145 = x1131.l_qty
                    val x1146 = (1 to x1145.asInstanceOf[Int]).map(v => x1144)
                    val x1148 = x1146.foldLeft(0)((acc1173, x1147) => 
                        acc1173 + {1})
                    x1148}} else {  acc1171})
                val x1150 = Record1175(x1123, x1124, x1125, x1149)
                val x1151 = List(x1150)
                x1151} else {  Nil}})
            x1152} else {  Nil}})
        x1153} else {  Nil}})
    val x1155 = Record1179(x1094, x1154)
    val x1156 = List(x1155)
    x1156})
val M_flat1 = x1157
val x1158 = M_flat1
val x1159 = (x1092,x1158)
x1159
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
