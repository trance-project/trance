
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record1038(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record1039(c_name: String, c_orders: Record1038)
case class Record1046(o__F: Orders, P__F: Int, L__F: Int)
case class Record1047(o_orderdate: String, o_parts: Record1046)
case class Record1057(p_name: String, l_qty: Double)
case class Record1135(lbl: Q1Flat2)
case class Input_Q1__DFlat1137(c_name: String, c_orders: Int)
case class Flat1138(o_orderdate: String, o_parts: Int)
case class Dict1138(o_parts: (List[(Int, List[Record1057])], Unit))
case class Input_Q1__DDict1137(c_orders: (List[(Int, List[Flat1138])], Dict1138))
case class Record1151(c_name: String, p_name: String, month: String, t_qty: Double)
case class Record1152(_1: Q1Flat2, _2: List[Record1151])
case class RecM_flat2(_1: Record1038, _2: List[Record1047])
case class RecM_flat3(_1: Record1046, _2: List[Record1057])
case class RecM_flat1(_1: Q1Flat, _2: List[Record1039])
case class RecM_ctx3(lbl: Record1046)
case class RecM_ctx2(lbl: Record1038)
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
    val Q1 = { val x919 = Q1Flat(P__F, C__F, L__F, O__F)
val x920 = RecM_ctx1(x919)
val x921 = List(x920)
val M_ctx1 = x921
val x922 = M_ctx1
val x945 = M_ctx1.flatMap(x923 => { 
    val x924 = x923.lbl
    val x925 = C__D._1
    val x942 = x925.flatMap(x926 => { 
      if({val x927 = x923.lbl
      val x928 = x927.C__F
      val x929 = x926._1
      val x930 = x928 == x929
      x930}) {  val x931 = x926._2
        val x941 = x931.flatMap(x932 => { 
            val x933 = x932.c_name
            val x934 = x923.lbl
            val x935 = x934.P__F
            val x936 = x934.L__F
            val x937 = x934.O__F
            val x938 = Record1038(x935, x936, x937, x932)
            val x939 = Record1039(x933, x938)
            val x940 = List(x939)
            x940})
        x941} else {  Nil}})
    val x943 = RecM_flat1(x924, x942)
    val x944 = List(x943)
    x944})
val M_flat1 = x945
val x946 = M_flat1
val x954 = M_flat1.flatMap(x947 => { 
    val x948 = x947._2
    val x953 = x948.flatMap(x949 => { 
        val x950 = x949.c_orders
        val x951 = RecM_ctx2(x950)
        val x952 = List(x951)
        x952})
    x953})
val x955 = x954.distinct
val M_ctx2 = x955
val x956 = M_ctx2
val x983 = M_ctx2.flatMap(x957 => { 
    val x958 = x957.lbl
    val x959 = O__D._1
    val x980 = x959.flatMap(x960 => { 
      if({val x961 = x957.lbl
      val x962 = x961.O__F
      val x963 = x960._1
      val x964 = x962 == x963
      x964}) {  val x965 = x960._2
        val x979 = x965.flatMap(x966 => { 
          if({val x967 = x966.o_custkey
          val x968 = x957.lbl
          val x969 = x968.c__F
          val x970 = x969.c_custkey
          val x971 = x967 == x970
          x971}) {  val x972 = x966.o_orderdate
            val x973 = x957.lbl
            val x974 = x973.P__F
            val x975 = x973.L__F
            val x976 = Record1046(x966, x974, x975)
            val x977 = Record1047(x972, x976)
            val x978 = List(x977)
            x978} else {  Nil}})
        x979} else {  Nil}})
    val x981 = RecM_flat2(x958, x980)
    val x982 = List(x981)
    x982})
val M_flat2 = x983
val x984 = M_flat2
val x992 = M_flat2.flatMap(x985 => { 
    val x986 = x985._2
    val x991 = x986.flatMap(x987 => { 
        val x988 = x987.o_parts
        val x989 = RecM_ctx3(x988)
        val x990 = List(x989)
        x990})
    x991})
val x993 = x992.distinct
val M_ctx3 = x993
val x994 = M_ctx3
val x1031 = M_ctx3.flatMap(x995 => { 
    val x996 = x995.lbl
    val x997 = L__D._1
    val x1028 = x997.flatMap(x998 => { 
      if({val x999 = x995.lbl
      val x1000 = x999.L__F
      val x1001 = x998._1
      val x1002 = x1000 == x1001
      x1002}) {  val x1003 = x998._2
        val x1027 = x1003.flatMap(x1004 => { 
          if({val x1005 = x1004.l_orderkey
          val x1006 = x995.lbl
          val x1007 = x1006.o__F
          val x1008 = x1007.o_orderkey
          val x1009 = x1005 == x1008
          x1009}) {  val x1010 = P__D._1
            val x1026 = x1010.flatMap(x1011 => { 
              if({val x1012 = x995.lbl
              val x1013 = x1012.P__F
              val x1014 = x1011._1
              val x1015 = x1013 == x1014
              x1015}) {  val x1016 = x1011._2
                val x1025 = x1016.flatMap(x1017 => { 
                  if({val x1018 = x1004.l_partkey
                  val x1019 = x1017.p_partkey
                  val x1020 = x1018 == x1019
                  x1020}) {  val x1021 = x1017.p_name
                    val x1022 = x1004.l_quantity
                    val x1023 = Record1057(x1021, x1022)
                    val x1024 = List(x1023)
                    x1024} else {  Nil}})
                x1025} else {  Nil}})
            x1026} else {  Nil}})
        x1027} else {  Nil}})
    val x1029 = RecM_flat3(x996, x1028)
    val x1030 = List(x1029)
    x1030})
val M_flat3 = x1031
val x1032 = M_flat3
val x1033 = (x922,x946,x956,x984,x994,x1032)
x1033 }
    var end0 = System.currentTimeMillis() - start0
    
val Q1__F = Q1._1.head.lbl
val Q1__D = (Q1._2, Input_Q1_Dict1((Q1._4, Input_Q1_Dict2((Q1._6, Unit)))))
    def f(){
      val x1068 = Q1Flat2(Q1__F)
val x1069 = Record1135(x1068)
val x1070 = List(x1069)
val M_ctx1 = x1070
val x1071 = M_ctx1
val x1132 = M_ctx1.flatMap(x1072 => { 
    val x1073 = x1072.lbl
    val x1074 = Q1__D._1
    val x1129 = x1074.flatMap(x1075 => { 
      if({val x1076 = x1072.lbl
      val x1077 = x1076.Q1__F
      val x1078 = x1075._1
      val x1079 = x1077 == x1078
      x1079}) {  val x1080 = x1075._2
        val x1128 = x1080.flatMap(x1081 => { 
            val x1082 = Q1__D._2
            val x1083 = x1082.c_orders
            val x1084 = x1083._1
            val x1127 = x1084.flatMap(x1085 => { 
              if({val x1086 = x1081.c_orders
              val x1087 = x1085._1
              val x1088 = x1086 == x1087
              x1088}) {  val x1089 = x1085._2
                val x1126 = x1089.flatMap(x1090 => { 
                    val x1091 = Q1__D._2
                    val x1092 = x1091.c_orders
                    val x1093 = x1092._2
                    val x1094 = x1093.o_parts
                    val x1095 = x1094._1
                    val x1125 = x1095.flatMap(x1096 => { 
                      if({val x1097 = x1090.o_parts
                      val x1098 = x1096._1
                      val x1099 = x1097 == x1098
                      x1099}) {  val x1100 = x1096._2
                        val x1124 = x1100.flatMap(x1101 => { 
                            val x1102 = x1081.c_name
                            val x1103 = x1101.p_name
                            val x1104 = x1090.o_orderdate
                            val x1105 = Q1__D._2
                            val x1106 = x1105.c_orders
                            val x1107 = x1106._2
                            val x1108 = x1107.o_parts
                            val x1109 = x1108._1
                            val x1121 = x1109.foldLeft(0.0)((acc1149, x1110) => 
                              if({val x1111 = x1090.o_parts
                              val x1112 = x1110._1
                              val x1113 = x1111 == x1112
                              x1113}) {  acc1149 + {val x1114 = x1110._2
                                val x1120 = x1114.foldLeft(0.0)((acc1150, x1115) => 
                                  if({val x1116 = x1115.p_name
                                  val x1117 = x1101.p_name
                                  val x1118 = x1116 == x1117
                                  x1118}) {  acc1150 + {val x1119 = x1115.l_qty
                                    x1119}} else {  acc1150})
                                x1120}} else {  acc1149})
                            val x1122 = Record1151(x1102, x1103, x1104, x1121)
                            val x1123 = List(x1122)
                            x1123})
                        x1124} else {  Nil}})
                    x1125})
                x1126} else {  Nil}})
            x1127})
        x1128} else {  Nil}})
    val x1130 = Record1152(x1073, x1129)
    val x1131 = List(x1130)
    x1131})
val M_flat1 = x1132
val x1133 = M_flat1
val x1134 = (x1071,x1133)
x1134
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
