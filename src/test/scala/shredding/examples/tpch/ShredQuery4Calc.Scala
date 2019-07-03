
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record991(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record992(c_name: String, c_orders: Record991)
case class Record999(o__F: Orders, P__F: Int, L__F: Int)
case class Record1000(o_orderdate: String, o_parts: Record999)
case class Record1009(p_name: String, l_qty: Double)
case class Record1084(lbl: Q1Flat2)
case class Input_Q1__DFlat1086(c_name: String, c_orders: Int)
case class Flat1087(o_orderdate: String, o_parts: Int)
case class Dict1087(o_parts: (List[(Int, List[Record1009])], Unit))
case class Input_Q1__DDict1086(c_orders: (List[(Int, List[Flat1087])], Dict1087))
case class Record1097(c_name: String, p_name: String, month: String, t_qty: Double)
case class Record1101(_1: Q1Flat2, _2: List[Record1097])
case class RecM_flat2(_1: Record991, _2: List[Record1000])
case class RecM_flat3(_1: Record999, _2: List[Record1009])
case class RecM_flat1(_1: Q1Flat, _2: List[Record992])
case class RecM_ctx3(lbl: Record999)
case class RecM_ctx2(lbl: Record991)
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
    val Q1 = { val x884 = Q1Flat(P__F, C__F, L__F, O__F)
val x885 = RecM_ctx1(x884)
val x886 = List(x885)
val M_ctx1 = x886
val x887 = M_ctx1
val x907 = M_ctx1.flatMap(x888 => { 
    val x889 = x888.lbl
    val x890 = C__D._1
    val x904 = x890.flatMap(x891 => { 
      if({val x892 = x888.lbl
      val x893 = x892.C__F
      val x894 = C__D._1
      val x895 = x893 == x894
      x895}) {  val x896 = x891.c_name
        val x897 = x888.lbl
        val x898 = x897.P__F
        val x899 = x897.L__F
        val x900 = x897.O__F
        val x901 = Record991(x898, x899, x900, x891)
        val x902 = Record992(x896, x901)
        val x903 = List(x902)
        x903} else {  Nil}})
    val x905 = RecM_flat1(x889, x904)
    val x906 = List(x905)
    x906})
val M_flat1 = x907
val x908 = M_flat1
val x916 = M_flat1.flatMap(x909 => { 
    val x910 = x909._2
    val x915 = x910.flatMap(x911 => { 
        val x912 = x911.c_orders
        val x913 = RecM_ctx2(x912)
        val x914 = List(x913)
        x914})
    x915})
val x917 = x916.distinct
val M_ctx2 = x917
val x918 = M_ctx2
val x942 = M_ctx2.flatMap(x919 => { 
    val x920 = x919.lbl
    val x921 = O__D._1
    val x939 = x921.flatMap(x922 => { 
      if({val x923 = x919.lbl
      val x924 = x923.O__F
      val x925 = O__D._1
      val x926 = x924 == x925
      val x927 = x922.o_custkey
      val x928 = x923.c__F
      val x929 = x928.c_custkey
      val x930 = x927 == x929
      val x931 = x926 && x930
      x931}) {  val x932 = x922.o_orderdate
        val x933 = x919.lbl
        val x934 = x933.P__F
        val x935 = x933.L__F
        val x936 = Record999(x922, x934, x935)
        val x937 = Record1000(x932, x936)
        val x938 = List(x937)
        x938} else {  Nil}})
    val x940 = RecM_flat2(x920, x939)
    val x941 = List(x940)
    x941})
val M_flat2 = x942
val x943 = M_flat2
val x951 = M_flat2.flatMap(x944 => { 
    val x945 = x944._2
    val x950 = x945.flatMap(x946 => { 
        val x947 = x946.o_parts
        val x948 = RecM_ctx3(x947)
        val x949 = List(x948)
        x949})
    x950})
val x952 = x951.distinct
val M_ctx3 = x952
val x953 = M_ctx3
val x985 = M_ctx3.flatMap(x954 => { 
    val x955 = x954.lbl
    val x956 = L__D._1
    val x982 = x956.flatMap(x957 => { 
      if({val x958 = x954.lbl
      val x959 = x958.L__F
      val x960 = L__D._1
      val x961 = x959 == x960
      val x962 = x957.l_orderkey
      val x963 = x958.o__F
      val x964 = x963.o_orderkey
      val x965 = x962 == x964
      val x966 = x961 && x965
      x966}) {  val x967 = P__D._1
        val x981 = x967.flatMap(x968 => { 
          if({val x969 = x954.lbl
          val x970 = x969.P__F
          val x971 = P__D._1
          val x972 = x970 == x971
          val x973 = x957.l_partkey
          val x974 = x968.p_partkey
          val x975 = x973 == x974
          val x976 = x972 && x975
          x976}) {  val x977 = x968.p_name
            val x978 = x957.l_quantity
            val x979 = Record1009(x977, x978)
            val x980 = List(x979)
            x980} else {  Nil}})
        x981} else {  Nil}})
    val x983 = RecM_flat3(x955, x982)
    val x984 = List(x983)
    x984})
val M_flat3 = x985
val x986 = M_flat3
val x987 = (x887,x908,x918,x943,x953,x986)
x987 }
    var end0 = System.currentTimeMillis() - start0
    
val Q1__F = Q1._1.head.lbl
val Q1__D = (Q1._2, Input_Q1_Dict1((Q1._4, Input_Q1_Dict2((Q1._6, Unit)))))
    def f(){
      val x1018 = Q1Flat2(Q1__F)
val x1019 = Record1084(x1018)
val x1020 = List(x1019)
val M_ctx1 = x1020
val x1021 = M_ctx1
val x1081 = M_ctx1.flatMap(x1022 => { 
    val x1023 = x1022.lbl
    val x1024 = Q1__D._1
    val x1078 = x1024.flatMap(x1025 => { 
      if({val x1026 = x1022.lbl
      val x1027 = x1026.Q1__F
      val x1028 = Q1__D._1
      val x1029 = x1027 == x1028
      x1029}) {  val x1030 = Q1__D._2
        val x1031 = x1030.c_orders
        val x1032 = x1031._1
        val x1077 = x1032.flatMap(x1033 => { 
          if({val x1034 = x1025.c_orders
          val x1035 = Q1__D._2
          val x1036 = x1035.c_orders
          val x1037 = x1036._1
          val x1038 = x1034 == x1037
          x1038}) {  val x1039 = Q1__D._2
            val x1040 = x1039.c_orders
            val x1041 = x1040._2
            val x1042 = x1041.o_parts
            val x1043 = x1042._1
            val x1076 = x1043.flatMap(x1044 => { 
              if({val x1045 = x1033.o_parts
              val x1046 = Q1__D._2
              val x1047 = x1046.c_orders
              val x1048 = x1047._2
              val x1049 = x1048.o_parts
              val x1050 = x1049._1
              val x1051 = x1045 == x1050
              x1051}) {  val x1052 = x1025.c_name
                val x1053 = x1044.p_name
                val x1054 = x1033.o_orderdate
                val x1055 = Q1__D._2
                val x1056 = x1055.c_orders
                val x1057 = x1056._2
                val x1058 = x1057.o_parts
                val x1059 = x1058._1
                val x1073 = x1059.foldLeft(0.0)((acc1095, x1060) => 
                  if({val x1061 = x1033.o_parts
                  val x1062 = Q1__D._2
                  val x1063 = x1062.c_orders
                  val x1064 = x1063._2
                  val x1065 = x1064.o_parts
                  val x1066 = x1065._1
                  val x1067 = x1061 == x1066
                  val x1068 = x1060.p_name
                  val x1069 = x1044.p_name
                  val x1070 = x1068 == x1069
                  val x1071 = x1067 && x1070
                  x1071}) {  acc1095 + {val x1072 = x1060.l_qty
                    x1072}} else {  acc1095})
                val x1074 = Record1097(x1052, x1053, x1054, x1073)
                val x1075 = List(x1074)
                x1075} else {  Nil}})
            x1076} else {  Nil}})
        x1077} else {  Nil}})
    val x1079 = Record1101(x1023, x1078)
    val x1080 = List(x1079)
    x1080})
val M_flat1 = x1081
val x1082 = M_flat1
val x1083 = (x1021,x1082)
x1083
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
