
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record878(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record879(c_name: String, c_orders: Record878)
case class Record886(o__F: Orders, P__F: Int, L__F: Int)
case class Record887(o_orderdate: String, o_parts: Record886)
case class Record897(p_name: String, l_qty: Double)
case class Record1050(lbl: Q1Flat2)
case class Input_Q1__DFlat1051(c_name: String, c_orders: Int)
case class Flat1052(o_orderdate: String, o_parts: Int)
case class Dict1052(o_parts: (List[(Int, List[Record897])], Unit))
case class Input_Q1__DDict1051(c_orders: (List[(Int, List[Flat1052])], Dict1052))
case class Record1064(l_qty: Double)
case class Record1069(c_name: String, p_name: String, month: String, t_qty: Int)
case class Record1070(_1: Q1Flat2, _2: List[Record1069])
case class RecM_flat2(_1: Record878, _2: List[Record887])
case class RecM_flat3(_1: Record886, _2: List[Record897])
case class RecM_flat1(_1: Q1Flat, _2: List[Record879])
case class RecM_ctx3(lbl: Record886)
case class RecM_ctx2(lbl: Record878)
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
    val Q1 = { val x709 = Q1Flat(P__F, C__F, L__F, O__F)
val x710 = RecM_ctx1(x709)
val x711 = List(x710)
val M_ctx1 = x711
val x712 = M_ctx1
val x714 = M_ctx1
val x715 = C__D._1
val x717 = x715
val x723 = { val hm874 = x714.groupBy{ case x718 => {
  val x720 = x718.lbl
  val x721 = x720.C__F
  x721}}
x717.flatMap(x719 => hm874.get({val x722 = x719._1
x722}) match {
 case Some(a) => a.map(v => (v, x719))
 case _ => Nil
}) }
val x728 = x723.flatMap{ case (x724, x725) => 
  val x726 = x725._2
  x726.map(x727 => {
  val nv875 = ((x724, x725), x727) 
     nv875
})}
val x741 = { val grps876 = x728.groupBy{ case ((x729, x730), x731) => { val x732 = (x729)
x732 }}
 grps876.map(x740 => (x740._1, x740._2.map{case ((x729, x730), x731) => val x733 = x731.c_name
val x734 = x729.lbl
val x735 = x734.P__F
val x736 = x734.L__F
val x737 = x734.O__F
val x738 = Record878(x735, x736, x737, x731)
val x739 = Record879(x733, x738)
x739})).toList }
val x746 = x741.map{ case (x742, x743) => { 
  val x744 = x742.lbl
  val x745 = RecM_flat1(x744, x743)
  x745 }}
val M_flat1 = x746
val x747 = M_flat1
val x749 = M_flat1
val x753 = x749.flatMap{ case x750 => 
  val x751 = x750._2
  x751.map(x752 => {
  val nv880 = (x750, x752) 
     nv880
})}
val x758 = x753.map{ case (x754, x755) => { 
  val x756 = x755.c_orders
  val x757 = RecM_ctx2(x756)
  x757 }}
val x759 = x758.distinct
val M_ctx2 = x759
val x760 = M_ctx2
val x762 = M_ctx2
val x763 = O__D._1
val x765 = x763
val x771 = { val hm882 = x762.groupBy{ case x766 => {
  val x768 = x766.lbl
  val x769 = x768.O__F
  x769}}
x765.flatMap(x767 => hm882.get({val x770 = x767._1
x770}) match {
 case Some(a) => a.map(v => (v, x767))
 case _ => Nil
}) }
val x781 = x771.flatMap{ case (x772, x773) =>
  val x774 = x773._2
  x774.withFilter{ case x775 =>
  {val x776 = x775.o_custkey
  val x777 = x772.lbl
  val x778 = x777.c__F
  val x779 = x778.c_custkey
  val x780 = x776 == x779
  x780}}.map(x775 => ((x772, x773), x775))
}
val x793 = { val grps884 = x781.groupBy{ case ((x782, x783), x784) => { val x785 = (x782)
x785 }}
 grps884.map(x792 => (x792._1, x792._2.map{case ((x782, x783), x784) => val x786 = x784.o_orderdate
val x787 = x782.lbl
val x788 = x787.P__F
val x789 = x787.L__F
val x790 = Record886(x784, x788, x789)
val x791 = Record887(x786, x790)
x791})).toList }
val x798 = x793.map{ case (x794, x795) => { 
  val x796 = x794.lbl
  val x797 = RecM_flat2(x796, x795)
  x797 }}
val M_flat2 = x798
val x799 = M_flat2
val x801 = M_flat2
val x805 = x801.flatMap{ case x802 => 
  val x803 = x802._2
  x803.map(x804 => {
  val nv888 = (x802, x804) 
     nv888
})}
val x810 = x805.map{ case (x806, x807) => { 
  val x808 = x807.o_parts
  val x809 = RecM_ctx3(x808)
  x809 }}
val x811 = x810.distinct
val M_ctx3 = x811
val x812 = M_ctx3
val x814 = M_ctx3
val x815 = L__D._1
val x817 = x815
val x823 = { val hm890 = x814.groupBy{ case x818 => {
  val x820 = x818.lbl
  val x821 = x820.L__F
  x821}}
x817.flatMap(x819 => hm890.get({val x822 = x819._1
x822}) match {
 case Some(a) => a.map(v => (v, x819))
 case _ => Nil
}) }
val x833 = x823.flatMap{ case (x824, x825) =>
  val x826 = x825._2
  x826.withFilter{ case x827 =>
  {val x828 = x827.l_orderkey
  val x829 = x824.lbl
  val x830 = x829.o__F
  val x831 = x830.o_orderkey
  val x832 = x828 == x831
  x832}}.map(x827 => ((x824, x825), x827))
}
val x834 = P__D._1
val x836 = x834
val x844 = { val hm893 = x833.groupBy{ case ((x837, x838), x839) => {
  val x841 = x837.lbl
  val x842 = x841.P__F
  x842}}
x836.flatMap(x840 => hm893.get({val x843 = x840._1
x843}) match {
 case Some(a) => a.map(v => (v, x840))
 case _ => Nil
}) }
val x854 = x844.flatMap{ case (((x845, x846), x847), x848) =>
  val x849 = x848._2
  x849.withFilter{ case x850 =>
  {val x851 = x847.l_partkey
  val x852 = x850.p_partkey
  val x853 = x851 == x852
  x853}}.map(x850 => ((((x845, x846), x847), x848), x850))
}
val x865 = { val grps895 = x854.groupBy{ case ((((x855, x856), x857), x858), x859) => { val x860 = (x855)
x860 }}
 grps895.map(x864 => (x864._1, x864._2.map{case ((((x855, x856), x857), x858), x859) => val x861 = x859.p_name
val x862 = x857.l_quantity
val x863 = Record897(x861, x862)
x863})).toList }
val x870 = x865.map{ case (x866, x867) => { 
  val x868 = x866.lbl
  val x869 = RecM_flat3(x868, x867)
  x869 }}
val M_flat3 = x870
val x871 = M_flat3
val x872 = (x712,x747,x760,x799,x812,x871)
x872 }
    var end0 = System.currentTimeMillis() - start0
    
val Q1__F = Q1._1.head.lbl
val Q1__D = (Q1._2, Input_Q1_Dict1((Q1._4, Input_Q1_Dict2((Q1._6, Unit)))))
    def f(){
      val x911 = Q1Flat2(Q1__F)
val x912 = Record1050(x911)
val x913 = List(x912)
val M_ctx1 = x913
val x914 = M_ctx1
val x916 = M_ctx1
val x917 = Q1__D._1
val x919 = x917
val x925 = { val hm1054 = x916.groupBy{ case x920 => {
  val x922 = x920.lbl
  val x923 = x922.Q1__F
  x923}}
x919.flatMap(x921 => hm1054.get({val x924 = x921._1
x924}) match {
 case Some(a) => a.map(v => (v, x921))
 case _ => Nil
}) }
val x930 = x925.flatMap{ case (x926, x927) => 
  val x928 = x927._2
  x928.map(x929 => {
  val nv1055 = ((x926, x927), x929) 
     nv1055
})}
val x931 = Q1__D._2
val x932 = x931.c_orders
val x933 = x932._1
val x935 = x933
val x942 = { val hm1057 = x930.groupBy{ case ((x936, x937), x938) => {
  val x940 = x938.c_orders
  x940}}
x935.flatMap(x939 => hm1057.get({val x941 = x939._1
x941}) match {
 case Some(a) => a.map(v => (v, x939))
 case _ => Nil
}) }
val x949 = x942.flatMap{ case (((x943, x944), x945), x946) => 
  val x947 = x946._2
  x947.map(x948 => {
  val nv1058 = ((((x943, x944), x945), x946), x948) 
     nv1058
})}
val x950 = x932._2
val x951 = x950.o_parts
val x952 = x951._1
val x954 = x952
val x963 = { val hm1059 = x949.groupBy{ case ((((x955, x956), x957), x958), x959) => {
  val x961 = x959.o_parts
  x961}}
x954.flatMap(x960 => hm1059.get({val x962 = x960._1
x962}) match {
 case Some(a) => a.map(v => (v, x960))
 case _ => Nil
}) }
val x972 = x963.flatMap{ case (((((x964, x965), x966), x967), x968), x969) => 
  val x970 = x969._2
  x970.map(x971 => {
  val nv1060 = ((((((x964, x965), x966), x967), x968), x969), x971) 
     nv1060
})}
val x974 = x952
val x985 = { val hm1061 = x972.groupBy{ case ((((((x975, x976), x977), x978), x979), x980), x981) => {
  val x983 = x979.o_parts
  x983}}
x974.flatMap(x982 => hm1061.get({val x984 = x982._1
x984}) match {
 case Some(a) => a.map(v => (v, x982))
 case _ => Nil
}) }
val x999 = x985.flatMap{ case (((((((x986, x987), x988), x989), x990), x991), x992), x993) =>
  val x994 = x993._2
  x994.withFilter{ case x995 =>
  {val x996 = x995.p_name
  val x997 = x992.p_name
  val x998 = x996 == x997
  x998}}.map(x995 => ((((((((x986, x987), x988), x989), x990), x991), x992), x993), x995))
}
val x1014 = x999.flatMap{ case ((((((((x1000, x1001), x1002), x1003), x1004), x1005), x1006), x1007), x1008) => 
  val x1009 = x1006.l_qty
  val x1010 = Record1064(x1009)
  val x1011 = x1008.l_qty
  val x1012 = (1 to x1011.asInstanceOf[Int]).map(v => x1010)
  x1012.map(x1013 => {
  val nv1063 = (((((((((x1000, x1001), x1002), x1003), x1004), x1005), x1006), x1007), x1008), x1013) 
     nv1063
})}
val x1027 = { val grps1065 = x1014.groupBy{ case (((((((((x1015, x1016), x1017), x1018), x1019), x1020), x1021), x1022), x1023), x1024) => { val x1025 = (x1015,x1016,x1017,x1018,x1019,x1020,x1021)
x1025 }}
 grps1065.map(x1026 => (x1026._1, x1026._2.foldLeft(0)((acc1066, x1026) => acc1066 + 1))).toList }
val x1042 = { val grps1067 = x1027.groupBy{ case ((x1028, x1029, x1030, x1031, x1032, x1033, x1034), x1035) => { val x1036 = (x1028)
x1036 }}
 grps1067.map(x1041 => (x1041._1, x1041._2.map{case ((x1028, x1029, x1030, x1031, x1032, x1033, x1034), x1035) => val x1037 = x1030.c_name
val x1038 = x1034.p_name
val x1039 = x1032.o_orderdate
val x1040 = Record1069(x1037, x1038, x1039, x1035)
x1040})).toList }
val x1047 = x1042.map{ case (x1043, x1044) => { 
  val x1045 = x1043.lbl
  val x1046 = Record1070(x1045, x1044)
  x1046 }}
val M_flat1 = x1047
val x1048 = M_flat1
val x1049 = (x914,x1048)
x1049
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
