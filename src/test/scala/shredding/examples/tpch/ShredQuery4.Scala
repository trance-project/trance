
package experiments
/** Generated code **/
import shredding.examples.tpch._
object ShredQuery4Calc {
 case class Record827(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record828(c_name: String, c_orders: Record827)
case class Record835(o__F: Orders, P__F: Int, L__F: Int)
case class Record836(o_orderdate: String, o_parts: Record835)
case class Record846(p_name: String, l_qty: Double)
case class Record1001(lbl: Q1Flat2)
case class Input_Q1__DFlat1002(c_name: String, c_orders: Int)
case class Flat1003(o_orderdate: String, o_parts: Int)
case class Dict1003(o_parts: (List[(Int, List[Record846])], Unit))
case class Input_Q1__DDict1002(c_orders: (List[(Int, List[Flat1003])], Dict1003))
case class Record1014(l_qty: Double)
case class Record1020(c_name: String, p_name: String, month: String, t_qty: Int)
case class Record1021(_1: Q1Flat2, _2: List[Record1020])
case class RecM_flat2(_1: Record827, _2: List[Record836])
case class RecM_flat3(_1: Record835, _2: List[Record846])
case class RecM_flat1(_1: Q1Flat, _2: List[Record828])
case class RecM_ctx3(lbl: Record835)
case class RecM_ctx2(lbl: Record827)
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

case class Input_Q1_Dict2(o_parts: (List[RecM_flat3], Unit))
case class Input_Q1_Dict1(c_orders: (List[RecM_flat2], Input_Q1_Dict2))
val Q1__F = Q1._1.head.lbl
val Q1__D = (Q1._2, Input_Q1_Dict1((Q1._4, Input_Q1_Dict2((Q1._6, Unit)))))
    val Q1 = { 
      val x658 = Q1Flat(P__F, C__F, L__F, O__F)
val x659 = RecM_ctx1(x658)
val x660 = List(x659)
val M_ctx1 = x660
val x661 = M_ctx1
val x663 = M_ctx1
val x664 = C__D._1
val x666 = x664
val x672 = { val hm823 = x663.groupBy{ case x667 => {
  val x669 = x667.lbl
  val x670 = x669.C__F
  x670}}
x666.flatMap(x668 => hm823.get({val x671 = x668._1
x671}) match {
 case Some(a) => a.map(v => (v, x668))
 case _ => Nil
}) }
val x677 = x672.flatMap{ case (x673, x674) => 
  val x675 = x674._2
  x675.map(x676 => {
  val nv824 = ((x673, x674), x676) 
     nv824
})}
val x690 = { val grps825 = x677.groupBy{ case ((x678, x679), x680) => { val x681 = (x678)
x681 }}
 grps825.map(x689 => (x689._1, x689._2.map{case ((x678, x679), x680) => val x682 = x680.c_name
val x683 = x678.lbl
val x684 = x683.P__F
val x685 = x683.L__F
val x686 = x683.O__F
val x687 = Record827(x684, x685, x686, x680)
val x688 = Record828(x682, x687)
x688})).toList }
val x695 = x690.map{ case (x691, x692) => { 
  val x693 = x691.lbl
  val x694 = RecM_flat1(x693, x692)
  x694 }}
val M_flat1 = x695
val x696 = M_flat1
val x698 = M_flat1
val x702 = x698.flatMap{ case x699 => 
  val x700 = x699._2
  x700.map(x701 => {
  val nv829 = (x699, x701) 
     nv829
})}
val x707 = x702.map{ case (x703, x704) => { 
  val x705 = x704.c_orders
  val x706 = RecM_ctx2(x705)
  x706 }}
val x708 = x707.distinct
val M_ctx2 = x708
val x709 = M_ctx2
val x711 = M_ctx2
val x712 = O__D._1
val x714 = x712
val x720 = { val hm831 = x711.groupBy{ case x715 => {
  val x717 = x715.lbl
  val x718 = x717.O__F
  x718}}
x714.flatMap(x716 => hm831.get({val x719 = x716._1
x719}) match {
 case Some(a) => a.map(v => (v, x716))
 case _ => Nil
}) }
val x730 = x720.flatMap{ case (x721, x722) => 
  val x723 = x722._2
  x723.map(x724 => {
  val nv832 = ((x721, x722), x724) 
   if({val x725 = x724.o_custkey
  val x726 = x721.lbl
  val x727 = x726.c__F
  val x728 = x727.c_custkey
  val x729 = x725 == x728
  x729}) {  nv832} else {  Nil}
})}
val x742 = { val grps833 = x730.groupBy{ case ((x731, x732), x733) => { val x734 = (x731)
x734 }}
 grps833.map(x741 => (x741._1, x741._2.map{case ((x731, x732), x733) => val x735 = x733.o_orderdate
val x736 = x731.lbl
val x737 = x736.P__F
val x738 = x736.L__F
val x739 = Record835(x733, x737, x738)
val x740 = Record836(x735, x739)
x740})).toList }
val x747 = x742.map{ case (x743, x744) => { 
  val x745 = x743.lbl
  val x746 = RecM_flat2(x745, x744)
  x746 }}
val M_flat2 = x747
val x748 = M_flat2
val x750 = M_flat2
val x754 = x750.flatMap{ case x751 => 
  val x752 = x751._2
  x752.map(x753 => {
  val nv837 = (x751, x753) 
     nv837
})}
val x759 = x754.map{ case (x755, x756) => { 
  val x757 = x756.o_parts
  val x758 = RecM_ctx3(x757)
  x758 }}
val x760 = x759.distinct
val M_ctx3 = x760
val x761 = M_ctx3
val x763 = M_ctx3
val x764 = L__D._1
val x766 = x764
val x772 = { val hm839 = x763.groupBy{ case x767 => {
  val x769 = x767.lbl
  val x770 = x769.L__F
  x770}}
x766.flatMap(x768 => hm839.get({val x771 = x768._1
x771}) match {
 case Some(a) => a.map(v => (v, x768))
 case _ => Nil
}) }
val x782 = x772.flatMap{ case (x773, x774) => 
  val x775 = x774._2
  x775.map(x776 => {
  val nv840 = ((x773, x774), x776) 
   if({val x777 = x776.l_orderkey
  val x778 = x773.lbl
  val x779 = x778.o__F
  val x780 = x779.o_orderkey
  val x781 = x777 == x780
  x781}) {  nv840} else {  Nil}
})}
val x783 = P__D._1
val x785 = x783
val x793 = { val hm842 = x782.groupBy{ case ((x786, x787), x788) => {
  val x790 = x786.lbl
  val x791 = x790.P__F
  x791}}
x785.flatMap(x789 => hm842.get({val x792 = x789._1
x792}) match {
 case Some(a) => a.map(v => (v, x789))
 case _ => Nil
}) }
val x803 = x793.flatMap{ case (((x794, x795), x796), x797) => 
  val x798 = x797._2
  x798.map(x799 => {
  val nv843 = ((((x794, x795), x796), x797), x799) 
   if({val x800 = x796.l_partkey
  val x801 = x799.p_partkey
  val x802 = x800 == x801
  x802}) {  nv843} else {  Nil}
})}
val x814 = { val grps844 = x803.groupBy{ case ((((x804, x805), x806), x807), x808) => { val x809 = (x804)
x809 }}
 grps844.map(x813 => (x813._1, x813._2.map{case ((((x804, x805), x806), x807), x808) => val x810 = x808.p_name
val x811 = x806.l_quantity
val x812 = Record846(x810, x811)
x812})).toList }
val x819 = x814.map{ case (x815, x816) => { 
  val x817 = x815.lbl
  val x818 = RecM_flat3(x817, x816)
  x818 }}
val M_flat3 = x819
val x820 = M_flat3
val x821 = (x661,x696,x709,x748,x761,x820)
x821 
    }
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
 def f(Q1__F: Q1Flat, Q1__D: (RecM_flat1, Input_Q1_Dict1)){
  val x860 = Q1Flat2(Q1__F)
val x861 = Record1001(x860)
val x862 = List(x861)
val M_ctx1 = x862
val x863 = M_ctx1
val x865 = M_ctx1
val x866 = Q1__D._1
val x868 = x866
val x874 = { val hm1005 = x865.groupBy{ case x869 => {
  val x871 = x869.lbl
  val x872 = x871.Q1__F
  x872}}
x868.flatMap(x870 => hm1005.get({val x873 = x870._1
x873}) match {
 case Some(a) => a.map(v => (v, x870))
 case _ => Nil
}) }
val x879 = x874.flatMap{ case (x875, x876) => 
  val x877 = x876._2
  x877.map(x878 => {
  val nv1006 = ((x875, x876), x878) 
     nv1006
})}
val x880 = Q1__D._2
val x881 = x880.c_orders
val x882 = x881._1
val x884 = x882
val x891 = { val hm1008 = x879.groupBy{ case ((x885, x886), x887) => {
  val x889 = x887.c_orders
  x889}}
x884.flatMap(x888 => hm1008.get({val x890 = x888._1
x890}) match {
 case Some(a) => a.map(v => (v, x888))
 case _ => Nil
}) }
val x898 = x891.flatMap{ case (((x892, x893), x894), x895) => 
  val x896 = x895._2
  x896.map(x897 => {
  val nv1009 = ((((x892, x893), x894), x895), x897) 
     nv1009
})}
val x899 = x881._2
val x900 = x899.o_parts
val x901 = x900._1
val x903 = x901
val x912 = { val hm1010 = x898.groupBy{ case ((((x904, x905), x906), x907), x908) => {
  val x910 = x908.o_parts
  x910}}
x903.flatMap(x909 => hm1010.get({val x911 = x909._1
x911}) match {
 case Some(a) => a.map(v => (v, x909))
 case _ => Nil
}) }
val x921 = x912.flatMap{ case (((((x913, x914), x915), x916), x917), x918) => 
  val x919 = x918._2
  x919.map(x920 => {
  val nv1011 = ((((((x913, x914), x915), x916), x917), x918), x920) 
     nv1011
})}
val x923 = x901
val x934 = { val hm1012 = x921.groupBy{ case ((((((x924, x925), x926), x927), x928), x929), x930) => {
  val x932 = x928.o_parts
  x932}}
x923.flatMap(x931 => hm1012.get({val x933 = x931._1
x933}) match {
 case Some(a) => a.map(v => (v, x931))
 case _ => Nil
}) }
val x948 = x934.flatMap{ case (((((((x935, x936), x937), x938), x939), x940), x941), x942) => 
  val x943 = x942._2
  x943.map(x944 => {
  val nv1013 = ((((((((x935, x936), x937), x938), x939), x940), x941), x942), x944) 
   if({val x945 = x944.p_name
  val x946 = x941.p_name
  val x947 = x945 == x946
  x947}) {  nv1013} else {  Nil}
})}
val x949 = x854.l_qty
val x950 = Record1014(x949)
val x951 = x856.l_qty
val x952 = (1 to x951.asInstanceOf[Int]).map(v => x950)
val x954 = x952
val x965 = { val hm1015 = x948.groupBy{ case ((((((((x955, x956), x957), x958), x959), x960), x961), x962), x963) => {
  true}}
x954.flatMap(x964 => hm1015.get({true}) match {
 case Some(a) => a.map(v => (v, x964))
 case _ => Nil
}) }
val x978 = { val grps1016 = x965.groupBy{ case (((((((((x966, x967), x968), x969), x970), x971), x972), x973), x974), x975) => { val x976 = (x966,x967,x968,x969,x970,x971,x972)
x976 }}
 grps1016.map(x977 => (x977._1, x977._2.foldLeft(0)(acc1017, x977) => acc1017 + 1))).toList
val x993 = { val grps1018 = x978.groupBy{ case ((x979, x980, x981, x982, x983, x984, x985), x986) => { val x987 = (x979)
x987 }}
 grps1018.map(x992 => (x992._1, x992._2.map{case ((x979, x980, x981, x982, x983, x984, x985), x986) => val x988 = x981.c_name
val x989 = x985.p_name
val x990 = x983.o_orderdate
val x991 = Record1020(x988, x989, x990, x986)
x991})).toList }
val x998 = x993.map{ case (x994, x995) => { 
  val x996 = x994.lbl
  val x997 = Record1021(x996, x995)
  x997 }}
val M_flat1 = x998
val x999 = M_flat1
val x1000 = (x863,x999)
x1000
 }
}
