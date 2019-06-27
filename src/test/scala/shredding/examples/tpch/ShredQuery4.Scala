
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record825(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record826(c_name: String, c_orders: Record825)
case class Record833(o__F: Orders, P__F: Int, L__F: Int)
case class Record834(o_orderdate: String, o_parts: Record833)
case class Record844(p_name: String, l_qty: Double)
case class Record997(lbl: Q1Flat2)
case class Input_Q1__DFlat998(c_name: String, c_orders: Int)
case class Flat999(o_orderdate: String, o_parts: Int)
case class Dict999(o_parts: (List[(Int, List[Record844])], Unit))
case class Input_Q1__DDict998(c_orders: (List[(Int, List[Flat999])], Dict999))
case class Record1011(l_qty: Double)
case class Record1016(c_name: String, p_name: String, month: String, t_qty: Int)
case class Record1017(_1: Q1Flat2, _2: List[Record1016])
case class RecM_flat2(_1: Record825, _2: List[Record834])
case class RecM_flat3(_1: Record833, _2: List[Record844])
case class RecM_flat1(_1: Q1Flat, _2: List[Record826])
case class RecM_ctx3(lbl: Record833)
case class RecM_ctx2(lbl: Record825)
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
    val Q1 = { val x656 = Q1Flat(P__F, C__F, L__F, O__F)
val x657 = RecM_ctx1(x656)
val x658 = List(x657)
val M_ctx1 = x658
val x659 = M_ctx1
val x661 = M_ctx1
val x662 = C__D._1
val x664 = x662
val x670 = { val hm821 = x661.groupBy{ case x665 => {
  val x667 = x665.lbl
  val x668 = x667.C__F
  x668}}
x664.flatMap(x666 => hm821.get({val x669 = x666._1
x669}) match {
 case Some(a) => a.map(v => (v, x666))
 case _ => Nil
}) }
val x675 = x670.flatMap{ case (x671, x672) => 
  val x673 = x672._2
  x673.map(x674 => {
  val nv822 = ((x671, x672), x674) 
     nv822
})}
val x688 = { val grps823 = x675.groupBy{ case ((x676, x677), x678) => { val x679 = (x676)
x679 }}
 grps823.map(x687 => (x687._1, x687._2.map{case ((x676, x677), x678) => val x680 = x678.c_name
val x681 = x676.lbl
val x682 = x681.P__F
val x683 = x681.L__F
val x684 = x681.O__F
val x685 = Record825(x682, x683, x684, x678)
val x686 = Record826(x680, x685)
x686})).toList }
val x693 = x688.map{ case (x689, x690) => { 
  val x691 = x689.lbl
  val x692 = RecM_flat1(x691, x690)
  x692 }}
val M_flat1 = x693
val x694 = M_flat1
val x696 = M_flat1
val x700 = x696.flatMap{ case x697 => 
  val x698 = x697._2
  x698.map(x699 => {
  val nv827 = (x697, x699) 
     nv827
})}
val x705 = x700.map{ case (x701, x702) => { 
  val x703 = x702.c_orders
  val x704 = RecM_ctx2(x703)
  x704 }}
val x706 = x705.distinct
val M_ctx2 = x706
val x707 = M_ctx2
val x709 = M_ctx2
val x710 = O__D._1
val x712 = x710
val x718 = { val hm829 = x709.groupBy{ case x713 => {
  val x715 = x713.lbl
  val x716 = x715.O__F
  x716}}
x712.flatMap(x714 => hm829.get({val x717 = x714._1
x717}) match {
 case Some(a) => a.map(v => (v, x714))
 case _ => Nil
}) }
val x728 = x718.flatMap{ case (x719, x720) => 
  val x721 = x720._2
  x721.flatMap(x722 => {
  val nv830 = ((x719, x720), x722) 
   if({val x723 = x722.o_custkey
  val x724 = x719.lbl
  val x725 = x724.c__F
  val x726 = x725.c_custkey
  val x727 = x723 == x726
  x727}) {  List(nv830)} else {  Nil}
})}
val x740 = { val grps831 = x728.groupBy{ case ((x729, x730), x731) => { val x732 = (x729)
x732 }}
 grps831.map(x739 => (x739._1, x739._2.map{case ((x729, x730), x731) => val x733 = x731.o_orderdate
val x734 = x729.lbl
val x735 = x734.P__F
val x736 = x734.L__F
val x737 = Record833(x731, x735, x736)
val x738 = Record834(x733, x737)
x738})).toList }
val x745 = x740.map{ case (x741, x742) => { 
  val x743 = x741.lbl
  val x744 = RecM_flat2(x743, x742)
  x744 }}
val M_flat2 = x745
val x746 = M_flat2
val x748 = M_flat2
val x752 = x748.flatMap{ case x749 => 
  val x750 = x749._2
  x750.map(x751 => {
  val nv835 = (x749, x751) 
     nv835
})}
val x757 = x752.map{ case (x753, x754) => { 
  val x755 = x754.o_parts
  val x756 = RecM_ctx3(x755)
  x756 }}
val x758 = x757.distinct
val M_ctx3 = x758
val x759 = M_ctx3
val x761 = M_ctx3
val x762 = L__D._1
val x764 = x762
val x770 = { val hm837 = x761.groupBy{ case x765 => {
  val x767 = x765.lbl
  val x768 = x767.L__F
  x768}}
x764.flatMap(x766 => hm837.get({val x769 = x766._1
x769}) match {
 case Some(a) => a.map(v => (v, x766))
 case _ => Nil
}) }
val x780 = x770.flatMap{ case (x771, x772) => 
  val x773 = x772._2
  x773.flatMap(x774 => {
  val nv838 = ((x771, x772), x774) 
   if({val x775 = x774.l_orderkey
  val x776 = x771.lbl
  val x777 = x776.o__F
  val x778 = x777.o_orderkey
  val x779 = x775 == x778
  x779}) {  List(nv838)} else {  Nil}
})}
val x781 = P__D._1
val x783 = x781
val x791 = { val hm840 = x780.groupBy{ case ((x784, x785), x786) => {
  val x788 = x784.lbl
  val x789 = x788.P__F
  x789}}
x783.flatMap(x787 => hm840.get({val x790 = x787._1
x790}) match {
 case Some(a) => a.map(v => (v, x787))
 case _ => Nil
}) }
val x801 = x791.flatMap{ case (((x792, x793), x794), x795) => 
  val x796 = x795._2
  x796.flatMap(x797 => {
  val nv841 = ((((x792, x793), x794), x795), x797) 
   if({val x798 = x794.l_partkey
  val x799 = x797.p_partkey
  val x800 = x798 == x799
  x800}) {  List(nv841)} else {  Nil}
})}
val x812 = { val grps842 = x801.groupBy{ case ((((x802, x803), x804), x805), x806) => { val x807 = (x802)
x807 }}
 grps842.map(x811 => (x811._1, x811._2.map{case ((((x802, x803), x804), x805), x806) => val x808 = x806.p_name
val x809 = x804.l_quantity
val x810 = Record844(x808, x809)
x810})).toList }
val x817 = x812.map{ case (x813, x814) => { 
  val x815 = x813.lbl
  val x816 = RecM_flat3(x815, x814)
  x816 }}
val M_flat3 = x817
val x818 = M_flat3
val x819 = (x659,x694,x707,x746,x759,x818)
x819 }
    var end0 = System.currentTimeMillis() - start0
    
val Q1__F = Q1._1.head.lbl
val Q1__D = (Q1._2, Input_Q1_Dict1((Q1._4, Input_Q1_Dict2((Q1._6, Unit)))))
    def f(){
      val x858 = Q1Flat2(Q1__F)
val x859 = Record997(x858)
val x860 = List(x859)
val M_ctx1 = x860
val x861 = M_ctx1
val x863 = M_ctx1
val x864 = Q1__D._1
val x866 = x864
val x872 = { val hm1001 = x863.groupBy{ case x867 => {
  val x869 = x867.lbl
  val x870 = x869.Q1__F
  x870}}
x866.flatMap(x868 => hm1001.get({val x871 = x868._1
x871}) match {
 case Some(a) => a.map(v => (v, x868))
 case _ => Nil
}) }
val x877 = x872.flatMap{ case (x873, x874) => 
  val x875 = x874._2
  x875.map(x876 => {
  val nv1002 = ((x873, x874), x876) 
     nv1002
})}
val x878 = Q1__D._2
val x879 = x878.c_orders
val x880 = x879._1
val x882 = x880
val x889 = { val hm1004 = x877.groupBy{ case ((x883, x884), x885) => {
  val x887 = x885.c_orders
  x887}}
x882.flatMap(x886 => hm1004.get({val x888 = x886._1
x888}) match {
 case Some(a) => a.map(v => (v, x886))
 case _ => Nil
}) }
val x896 = x889.flatMap{ case (((x890, x891), x892), x893) => 
  val x894 = x893._2
  x894.map(x895 => {
  val nv1005 = ((((x890, x891), x892), x893), x895) 
     nv1005
})}
val x897 = x879._2
val x898 = x897.o_parts
val x899 = x898._1
val x901 = x899
val x910 = { val hm1006 = x896.groupBy{ case ((((x902, x903), x904), x905), x906) => {
  val x908 = x906.o_parts
  x908}}
x901.flatMap(x907 => hm1006.get({val x909 = x907._1
x909}) match {
 case Some(a) => a.map(v => (v, x907))
 case _ => Nil
}) }
val x919 = x910.flatMap{ case (((((x911, x912), x913), x914), x915), x916) => 
  val x917 = x916._2
  x917.map(x918 => {
  val nv1007 = ((((((x911, x912), x913), x914), x915), x916), x918) 
     nv1007
})}
val x921 = x899
val x932 = { val hm1008 = x919.groupBy{ case ((((((x922, x923), x924), x925), x926), x927), x928) => {
  val x930 = x926.o_parts
  x930}}
x921.flatMap(x929 => hm1008.get({val x931 = x929._1
x931}) match {
 case Some(a) => a.map(v => (v, x929))
 case _ => Nil
}) }
val x946 = x932.flatMap{ case (((((((x933, x934), x935), x936), x937), x938), x939), x940) => 
  val x941 = x940._2
  x941.flatMap(x942 => {
  val nv1009 = ((((((((x933, x934), x935), x936), x937), x938), x939), x940), x942) 
   if({val x943 = x942.p_name
  val x944 = x939.p_name
  val x945 = x943 == x944
  x945}) {  List(nv1009)} else {  Nil}
})}
val x961 = x946.flatMap{ case ((((((((x947, x948), x949), x950), x951), x952), x953), x954), x955) => 
  val x956 = x953.l_qty
  val x957 = Record1011(x956)
  val x958 = x955.l_qty
  val x959 = (1 to x958.asInstanceOf[Int]).map(v => x957)
  x959.map(x960 => {
  val nv1010 = (((((((((x947, x948), x949), x950), x951), x952), x953), x954), x955), x960) 
     nv1010
})}
val x974 = { val grps1012 = x961.groupBy{ case (((((((((x962, x963), x964), x965), x966), x967), x968), x969), x970), x971) => { val x972 = (x962,x963,x964,x965,x966,x967,x968)
x972 }}
 grps1012.map(x973 => (x973._1, x973._2.foldLeft(0)((acc1013, x973) => acc1013 + 1))).toList }
val x989 = { val grps1014 = x974.groupBy{ case ((x975, x976, x977, x978, x979, x980, x981), x982) => { val x983 = (x975)
x983 }}
 grps1014.map(x988 => (x988._1, x988._2.map{case ((x975, x976, x977, x978, x979, x980, x981), x982) => val x984 = x977.c_name
val x985 = x981.p_name
val x986 = x979.o_orderdate
val x987 = Record1016(x984, x985, x986, x982)
x987})).toList }
val x994 = x989.map{ case (x990, x991) => { 
  val x992 = x990.lbl
  val x993 = Record1017(x992, x991)
  x993 }}
val M_flat1 = x994
val x995 = M_flat1
val x996 = (x861,x995)
x996
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
