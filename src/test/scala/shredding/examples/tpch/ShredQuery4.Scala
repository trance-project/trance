
package experiments
/** Generated code **/
import shredding.examples.tpch._
object ShredQuery4 {
 case class Record821(lbl: Q1Flat)
case class Input_C__DFlat822(c_acctbal: Double, c_name: String, c_nationkey: Int, c_custkey: Int, c_comment: String, c_address: String, c_mktsegment: String, c_phone: String)
case class Record826(P__F: Int, L__F: Int, O__F: Int, c__F: Input_C__DFlat822)
case class Record827(c_name: String, c_orders: Record826)
case class Record828(_1: Q1Flat, _2: Record827)
case class Input_M_flat1(_1: Q1Flat, _2: List[Record827])
case class Record829(lbl: Record826)
case class Input_O__DFlat830(o_shippriority: Int, o_orderdate: String, o_custkey: Int, o_orderpriority: String, o_clerk: String, o_orderstatus: String, o_totalprice: Double, o_orderkey: Int, o_comment: String)
case class Record834(o__F: Input_O__DFlat830, P__F: Int, L__F: Int)
case class Record835(o_orderdate: String, o_parts: Record834)
case class Record836(_1: Record826, _2: Record835)
case class Input_M_flat2(_1: Record826, _2: List[Record835])
case class Record837(lbl: Record834)
case class Input_L__DFlat838(l_returnflag: String, l_comment: String, l_linestatus: String, l_shipmode: String, l_shipinstruct: String, l_quantity: Double, l_receiptdate: String, l_linenumber: Int, l_tax: Double, l_shipdate: String, l_extendedprice: Double, l_partkey: Int, l_discount: Double, l_commitdate: String, l_suppkey: Int, l_orderkey: Int)
case class Input_P__DFlat840(p_comment: String, p_name: String, p_type: String, p_retailprice: Double, p_mfgr: String, p_container: String, p_brand: String, p_size: Int, p_partkey: Int)
case class Record844(p_name: String, l_qty: Double)
case class Record845(_1: Record834, _2: Record844)
case class Record1000(lbl: Q1Flat2)
case class Input_Q1__DFlat1001(c_name: String, c_orders: Int)
case class Flat1002(o_orderdate: String, o_parts: Int)
case class Dict1002(o_parts: (List[(Int, List[Record844])], Unit))
case class Input_Q1__DDict1001(c_orders: (List[(Int, List[Flat1002])], Dict1002))
case class Record1009(l_qty: Double)
case class Record1015(c_name: String, p_name: String, month: String, t_qty: Int)
case class Record1016(_1: Q1Flat2, _2: Record1015)
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
    val foo = { val x657 = Q1Flat(P__F, C__F, L__F, O__F)
val x658 = Record821(x657)
val x659 = List(x658)
val M_ctx1 = x659
val x660 = M_ctx1
val x662 = M_ctx1
val x663 = C__D._1
val x665 = x663
val x671 = { val hm823 = x662.groupBy{ case x666 => {
  val x668 = x666.lbl
  val x669 = x668.C__F
  x669}}
x665.flatMap(x667 => hm823.get({val x670 = x667._1
x670}) match {
 case Some(a) => a.map(v => (v, x667))
 case _ => Nil
}) }
val x676 = 
x671.flatMap{(x672, x673) => 
  val x674 = x673._2
  x674.map(x675 => {
  val nv = List(x672, x673) :+ x675 
   if (true) { nv } else { Nil }
)}
val x689 = { val grps824 = x676.groupBy{ case ((x677, x678), x679) => { val x680 = (x677)
x680 }}
 grps824.map(x688 => (x688._1, x688._2.map{case ((x677, x678), x679) => val x681 = x679.c_name
val x682 = x677.lbl
val x683 = x682.P__F
val x684 = x682.L__F
val x685 = x682.O__F
val x686 = Record826(x683, x684, x685, x679)
val x687 = Record827(x681, x686)
x687})).toList }
val x694 = x689.map{ case (x690, x691) => { 
  val x692 = x690.lbl
  val x693 = Record828(x692, x691)
  x693 }}
val M_flat1 = x694
val x695 = M_flat1
val x697 = M_flat1
val x701 = 
x697.flatMap{x698 => 
  val x699 = x698._2
  x699.map(x700 => {
  val nv = Listx698 :+ x700 
   if (true) { nv } else { Nil }
)}
val x706 = x701.map{ case (x702, x703) => { 
  val x704 = x703.c_orders
  val x705 = Record829(x704)
  x705 }}
val x707 = x706.distinct
val M_ctx2 = x707
val x708 = M_ctx2
val x710 = M_ctx2
val x711 = O__D._1
val x713 = x711
val x719 = { val hm831 = x710.groupBy{ case x714 => {
  val x716 = x714.lbl
  val x717 = x716.O__F
  x717}}
x713.flatMap(x715 => hm831.get({val x718 = x715._1
x718}) match {
 case Some(a) => a.map(v => (v, x715))
 case _ => Nil
}) }
val x729 = 
x719.flatMap{(x720, x721) => 
  val x722 = x721._2
  x722.map(x723 => {
  val nv = List(x720, x721) :+ x723 
   if (val x724 = x723.o_custkey
  val x725 = x720.lbl
  val x726 = x725.c__F
  val x727 = x726.c_custkey
  val x728 = x724 == x727
  x728) { nv } else { Nil }
)}
val x741 = { val grps832 = x729.groupBy{ case ((x730, x731), x732) => { val x733 = (x730)
x733 }}
 grps832.map(x740 => (x740._1, x740._2.map{case ((x730, x731), x732) => val x734 = x732.o_orderdate
val x735 = x730.lbl
val x736 = x735.P__F
val x737 = x735.L__F
val x738 = Record834(x732, x736, x737)
val x739 = Record835(x734, x738)
x739})).toList }
val x746 = x741.map{ case (x742, x743) => { 
  val x744 = x742.lbl
  val x745 = Record836(x744, x743)
  x745 }}
val M_flat2 = x746
val x747 = M_flat2
val x749 = M_flat2
val x753 = 
x749.flatMap{x750 => 
  val x751 = x750._2
  x751.map(x752 => {
  val nv = Listx750 :+ x752 
   if (true) { nv } else { Nil }
)}
val x758 = x753.map{ case (x754, x755) => { 
  val x756 = x755.o_parts
  val x757 = Record837(x756)
  x757 }}
val x759 = x758.distinct
val M_ctx3 = x759
val x760 = M_ctx3
val x762 = M_ctx3
val x763 = L__D._1
val x765 = x763
val x771 = { val hm839 = x762.groupBy{ case x766 => {
  val x768 = x766.lbl
  val x769 = x768.L__F
  x769}}
x765.flatMap(x767 => hm839.get({val x770 = x767._1
x770}) match {
 case Some(a) => a.map(v => (v, x767))
 case _ => Nil
}) }
val x781 = 
x771.flatMap{(x772, x773) => 
  val x774 = x773._2
  x774.map(x775 => {
  val nv = List(x772, x773) :+ x775 
   if (val x776 = x775.l_orderkey
  val x777 = x772.lbl
  val x778 = x777.o__F
  val x779 = x778.o_orderkey
  val x780 = x776 == x779
  x780) { nv } else { Nil }
)}
val x782 = P__D._1
val x784 = x782
val x792 = { val hm841 = x781.groupBy{ case ((x785, x786), x787) => {
  val x789 = x785.lbl
  val x790 = x789.P__F
  x790}}
x784.flatMap(x788 => hm841.get({val x791 = x788._1
x791}) match {
 case Some(a) => a.map(v => (v, x788))
 case _ => Nil
}) }
val x802 = 
x792.flatMap{(((x793, x794), x795), x796) => 
  val x797 = x796._2
  x797.map(x798 => {
  val nv = List(((x793, x794), x795), x796) :+ x798 
   if (val x799 = x795.l_partkey
  val x800 = x798.p_partkey
  val x801 = x799 == x800
  x801) { nv } else { Nil }
)}
val x813 = { val grps842 = x802.groupBy{ case ((((x803, x804), x805), x806), x807) => { val x808 = (x803)
x808 }}
 grps842.map(x812 => (x812._1, x812._2.map{case ((((x803, x804), x805), x806), x807) => val x809 = x807.p_name
val x810 = x805.l_quantity
val x811 = Record844(x809, x810)
x811})).toList }
val x818 = x813.map{ case (x814, x815) => { 
  val x816 = x814.lbl
  val x817 = Record845(x816, x815)
  x817 }}
val M_flat3 = x818
val x819 = M_flat3
val x820 = (x660,x695,x708,x747,x760,x819)
x820 }
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
  val x859 = Q1Flat2(Q1__F)
val x860 = Record1000(x859)
val x861 = List(x860)
val M_ctx1 = x861
val x862 = M_ctx1
val x864 = M_ctx1
val x865 = Q1__D._1
val x867 = x865
val x873 = { val hm1004 = x864.groupBy{ case x868 => {
  val x870 = x868.lbl
  val x871 = x870.Q1__F
  x871}}
x867.flatMap(x869 => hm1004.get({val x872 = x869._1
x872}) match {
 case Some(a) => a.map(v => (v, x869))
 case _ => Nil
}) }
val x878 = 
x873.flatMap{(x874, x875) => 
  val x876 = x875._2
  x876.map(x877 => {
  val nv = List(x874, x875) :+ x877 
   if (true) { nv } else { Nil }
)}
val x879 = Q1__D._2
val x880 = x879.c_orders
val x881 = x880._1
val x883 = x881
val x890 = { val hm1006 = x878.groupBy{ case ((x884, x885), x886) => {
  val x888 = x886.c_orders
  x888}}
x883.flatMap(x887 => hm1006.get({val x889 = x887._1
x889}) match {
 case Some(a) => a.map(v => (v, x887))
 case _ => Nil
}) }
val x897 = 
x890.flatMap{(((x891, x892), x893), x894) => 
  val x895 = x894._2
  x895.map(x896 => {
  val nv = List(((x891, x892), x893), x894) :+ x896 
   if (true) { nv } else { Nil }
)}
val x898 = x880._2
val x899 = x898.o_parts
val x900 = x899._1
val x902 = x900
val x911 = { val hm1007 = x897.groupBy{ case ((((x903, x904), x905), x906), x907) => {
  val x909 = x907.o_parts
  x909}}
x902.flatMap(x908 => hm1007.get({val x910 = x908._1
x910}) match {
 case Some(a) => a.map(v => (v, x908))
 case _ => Nil
}) }
val x920 = 
x911.flatMap{(((((x912, x913), x914), x915), x916), x917) => 
  val x918 = x917._2
  x918.map(x919 => {
  val nv = List(((((x912, x913), x914), x915), x916), x917) :+ x919 
   if (true) { nv } else { Nil }
)}
val x922 = x900
val x933 = { val hm1008 = x920.groupBy{ case ((((((x923, x924), x925), x926), x927), x928), x929) => {
  val x931 = x927.o_parts
  x931}}
x922.flatMap(x930 => hm1008.get({val x932 = x930._1
x932}) match {
 case Some(a) => a.map(v => (v, x930))
 case _ => Nil
}) }
val x947 = 
x933.flatMap{(((((((x934, x935), x936), x937), x938), x939), x940), x941) => 
  val x942 = x941._2
  x942.map(x943 => {
  val nv = List(((((((x934, x935), x936), x937), x938), x939), x940), x941) :+ x943 
   if (val x944 = x943.p_name
  val x945 = x940.p_name
  val x946 = x944 == x945
  x946) { nv } else { Nil }
)}
val x948 = x853.l_qty
val x949 = Record1009(x948)
val x950 = x855.l_qty
val x951 = (1 to x950.asInstanceOf[Int]).map(v => x949)
val x953 = x951
val x964 = { val hm1010 = x947.groupBy{ case ((((((((x954, x955), x956), x957), x958), x959), x960), x961), x962) => {
  true}}
x953.flatMap(x963 => hm1010.get({true}) match {
 case Some(a) => a.map(v => (v, x963))
 case _ => Nil
}) }
val x977 = { val grps1011 = x964.groupBy{ case (((((((((x965, x966), x967), x968), x969), x970), x971), x972), x973), x974) => { val x975 = (x965,x966,x967,x968,x969,x970,x971)
x975 }}
 grps1011.map(x976 => (x976._1, x976._2.foldLeft(0)(acc1012, x976) => acc1012 + 1))).toList
val x992 = { val grps1013 = x977.groupBy{ case (x978, x979, x980, x981, x982, x983, x984, x985) => { val x986 = (x978)
x986 }}
 grps1013.map(x991 => (x991._1, x991._2.map{case (x978, x979, x980, x981, x982, x983, x984, x985) => val x987 = x980.c_name
val x988 = x984.p_name
val x989 = x982.o_orderdate
val x990 = Record1015(x987, x988, x989, x985)
x990})).toList }
val x997 = x992.map{ case (x993, x994) => { 
  val x995 = x993.lbl
  val x996 = Record1016(x995, x994)
  x996 }}
val M_flat1 = x997
val x998 = M_flat1
val x999 = (x862,x998)
x999
 }
}
