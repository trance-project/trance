
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record784(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record785(c_name: String, c_orders: Record784)
case class Record791(o__F: Orders, P__F: Int, L__F: Int)
case class Record792(o_orderdate: String, o_parts: Record791)
case class Record800(p_name: String, l_qty: Double)
case class Record887(lbl: Q1Flat2)
case class Input_Q1__DFlat888(c_name: String, c_orders: Int)
case class Flat889(o_orderdate: String, o_parts: Int)
case class Dict889(o_parts: (List[(Int, List[Record800])], Unit))
case class Input_Q1__DDict888(c_orders: (List[(Int, List[Flat889])], Dict889))
case class Record900(c_name: String, p_name: String, month: String, t_qty: Double)
case class Record901(_1: Q1Flat2, _2: List[Record900])
case class RecM_flat2(_1: Record784, _2: List[Record792])
case class RecM_flat3(_1: Record791, _2: List[Record800])
case class RecM_flat1(_1: Q1Flat, _2: List[Record785])
case class RecM_ctx3(lbl: Record791)
case class RecM_ctx2(lbl: Record784)
case class RecM_ctx1(lbl: Q1Flat)

case class Input_Q1_Dict2(o_parts: (List[RecM_flat3], Unit))
case class Input_Q1_Dict1(c_orders: (List[RecM_flat2], Input_Q1_Dict2))
object ShredQuery4 {
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
    val Q1 = { val x650 = Q1Flat(P__F, C__F, L__F, O__F)
val x651 = RecM_ctx1(x650)
val x652 = List(x651)
val M_ctx1 = x652
val x653 = M_ctx1
val x655 = M_ctx1
val x656 = C__D._1
val x658 = x656
val x663 = { val hm781 = x655.groupBy{case x659 => { val x661 = x659.lbl
val x662 = x661.C__F
x662 } }
 x658.flatMap{x660 => hm781.get(x660._1) match {
 case Some(a) => a.map(a1 => (a1, x660._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x675 = { val grps782 = x663.groupBy{ case (x664, x665) => { val x666 = (x664)
x666 }}
 grps782.map(x674 => (x674._1, x674._2.map{case (x664, x665) => val x667 = x665.c_name
val x668 = x664.lbl
val x669 = x668.P__F
val x670 = x668.L__F
val x671 = x668.O__F
val x672 = Record784(x669, x670, x671, x665)
val x673 = Record785(x667, x672)
x673})).toList }
val x680 = x675.map{ case (x676, x677) => { 
  val x678 = x676.lbl
  val x679 = RecM_flat1(x678, x677)
  x679 }}
val M_flat1 = x680
val x681 = M_flat1
val x683 = M_flat1
val x687 = x683.flatMap{ case x684 => 
  val x685 = x684._2
  x685.map(x686 => {
  val nv786 = (x684, x686) 
     nv786
})}
val x692 = x687.map{ case (x688, x689) => { 
  val x690 = x689.c_orders
  val x691 = RecM_ctx2(x690)
  x691 }}
val x693 = x692.distinct
val M_ctx2 = x693
val x694 = M_ctx2
val x696 = M_ctx2
val x697 = O__D._1
val x699 = x697
val x708 = { val hm788 = x696.groupBy{case x700 => { val x702 = x700.lbl
val x703 = x702.O__F
x703 } }
 val join1 = x699.flatMap{x701 => hm788.get(x701._1) match {
 case Some(a) => x701._2
 case _ => Nil
 }}
 val join2 = x696.groupBy{case x700 => { val x705 = x700.lbl
val x706 = x705.c__F
val x707 = x706.c_custkey
x707 } }
 join1.flatMap(x701 => join2.get({ val x704 = x701.o_custkey
x704 }) match {
   case Some(a) => a.map(a1 => (a1, x701))
   case _ => Nil
 })
}
val x719 = { val grps789 = x708.groupBy{ case (x709, x710) => { val x711 = (x709)
x711 }}
 grps789.map(x718 => (x718._1, x718._2.map{case (x709, x710) => val x712 = x710.o_orderdate
val x713 = x709.lbl
val x714 = x713.P__F
val x715 = x713.L__F
val x716 = Record791(x710, x714, x715)
val x717 = Record792(x712, x716)
x717})).toList }
val x724 = x719.map{ case (x720, x721) => { 
  val x722 = x720.lbl
  val x723 = RecM_flat2(x722, x721)
  x723 }}
val M_flat2 = x724
val x725 = M_flat2
val x727 = M_flat2
val x731 = x727.flatMap{ case x728 => 
  val x729 = x728._2
  x729.map(x730 => {
  val nv793 = (x728, x730) 
     nv793
})}
val x736 = x731.map{ case (x732, x733) => { 
  val x734 = x733.o_parts
  val x735 = RecM_ctx3(x734)
  x735 }}
val x737 = x736.distinct
val M_ctx3 = x737
val x738 = M_ctx3
val x740 = M_ctx3
val x741 = L__D._1
val x743 = x741
val x752 = { val hm795 = x740.groupBy{case x744 => { val x746 = x744.lbl
val x747 = x746.L__F
x747 } }
 val join1 = x743.flatMap{x745 => hm795.get(x745._1) match {
 case Some(a) => x745._2
 case _ => Nil
 }}
 val join2 = x740.groupBy{case x744 => { val x749 = x744.lbl
val x750 = x749.o__F
val x751 = x750.o_orderkey
x751 } }
 join1.flatMap(x745 => join2.get({ val x748 = x745.l_orderkey
x748 }) match {
   case Some(a) => a.map(a1 => (a1, x745))
   case _ => Nil
 })
}
val x753 = P__D._1
val x755 = x753
val x763 = { val hm797 = x752.groupBy{case (x756, x757) => { val x759 = x756.lbl
val x760 = x759.P__F
x760 } }
 val join1 = x755.flatMap{x758 => hm797.get(x758._1) match {
 case Some(a) => x758._2
 case _ => Nil
 }}
 val join2 = x752.groupBy{case (x756, x757) => { val x762 = x757.l_partkey
x762 } }
 join1.flatMap(x758 => join2.get({ val x761 = x758.p_partkey
x761 }) match {
   case Some(a) => a.map(a1 => (a1, x758))
   case _ => Nil
 })
}
val x772 = { val grps798 = x763.groupBy{ case ((x764, x765), x766) => { val x767 = (x764)
x767 }}
 grps798.map(x771 => (x771._1, x771._2.map{case ((x764, x765), x766) => val x768 = x766.p_name
val x769 = x765.l_quantity
val x770 = Record800(x768, x769)
x770})).toList }
val x777 = x772.map{ case (x773, x774) => { 
  val x775 = x773.lbl
  val x776 = RecM_flat3(x775, x774)
  x776 }}
val M_flat3 = x777
val x778 = M_flat3
val x779 = (x653,x681,x694,x725,x738,x778)
x779 }
    var end0 = System.currentTimeMillis() - start0
    
val Q1__F = Q1._1.head.lbl
val Q1__D = (Q1._2, Input_Q1_Dict1((Q1._4, Input_Q1_Dict2((Q1._6, Unit)))))
    def f(){
      val x813 = Q1Flat2(Q1__F)
val x814 = Record887(x813)
val x815 = List(x814)
val M_ctx1 = x815
val x816 = M_ctx1
val x818 = M_ctx1
val x819 = Q1__D._1
val x821 = x819
val x826 = { val hm891 = x818.groupBy{case x822 => { val x824 = x822.lbl
val x825 = x824.Q1__F
x825 } }
 x821.flatMap{x823 => hm891.get(x823._1) match {
 case Some(a) => a.map(a1 => (a1, x823._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x827 = Q1__D._2
val x828 = x827.c_orders
val x829 = x828._1
val x831 = x829
val x836 = { val hm893 = x826.groupBy{case (x832, x833) => { val x835 = x833.c_orders
x835 } }
 x831.flatMap{x834 => hm893.get(x834._1) match {
 case Some(a) => a.map(a1 => (a1, x834._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x837 = x828._2
val x838 = x837.o_parts
val x839 = x838._1
val x841 = x839
val x847 = { val hm894 = x836.groupBy{case ((x842, x843), x844) => { val x846 = x844.o_parts
x846 } }
 x841.flatMap{x845 => hm894.get(x845._1) match {
 case Some(a) => a.map(a1 => (a1, x845._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x849 = x839
val x858 = { val hm895 = x847.groupBy{case (((x850, x851), x852), x853) => { val x855 = x852.o_parts
x855 } }
 val join1 = x849.flatMap{x854 => hm895.get(x854._1) match {
 case Some(a) => x854._2
 case _ => Nil
 }}
 val join2 = x847.groupBy{case (((x850, x851), x852), x853) => { val x857 = x853.p_name
x857 } }
 join1.flatMap(x854 => join2.get({ val x856 = x854.p_name
x856 }) match {
   case Some(a) => a.map(a1 => (a1, x854))
   case _ => Nil
 })
}
val x867 = { val grps896 = x858.groupBy{ case ((((x859, x860), x861), x862), x863) => { val x864 = (x859,x860,x861,x862)
x864 }}
 grps896.map(x866 => (x866._1, x866._2.foldLeft(0.0){ case (acc897, ((((x859, x860), x861), x862), x863)) => acc897 + {val x865 = x863.l_qty
x865} })).toList }
val x879 = { val grps898 = x867.groupBy{ case ((x868, x869, x870, x871), x872) => { val x873 = (x868)
x873 }}
 grps898.map(x878 => (x878._1, x878._2.map{case ((x868, x869, x870, x871), x872) => val x874 = x869.c_name
val x875 = x871.p_name
val x876 = x870.o_orderdate
val x877 = Record900(x874, x875, x876, x872)
x877})).toList }
val x884 = x879.map{ case (x880, x881) => { 
  val x882 = x880.lbl
  val x883 = Record901(x882, x881)
  x883 }}
val M_flat1 = x884
val x885 = M_flat1
val x886 = (x816,x885)
x886
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
