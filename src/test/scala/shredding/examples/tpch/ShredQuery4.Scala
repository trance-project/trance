
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record795(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record796(c_name: String, c_orders: Record795)
case class Record802(o__F: Orders, P__F: Int, L__F: Int)
case class Record803(o_orderdate: String, o_parts: Record802)
case class Record811(p_name: String, l_qty: Double)
case class Record922(lbl: Q1Flat2)
case class Input_Q1__DFlat923(c_name: String, c_orders: Int)
case class Flat924(o_orderdate: String, o_parts: Int)
case class Dict924(o_parts: (List[(Int, List[Record811])], Unit))
case class Input_Q1__DDict923(c_orders: (List[(Int, List[Flat924])], Dict924))
case class Record935(l_qty: Double)
case class Record940(c_name: String, p_name: String, month: String, t_qty: Int)
case class Record941(_1: Q1Flat2, _2: List[Record940])
case class RecM_flat2(_1: Record795, _2: List[Record803])
case class RecM_flat3(_1: Record802, _2: List[Record811])
case class RecM_flat1(_1: Q1Flat, _2: List[Record796])
case class RecM_ctx3(lbl: Record802)
case class RecM_ctx2(lbl: Record795)
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
    val Q1 = { val x653 = Q1Flat(P__F, C__F, L__F, O__F)
val x654 = RecM_ctx1(x653)
val x655 = List(x654)
val M_ctx1 = x655
val x656 = M_ctx1
val x658 = M_ctx1
val x659 = C__D._1
val x661 = x659
val x667 = { val hm792 = x658.groupBy{ case x662 => { val x664 = x662.lbl
val x665 = x664.C__F
x665 } }
 x661.flatMap(v1 => v1._2.map(x663 =>
 hm792.get({val x666 = v1._1
x666}) match {
 case Some(a) => (a.head, x663)
 case _ => (a.head, None)
 })}
val x679 = { val grps793 = x667.groupBy{ case (x668, x669) => { val x670 = (x668)
x670 }}
 grps793.map(x678 => (x678._1, x678._2.map{case (x668, x669) => val x671 = x669.c_name
val x672 = x668.lbl
val x673 = x672.P__F
val x674 = x672.L__F
val x675 = x672.O__F
val x676 = Record795(x673, x674, x675, x669)
val x677 = Record796(x671, x676)
x677})).toList }
val x684 = x679.map{ case (x680, x681) => { 
  val x682 = x680.lbl
  val x683 = RecM_flat1(x682, x681)
  x683 }}
val M_flat1 = x684
val x685 = M_flat1
val x687 = M_flat1
val x691 = x687.flatMap{ case x688 => 
  val x689 = x688._2
  x689.map(x690 => {
  val nv797 = (x688, x690) 
     nv797
})}
val x696 = x691.map{ case (x692, x693) => { 
  val x694 = x693.c_orders
  val x695 = RecM_ctx2(x694)
  x695 }}
val x697 = x696.distinct
val M_ctx2 = x697
val x698 = M_ctx2
val x700 = M_ctx2
val x701 = O__D._1
val x703 = x701
val x714 = { val hm799 = x700.groupBy{ case x704 => { val x706 = x704.lbl
val x707 = x706.O__F
val x708 = x706.c__F
val x709 = x708.c_custkey
val x710 = (x707,x709)
x710 } }
 x703.flatMap(v1 => v1._2.map(x705 =>
 hm799.get({val x711 = v1._1
val x712 = x705.o_custkey
val x713 = (x711,x712)
x713}) match {
 case Some(a) => (a.head, x705)
 case _ => (a.head, None)
 })}
val x725 = { val grps800 = x714.groupBy{ case (x715, x716) => { val x717 = (x715)
x717 }}
 grps800.map(x724 => (x724._1, x724._2.map{case (x715, x716) => val x718 = x716.o_orderdate
val x719 = x715.lbl
val x720 = x719.P__F
val x721 = x719.L__F
val x722 = Record802(x716, x720, x721)
val x723 = Record803(x718, x722)
x723})).toList }
val x730 = x725.map{ case (x726, x727) => { 
  val x728 = x726.lbl
  val x729 = RecM_flat2(x728, x727)
  x729 }}
val M_flat2 = x730
val x731 = M_flat2
val x733 = M_flat2
val x737 = x733.flatMap{ case x734 => 
  val x735 = x734._2
  x735.map(x736 => {
  val nv804 = (x734, x736) 
     nv804
})}
val x742 = x737.map{ case (x738, x739) => { 
  val x740 = x739.o_parts
  val x741 = RecM_ctx3(x740)
  x741 }}
val x743 = x742.distinct
val M_ctx3 = x743
val x744 = M_ctx3
val x746 = M_ctx3
val x747 = L__D._1
val x749 = x747
val x760 = { val hm806 = x746.groupBy{ case x750 => { val x752 = x750.lbl
val x753 = x752.L__F
val x754 = x752.o__F
val x755 = x754.o_orderkey
val x756 = (x753,x755)
x756 } }
 x749.flatMap(v1 => v1._2.map(x751 =>
 hm806.get({val x757 = v1._1
val x758 = x751.l_orderkey
val x759 = (x757,x758)
x759}) match {
 case Some(a) => (a.head, x751)
 case _ => (a.head, None)
 })}
val x761 = P__D._1
val x763 = x761
val x774 = { val hm808 = x760.groupBy{ case (x764, x765) => { val x767 = x764.lbl
val x768 = x767.P__F
val x769 = x765.l_partkey
val x770 = (x768,x769)
x770 } }
 x763.flatMap(v1 => v1._2.map(x766 =>
 hm808.get({val x771 = v1._1
val x772 = x766.p_partkey
val x773 = (x771,x772)
x773}) match {
 case Some(a) => (a.head, x766)
 case _ => (a.head, None)
 })}
val x783 = { val grps809 = x774.groupBy{ case ((x775, x776), x777) => { val x778 = (x775)
x778 }}
 grps809.map(x782 => (x782._1, x782._2.map{case ((x775, x776), x777) => val x779 = x777.p_name
val x780 = x776.l_quantity
val x781 = Record811(x779, x780)
x781})).toList }
val x788 = x783.map{ case (x784, x785) => { 
  val x786 = x784.lbl
  val x787 = RecM_flat3(x786, x785)
  x787 }}
val M_flat3 = x788
val x789 = M_flat3
val x790 = (x656,x685,x698,x731,x744,x789)
x790 }
    var end0 = System.currentTimeMillis() - start0
    
val Q1__F = Q1._1.head.lbl
val Q1__D = (Q1._2, Input_Q1_Dict1((Q1._4, Input_Q1_Dict2((Q1._6, Unit)))))
    def f(){
      val x821 = Q1Flat2(Q1__F)
val x822 = Record922(x821)
val x823 = List(x822)
val M_ctx1 = x823
val x824 = M_ctx1
val x826 = M_ctx1
val x827 = Q1__D._1
val x829 = x827
val x835 = { val hm926 = x826.groupBy{ case x830 => { val x832 = x830.lbl
val x833 = x832.Q1__F
x833 } }
 x829.flatMap(v1 => v1._2.map(x831 =>
 hm926.get({val x834 = v1._1
x834}) match {
 case Some(a) => (a.head, x831)
 case _ => (a.head, None)
 })}
val x836 = Q1__D._2
val x837 = x836.c_orders
val x838 = x837._1
val x840 = x838
val x848 = { val hm928 = x835.groupBy{ case (x841, x842) => {
  val x844 = x842.c_orders
  x844}}
x840.flatMap(x843 => hm928.get({val x845 = Q1__D._2
val x846 = x845.c_orders
val x847 = x846._1
x847}) match {
 case Some(a) => a.map(v => (v, x843))
 case _ => Nil
}) }
val x849 = x837._2
val x850 = x849.o_parts
val x851 = x850._1
val x853 = x851
val x864 = { val hm930 = x848.groupBy{ case ((x854, x855), x856) => {
  val x858 = x856.o_parts
  x858}}
x853.flatMap(x857 => hm930.get({val x859 = Q1__D._2
val x860 = x859.c_orders
val x861 = x860._2
val x862 = x861.o_parts
val x863 = x862._1
x863}) match {
 case Some(a) => a.map(v => (v, x857))
 case _ => Nil
}) }
val x866 = x851
val x882 = { val hm932 = x864.groupBy{ case (((x867, x868), x869), x870) => {
  val x872 = x869.o_parts
  val x873 = x870.p_name
  val x874 = (x872,x873)
  x874}}
x866.flatMap(x871 => hm932.get({val x875 = Q1__D._2
val x876 = x875.c_orders
val x877 = x876._2
val x878 = x877.o_parts
val x879 = x878._1
val x880 = x871.p_name
val x881 = (x879,x880)
x881}) match {
 case Some(a) => a.map(v => (v, x871))
 case _ => Nil
}) }
val x893 = x882.flatMap{ case ((((x883, x884), x885), x886), x887) => 
  val x888 = x886.l_qty
  val x889 = Record935(x888)
  val x890 = x887.l_qty
  val x891 = (1 to x890.asInstanceOf[Int]).map(v => x889)
  x891.map(x892 => {
  val nv934 = (((((x883, x884), x885), x886), x887), x892) 
     nv934
})}
val x902 = { val grps936 = x893.groupBy{ case (((((x894, x895), x896), x897), x898), x899) => { val x900 = (x894,x895,x896,x897)
x900 }}
 grps936.map(x901 => (x901._1, x901._2.foldLeft(0)((acc937, x901) => acc937 + 1))).toList }
val x914 = { val grps938 = x902.groupBy{ case ((x903, x904, x905, x906), x907) => { val x908 = (x903)
x908 }}
 grps938.map(x913 => (x913._1, x913._2.map{case ((x903, x904, x905, x906), x907) => val x909 = x904.c_name
val x910 = x906.p_name
val x911 = x905.o_orderdate
val x912 = Record940(x909, x910, x911, x907)
x912})).toList }
val x919 = x914.map{ case (x915, x916) => { 
  val x917 = x915.lbl
  val x918 = Record941(x917, x916)
  x918 }}
val M_flat1 = x919
val x920 = M_flat1
val x921 = (x824,x920)
x921
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
