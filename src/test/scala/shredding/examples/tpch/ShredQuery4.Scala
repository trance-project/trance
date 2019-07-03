
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record758(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record759(c_name: String, c_orders: Record758)
case class Record765(o__F: Orders, P__F: Int, L__F: Int)
case class Record766(o_orderdate: String, o_parts: Record765)
case class Record774(p_name: String, l_qty: Double)
case class Record857(lbl: Q1Flat2)
case class Input_Q1__DFlat858(c_name: String, c_orders: Int)
case class Flat859(o_orderdate: String, o_parts: Int)
case class Dict859(o_parts: (List[(Int, List[Record774])], Unit))
case class Input_Q1__DDict858(c_orders: (List[(Int, List[Flat859])], Dict859))
case class Record870(c_name: String, p_name: String, month: String, t_qty: Double)
case class Record871(_1: Q1Flat2, _2: List[Record870])
case class RecM_flat2(_1: Record758, _2: List[Record766])
case class RecM_flat3(_1: Record765, _2: List[Record774])
case class RecM_flat1(_1: Q1Flat, _2: List[Record759])
case class RecM_ctx3(lbl: Record765)
case class RecM_ctx2(lbl: Record758)
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
    val Q1 = { val x624 = Q1Flat(P__F, C__F, L__F, O__F)
val x625 = RecM_ctx1(x624)
val x626 = List(x625)
val M_ctx1 = x626
val x627 = M_ctx1
val x629 = M_ctx1
val x630 = C__D._1
val x632 = x630
val x637 = { val hm755 = x629.groupBy{case x633 => { val x635 = x633.lbl
val x636 = x635.C__F
x636 } }
 x632.flatMap{x634 => hm755.get(x634._1) match {
 case Some(a) => a.map(a1 => (a1, x634._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x649 = { val grps756 = x637.groupBy{ case (x638, x639) => { val x640 = (x638)
x640 }}
 grps756.map(x648 => (x648._1, x648._2.map{case (x638, x639) => val x641 = x639.c_name
val x642 = x638.lbl
val x643 = x642.P__F
val x644 = x642.L__F
val x645 = x642.O__F
val x646 = Record758(x643, x644, x645, x639)
val x647 = Record759(x641, x646)
x647})).toList }
val x654 = x649.map{ case (x650, x651) => { 
  val x652 = x650.lbl
  val x653 = RecM_flat1(x652, x651)
  x653 }}
val M_flat1 = x654
val x655 = M_flat1
val x657 = M_flat1
val x661 = x657.flatMap{ case x658 => 
  val x659 = x658._2
  x659.map(x660 => {
  val nv760 = (x658, x660) 
     nv760
})}
val x666 = x661.map{ case (x662, x663) => { 
  val x664 = x663.c_orders
  val x665 = RecM_ctx2(x664)
  x665 }}
val x667 = x666.distinct
val M_ctx2 = x667
val x668 = M_ctx2
val x670 = M_ctx2
val x671 = O__D._1
val x673 = x671
val x682 = { val hm762 = x670.groupBy{case x674 => { val x676 = x674.lbl
val x677 = x676.O__F
x677 } }
 val join1 = x673.flatMap{x675 => hm762.get(x675._1) match {
 case Some(a) => x675._2
 case _ => Nil
 }}
 val join2 = x670.groupBy{case x674 => { val x679 = x674.lbl
val x680 = x679.c__F
val x681 = x680.c_custkey
x681 } }
 join1.flatMap(x675 => join2.get({ val x678 = x675.o_custkey
x678 }) match {
   case Some(a) => a.map(a1 => (a1, x675))
   case _ => Nil
 })
}
val x693 = { val grps763 = x682.groupBy{ case (x683, x684) => { val x685 = (x683)
x685 }}
 grps763.map(x692 => (x692._1, x692._2.map{case (x683, x684) => val x686 = x684.o_orderdate
val x687 = x683.lbl
val x688 = x687.P__F
val x689 = x687.L__F
val x690 = Record765(x684, x688, x689)
val x691 = Record766(x686, x690)
x691})).toList }
val x698 = x693.map{ case (x694, x695) => { 
  val x696 = x694.lbl
  val x697 = RecM_flat2(x696, x695)
  x697 }}
val M_flat2 = x698
val x699 = M_flat2
val x701 = M_flat2
val x705 = x701.flatMap{ case x702 => 
  val x703 = x702._2
  x703.map(x704 => {
  val nv767 = (x702, x704) 
     nv767
})}
val x710 = x705.map{ case (x706, x707) => { 
  val x708 = x707.o_parts
  val x709 = RecM_ctx3(x708)
  x709 }}
val x711 = x710.distinct
val M_ctx3 = x711
val x712 = M_ctx3
val x714 = M_ctx3
val x715 = L__D._1
val x717 = x715
val x726 = { val hm769 = x714.groupBy{case x718 => { val x720 = x718.lbl
val x721 = x720.L__F
x721 } }
 val join1 = x717.flatMap{x719 => hm769.get(x719._1) match {
 case Some(a) => x719._2
 case _ => Nil
 }}
 val join2 = x714.groupBy{case x718 => { val x723 = x718.lbl
val x724 = x723.o__F
val x725 = x724.o_orderkey
x725 } }
 join1.flatMap(x719 => join2.get({ val x722 = x719.l_orderkey
x722 }) match {
   case Some(a) => a.map(a1 => (a1, x719))
   case _ => Nil
 })
}
val x727 = P__D._1
val x729 = x727
val x737 = { val hm771 = x726.groupBy{case (x730, x731) => { val x733 = x730.lbl
val x734 = x733.P__F
x734 } }
 val join1 = x729.flatMap{x732 => hm771.get(x732._1) match {
 case Some(a) => x732._2
 case _ => Nil
 }}
 val join2 = x726.groupBy{case (x730, x731) => { val x736 = x731.l_partkey
x736 } }
 join1.flatMap(x732 => join2.get({ val x735 = x732.p_partkey
x735 }) match {
   case Some(a) => a.map(a1 => (a1, x732))
   case _ => Nil
 })
}
val x746 = { val grps772 = x737.groupBy{ case ((x738, x739), x740) => { val x741 = (x738)
x741 }}
 grps772.map(x745 => (x745._1, x745._2.map{case ((x738, x739), x740) => val x742 = x740.p_name
val x743 = x739.l_quantity
val x744 = Record774(x742, x743)
x744})).toList }
val x751 = x746.map{ case (x747, x748) => { 
  val x749 = x747.lbl
  val x750 = RecM_flat3(x749, x748)
  x750 }}
val M_flat3 = x751
val x752 = M_flat3
val x753 = (x627,x655,x668,x699,x712,x752)
x753 }
    var end0 = System.currentTimeMillis() - start0
    
val Q1__F = Q1._1.head.lbl
val Q1__D = (Q1._2, Input_Q1_Dict1((Q1._4, Input_Q1_Dict2((Q1._6, Unit)))))
    def f(){
      val x783 = Q1Flat2(Q1__F)
val x784 = Record857(x783)
val x785 = List(x784)
val M_ctx1 = x785
val x786 = M_ctx1
val x788 = M_ctx1
val x789 = Q1__D._1
val x791 = x789
val x796 = { val hm861 = x788.groupBy{case x792 => { val x794 = x792.lbl
val x795 = x794.Q1__F
x795 } }
 x791.flatMap{x793 => hm861.get(x793._1) match {
 case Some(a) => a.map(a1 => (a1, x793._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x797 = Q1__D._2
val x798 = x797.c_orders
val x799 = x798._1
val x801 = x799
val x806 = { val hm863 = x796.groupBy{case (x802, x803) => { val x805 = x803.c_orders
x805 } }
 x801.flatMap{x804 => hm863.get(x804._1) match {
 case Some(a) => a.map(a1 => (a1, x804._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x807 = x798._2
val x808 = x807.o_parts
val x809 = x808._1
val x811 = x809
val x817 = { val hm864 = x806.groupBy{case ((x812, x813), x814) => { val x816 = x814.o_parts
x816 } }
 x811.flatMap{x815 => hm864.get(x815._1) match {
 case Some(a) => a.map(a1 => (a1, x815._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x819 = x809
val x828 = { val hm865 = x817.groupBy{case (((x820, x821), x822), x823) => { val x825 = x822.o_parts
x825 } }
 val join1 = x819.flatMap{x824 => hm865.get(x824._1) match {
 case Some(a) => x824._2
 case _ => Nil
 }}
 val join2 = x817.groupBy{case (((x820, x821), x822), x823) => { val x827 = x823.p_name
x827 } }
 join1.flatMap(x824 => join2.get({ val x826 = x824.p_name
x826 }) match {
   case Some(a) => a.map(a1 => (a1, x824))
   case _ => Nil
 })
}
val x837 = { val grps866 = x828.groupBy{ case ((((x829, x830), x831), x832), x833) => { val x834 = (x829,x830,x831,x832)
x834 }}
 grps866.map(x836 => (x836._1, x836._2.foldLeft(0.0){ case (acc867, ((((x829, x830), x831), x832), x833)) => acc867 + {val x835 = x833.l_qty
x835} })).toList }
val x849 = { val grps868 = x837.groupBy{ case ((x838, x839, x840, x841), x842) => { val x843 = (x838)
x843 }}
 grps868.map(x848 => (x848._1, x848._2.map{case ((x838, x839, x840, x841), x842) => val x844 = x839.c_name
val x845 = x841.p_name
val x846 = x840.o_orderdate
val x847 = Record870(x844, x845, x846, x842)
x847})).toList }
val x854 = x849.map{ case (x850, x851) => { 
  val x852 = x850.lbl
  val x853 = Record871(x852, x851)
  x853 }}
val M_flat1 = x854
val x855 = M_flat1
val x856 = (x786,x855)
x856
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
