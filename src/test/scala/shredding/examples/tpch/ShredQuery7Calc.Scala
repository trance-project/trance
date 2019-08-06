
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
case class Record2705(p__F: Part, S__F: Int, PS__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record2706(p__F: Part, C__F: Int, L__F: Int, O__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record2707(p_name: String, suppliers: Record2705, customers: Record2706, uniqueId: Long) extends CaseClassRecord
case class Record2717(s_name: String, s_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record2730(c_name: String, c_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record2833(lbl: Q3Flat3, uniqueId: Long) extends CaseClassRecord
case class Record2838(Query7__F: Int, n__F: Nation, uniqueId: Long) extends CaseClassRecord
case class Record2839(n_name: String, part_names: Record2838, uniqueId: Long) extends CaseClassRecord
case class Record2840(_1: Q3Flat3, _2: List[Record2839], uniqueId: Long) extends CaseClassRecord
case class Record2843(lbl: Record2838, uniqueId: Long) extends CaseClassRecord
case class Input_Query7__DFlat2845(p_name: String, suppliers: Int, customers: Int, uniqueId: Long) extends CaseClassRecord
case class Input_Query7__DDict2845(suppliers: (List[(Int, List[Record2717])], Unit), customers: (List[(Int, List[Record2730])], Unit), uniqueId: Long) extends CaseClassRecord
case class Record2853(p_name: String, uniqueId: Long) extends CaseClassRecord
case class Record2857(_1: Record2838, _2: List[Record2853], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx1(lbl: Q3Flat, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx3(lbl: Record2706, uniqueId: Long) extends CaseClassRecord
case class RecM_flat1(_1: Q3Flat, _2: List[Record2707], uniqueId: Long) extends CaseClassRecord
case class RecM_flat3(_1: Record2706, _2: List[Record2730], uniqueId: Long) extends CaseClassRecord
case class RecM_flat2(_1: Record2705, _2: List[Record2717], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx2(lbl: Record2705, uniqueId: Long) extends CaseClassRecord
object ShredQuery7Calc {
 def main(args: Array[String]){
    var start0 = System.currentTimeMillis()
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
    
val C__F = 1
val C__D = (List((C__F, TPCHLoader.loadCustomer[Customer].toList)), ())
val O__F = 2
val O__D = (List((O__F, TPCHLoader.loadOrders[Orders].toList)), ())
val L__F = 3
val L__D = (List((L__F, TPCHLoader.loadLineitem[Lineitem].toList)), ())
val P__F = 4
val P__D = (List((P__F, TPCHLoader.loadPart[Part].toList)), ())
val PS__F = 5
val PS__D = (List((PS__F, TPCHLoader.loadPartSupp[PartSupp].toList)), ())
val S__F = 6
val S__D = (List((S__F, TPCHLoader.loadSupplier[Supplier].toList)), ())
    val ShredQuery7 = { val x2560 = Q3Flat(O__F, C__F, PS__F, S__F, L__F, P__F, newId) 
val x2561 = RecM_ctx1(x2560, newId) 
val x2562 = List(x2561) 
val M_ctx1 = x2562
val x2563 = M_ctx1
val x2589 = M_ctx1.flatMap(x2564 => { 
    val x2565 = x2564.lbl 
    val x2566 = P__D._1 
    val x2586 = x2566.flatMap(x2567 => { 
      if({val x2568 = x2564.lbl 
      val x2569 = x2568.P__F 
      val x2570 = x2567._1 
      val x2571 = x2569 == x2570 
      x2571    }) {  val x2572 = x2567._2 
        val x2585 = x2572.flatMap(x2573 => { 
            val x2574 = x2573.p_name 
            val x2575 = x2564.lbl 
            val x2576 = x2575.S__F 
            val x2577 = x2575.PS__F 
            val x2578 = Record2705(x2573, x2576, x2577, newId) 
            val x2579 = x2575.C__F 
            val x2580 = x2575.L__F 
            val x2581 = x2575.O__F 
            val x2582 = Record2706(x2573, x2579, x2580, x2581, newId) 
            val x2583 = Record2707(x2574, x2578, x2582, newId) 
            val x2584 = List(x2583) 
            x2584}) 
        x2585} else {  Nil}}) 
    val x2587 = RecM_flat1(x2565, x2586, newId) 
    val x2588 = List(x2587) 
    x2588}) 
val M_flat1 = x2589
val x2590 = M_flat1
val x2598 = M_flat1.flatMap(x2591 => { 
    val x2592 = x2591._2 
    val x2597 = x2592.flatMap(x2593 => { 
        val x2594 = x2593.suppliers 
        val x2595 = RecM_ctx2(x2594, newId) 
        val x2596 = List(x2595) 
        x2596}) 
    x2597}) 
val x2599 = x2598.distinct 
val M_ctx2 = x2599
val x2600 = M_ctx2
val x2637 = M_ctx2.flatMap(x2601 => { 
    val x2602 = x2601.lbl 
    val x2603 = PS__D._1 
    val x2634 = x2603.flatMap(x2604 => { 
      if({val x2605 = x2601.lbl 
      val x2606 = x2605.PS__F 
      val x2607 = x2604._1 
      val x2608 = x2606 == x2607 
      x2608    }) {  val x2609 = x2604._2 
        val x2633 = x2609.flatMap(x2610 => { 
          if({val x2611 = x2610.ps_partkey 
          val x2612 = x2601.lbl 
          val x2613 = x2612.p__F 
          val x2614 = x2613.p_partkey 
          val x2615 = x2611 == x2614 
          x2615     }) {  val x2616 = S__D._1 
            val x2632 = x2616.flatMap(x2617 => { 
              if({val x2618 = x2601.lbl 
              val x2619 = x2618.S__F 
              val x2620 = x2617._1 
              val x2621 = x2619 == x2620 
              x2621    }) {  val x2622 = x2617._2 
                val x2631 = x2622.flatMap(x2623 => { 
                  if({val x2624 = x2623.s_suppkey 
                  val x2625 = x2610.ps_suppkey 
                  val x2626 = x2624 == x2625 
                  x2626   }) {  val x2627 = x2623.s_name 
                    val x2628 = x2623.s_nationkey 
                    val x2629 = Record2717(x2627, x2628, newId) 
                    val x2630 = List(x2629) 
                    x2630} else {  Nil}}) 
                x2631} else {  Nil}}) 
            x2632} else {  Nil}}) 
        x2633} else {  Nil}}) 
    val x2635 = RecM_flat2(x2602, x2634, newId) 
    val x2636 = List(x2635) 
    x2636}) 
val M_flat2 = x2637
val x2638 = M_flat2
val x2646 = M_flat1.flatMap(x2639 => { 
    val x2640 = x2639._2 
    val x2645 = x2640.flatMap(x2641 => { 
        val x2642 = x2641.customers 
        val x2643 = RecM_ctx3(x2642, newId) 
        val x2644 = List(x2643) 
        x2644}) 
    x2645}) 
val x2647 = x2646.distinct 
val M_ctx3 = x2647
val x2648 = M_ctx3
val x2698 = M_ctx3.flatMap(x2649 => { 
    val x2650 = x2649.lbl 
    val x2651 = L__D._1 
    val x2695 = x2651.flatMap(x2652 => { 
      if({val x2653 = x2649.lbl 
      val x2654 = x2653.L__F 
      val x2655 = x2652._1 
      val x2656 = x2654 == x2655 
      x2656    }) {  val x2657 = x2652._2 
        val x2694 = x2657.flatMap(x2658 => { 
          if({val x2659 = x2658.l_partkey 
          val x2660 = x2649.lbl 
          val x2661 = x2660.p__F 
          val x2662 = x2661.p_partkey 
          val x2663 = x2659 == x2662 
          x2663     }) {  val x2664 = O__D._1 
            val x2693 = x2664.flatMap(x2665 => { 
              if({val x2666 = x2649.lbl 
              val x2667 = x2666.O__F 
              val x2668 = x2665._1 
              val x2669 = x2667 == x2668 
              x2669    }) {  val x2670 = x2665._2 
                val x2692 = x2670.flatMap(x2671 => { 
                  if({val x2672 = x2671.o_orderkey 
                  val x2673 = x2658.l_orderkey 
                  val x2674 = x2672 == x2673 
                  x2674   }) {  val x2675 = C__D._1 
                    val x2691 = x2675.flatMap(x2676 => { 
                      if({val x2677 = x2649.lbl 
                      val x2678 = x2677.C__F 
                      val x2679 = x2676._1 
                      val x2680 = x2678 == x2679 
                      x2680    }) {  val x2681 = x2676._2 
                        val x2690 = x2681.flatMap(x2682 => { 
                          if({val x2683 = x2682.c_custkey 
                          val x2684 = x2671.o_custkey 
                          val x2685 = x2683 == x2684 
                          x2685   }) {  val x2686 = x2682.c_name 
                            val x2687 = x2682.c_nationkey 
                            val x2688 = Record2730(x2686, x2687, newId) 
                            val x2689 = List(x2688) 
                            x2689} else {  Nil}}) 
                        x2690} else {  Nil}}) 
                    x2691} else {  Nil}}) 
                x2692} else {  Nil}}) 
            x2693} else {  Nil}}) 
        x2694} else {  Nil}}) 
    val x2696 = RecM_flat3(x2650, x2695, newId) 
    val x2697 = List(x2696) 
    x2697}) 
val M_flat3 = x2698
val x2699 = M_flat3
val x2700 = (x2563,x2590,x2600,x2638,x2648,x2699) 
x2700            }
    var end0 = System.currentTimeMillis() - start0
    
case class Input_Q3_Dict1(suppliers: (List[RecM_flat2], Unit), customers: (List[RecM_flat3], Unit))
val N__F = 7
val N__D = (List((N__F, TPCHLoader.loadNation[Nation].toList)), ())
val Query7__F = ShredQuery7._1.head.lbl
val Query7__D = (ShredQuery7._2, Input_Q3_Dict1((ShredQuery7._4, Unit), (ShredQuery7._6, Unit)))
    def f(){
      val x2743 = Q3Flat3(N__F, Query7__F, newId) 
val x2744 = Record2833(x2743, newId) 
val x2745 = List(x2744) 
val M_ctx1 = x2745
val x2746 = M_ctx1
val x2767 = M_ctx1.flatMap(x2747 => { 
    val x2748 = x2747.lbl 
    val x2749 = N__D._1 
    val x2764 = x2749.flatMap(x2750 => { 
      if({val x2751 = x2747.lbl 
      val x2752 = x2751.N__F 
      val x2753 = x2750._1 
      val x2754 = x2752 == x2753 
      x2754    }) {  val x2755 = x2750._2 
        val x2763 = x2755.flatMap(x2756 => { 
            val x2757 = x2756.n_name 
            val x2758 = x2747.lbl 
            val x2759 = x2758.Query7__F 
            val x2760 = Record2838(x2759, x2756, newId) 
            val x2761 = Record2839(x2757, x2760, newId) 
            val x2762 = List(x2761) 
            x2762}) 
        x2763} else {  Nil}}) 
    val x2765 = Record2840(x2748, x2764, newId) 
    val x2766 = List(x2765) 
    x2766}) 
val M_flat1 = x2767
val x2768 = M_flat1
val x2776 = M_flat1.flatMap(x2769 => { 
    val x2770 = x2769._2 
    val x2775 = x2770.flatMap(x2771 => { 
        val x2772 = x2771.part_names 
        val x2773 = Record2843(x2772, newId) 
        val x2774 = List(x2773) 
        x2774}) 
    x2775}) 
val x2777 = x2776.distinct 
val M_ctx2 = x2777
val x2778 = M_ctx2
val x2830 = M_ctx2.flatMap(x2779 => { 
    val x2780 = x2779.lbl 
    val x2781 = Query7__D._1 
    val x2827 = x2781.flatMap(x2782 => { 
      if({val x2783 = x2779.lbl 
      val x2784 = x2783.Query7__F 
      val x2785 = x2782._1 
      val x2786 = x2784 == x2785 
      x2786    }) {  val x2787 = x2782._2 
        val x2826 = x2787.flatMap(x2788 => { 
            val x2789 = Query7__D._2 
            val x2790 = x2789.suppliers 
            val x2791 = x2790._1 
            val x2825 = x2791.flatMap(x2792 => { 
              if({val x2793 = x2788.suppliers 
              val x2794 = x2792._1 
              val x2795 = x2793 == x2794 
              x2795   }) {  val x2796 = x2792._2 
                val x2824 = x2796.flatMap(x2797 => { 
                  if({val x2798 = x2797.s_nationkey 
                  val x2799 = x2779.lbl 
                  val x2800 = x2799.n__F 
                  val x2801 = x2800.n_nationkey 
                  val x2802 = x2798 == x2801 
                  val x2803 = Query7__D._2 
                  val x2804 = x2803.customers 
                  val x2805 = x2804._1 
                  val x2818 = x2805.foldLeft(0)((acc2855, x2806) => 
                    if({val x2807 = x2788.customers 
                    val x2808 = x2806._1 
                    val x2809 = x2807 == x2808 
                    x2809   }) {  acc2855 + {val x2810 = x2806._2 
                      val x2817 = x2810.foldLeft(0)((acc2856, x2811) => 
                        if({val x2812 = x2811.c_nationkey 
                        val x2813 = x2779.lbl 
                        val x2814 = x2813.n__F 
                        val x2815 = x2814.n_nationkey 
                        val x2816 = x2812 == x2815 
                        x2816     }) {  acc2856 + {1}} else {  acc2856}) 
                      x2817  }} else {  acc2855}) 
                  val x2819 = x2818 == 0 
                  val x2820 = x2802 && x2819 
                  x2820           }) {  val x2821 = x2788.p_name 
                    val x2822 = Record2853(x2821, newId) 
                    val x2823 = List(x2822) 
                    x2823} else {  Nil}}) 
                x2824} else {  Nil}}) 
            x2825}) 
        x2826} else {  Nil}}) 
    val x2828 = Record2857(x2780, x2827, newId) 
    val x2829 = List(x2828) 
    x2829}) 
val M_flat2 = x2830
val x2831 = M_flat2
val x2832 = (x2746,x2768,x2778,x2831) 
x2832        
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
