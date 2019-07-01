
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record541(P__F: Int, L__F: Int, O__F: Int, c__F: Customer)
case class Record542(c_name: String, c_orders: Record541)
case class Record548(o__F: Orders, P__F: Int, L__F: Int)
case class Record549(o_orderdate: String, o_parts: Record548)
case class Record557(p_name: String, l_qty: Double)
case class Record652(lbl: Q1Flat2)
case class Input_Q1__DFlat653(c_name: String, c_orders: Int)
case class Flat654(o_orderdate: String, o_parts: Int)
case class Dict654(o_parts: (List[(Int, List[Record557])], Unit))
case class Input_Q1__DDict653(c_orders: (List[(Int, List[Flat654])], Dict654))
case class Record662(l_qty: Double)
case class Record667(c_name: String, p_name: String, month: String, t_qty: Int)
case class Record668(_1: Q1Flat2, _2: List[Record667])
case class RecM_flat2(_1: Record541, _2: List[Record549])
case class RecM_flat3(_1: Record548, _2: List[Record557])
case class RecM_flat1(_1: Q1Flat, _2: List[Record542])
case class RecM_ctx3(lbl: Record548)
case class RecM_ctx2(lbl: Record541)
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
    val Q1 = { val x407 = Q1Flat(P__F, C__F, L__F, O__F)
val x408 = RecM_ctx1(x407)
val x409 = List(x408)
val M_ctx1 = x409
val x410 = M_ctx1
val x412 = M_ctx1
val x413 = C__D._1
val x415 = x413
val x420 = { val hm538 = x412.groupBy{case x416 => { val x418 = x416.lbl
val x419 = x418.C__F
x419 } }
 x415.flatMap{x417 => hm538.get(x417._1) match {
 case Some(a) => a.map(a1 => (a1, x417._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x432 = { val grps539 = x420.groupBy{ case (x421, x422) => { val x423 = (x421)
x423 }}
 grps539.map(x431 => (x431._1, x431._2.map{case (x421, x422) => val x424 = x422.c_name
val x425 = x421.lbl
val x426 = x425.P__F
val x427 = x425.L__F
val x428 = x425.O__F
val x429 = Record541(x426, x427, x428, x422)
val x430 = Record542(x424, x429)
x430})).toList }
val x437 = x432.map{ case (x433, x434) => { 
  val x435 = x433.lbl
  val x436 = RecM_flat1(x435, x434)
  x436 }}
val M_flat1 = x437
val x438 = M_flat1
val x440 = M_flat1
val x444 = x440.flatMap{ case x441 => 
  val x442 = x441._2
  x442.map(x443 => {
  val nv543 = (x441, x443) 
     nv543
})}
val x449 = x444.map{ case (x445, x446) => { 
  val x447 = x446.c_orders
  val x448 = RecM_ctx2(x447)
  x448 }}
val x450 = x449.distinct
val M_ctx2 = x450
val x451 = M_ctx2
val x453 = M_ctx2
val x454 = O__D._1
val x456 = x454
val x465 = { val hm545 = x453.groupBy{case x457 => { val x459 = x457.lbl
val x460 = x459.O__F
x460 } }
 val join1 = x456.flatMap{x458 => hm545.get(x458._1) match {
 case Some(a) => x458._2
 case _ => Nil
 }}
 val join2 = x453.groupBy{case x457 => { val x462 = x457.lbl
val x463 = x462.c__F
val x464 = x463.c_custkey
x464 } }
 join1.flatMap(x458 => join2.get({ val x461 = x458.o_custkey
x461 }) match {
   case Some(a) => a.map(a1 => (a1, x458))
   case _ => Nil
 })
}
val x476 = { val grps546 = x465.groupBy{ case (x466, x467) => { val x468 = (x466)
x468 }}
 grps546.map(x475 => (x475._1, x475._2.map{case (x466, x467) => val x469 = x467.o_orderdate
val x470 = x466.lbl
val x471 = x470.P__F
val x472 = x470.L__F
val x473 = Record548(x467, x471, x472)
val x474 = Record549(x469, x473)
x474})).toList }
val x481 = x476.map{ case (x477, x478) => { 
  val x479 = x477.lbl
  val x480 = RecM_flat2(x479, x478)
  x480 }}
val M_flat2 = x481
val x482 = M_flat2
val x484 = M_flat2
val x488 = x484.flatMap{ case x485 => 
  val x486 = x485._2
  x486.map(x487 => {
  val nv550 = (x485, x487) 
     nv550
})}
val x493 = x488.map{ case (x489, x490) => { 
  val x491 = x490.o_parts
  val x492 = RecM_ctx3(x491)
  x492 }}
val x494 = x493.distinct
val M_ctx3 = x494
val x495 = M_ctx3
val x497 = M_ctx3
val x498 = L__D._1
val x500 = x498
val x509 = { val hm552 = x497.groupBy{case x501 => { val x503 = x501.lbl
val x504 = x503.L__F
x504 } }
 val join1 = x500.flatMap{x502 => hm552.get(x502._1) match {
 case Some(a) => x502._2
 case _ => Nil
 }}
 val join2 = x497.groupBy{case x501 => { val x506 = x501.lbl
val x507 = x506.o__F
val x508 = x507.o_orderkey
x508 } }
 join1.flatMap(x502 => join2.get({ val x505 = x502.l_orderkey
x505 }) match {
   case Some(a) => a.map(a1 => (a1, x502))
   case _ => Nil
 })
}
val x510 = P__D._1
val x512 = x510
val x520 = { val hm554 = x509.groupBy{case (x513, x514) => { val x516 = x513.lbl
val x517 = x516.P__F
x517 } }
 val join1 = x512.flatMap{x515 => hm554.get(x515._1) match {
 case Some(a) => x515._2
 case _ => Nil
 }}
 val join2 = x509.groupBy{case (x513, x514) => { val x519 = x514.l_partkey
x519 } }
 join1.flatMap(x515 => join2.get({ val x518 = x515.p_partkey
x518 }) match {
   case Some(a) => a.map(a1 => (a1, x515))
   case _ => Nil
 })
}
val x529 = { val grps555 = x520.groupBy{ case ((x521, x522), x523) => { val x524 = (x521)
x524 }}
 grps555.map(x528 => (x528._1, x528._2.map{case ((x521, x522), x523) => val x525 = x523.p_name
val x526 = x522.l_quantity
val x527 = Record557(x525, x526)
x527})).toList }
val x534 = x529.map{ case (x530, x531) => { 
  val x532 = x530.lbl
  val x533 = RecM_flat3(x532, x531)
  x533 }}
val M_flat3 = x534
val x535 = M_flat3
val x536 = (x410,x438,x451,x482,x495,x535)
x536 }
    var end0 = System.currentTimeMillis() - start0
    
val Q1__F = Q1._1.head.lbl
val Q1__D = (Q1._2, Input_Q1_Dict1((Q1._4, Input_Q1_Dict2((Q1._6, Unit)))))
    def f(){
      val x567 = Q1Flat2(Q1__F)
val x568 = Record652(x567)
val x569 = List(x568)
val M_ctx1 = x569
val x570 = M_ctx1
val x572 = M_ctx1
val x573 = Q1__D._1
val x575 = x573
val x580 = { val hm656 = x572.groupBy{case x576 => { val x578 = x576.lbl
val x579 = x578.Q1__F
x579 } }
 x575.flatMap{x577 => hm656.get(x577._1) match {
 case Some(a) => a.map(a1 => (a1, x577._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x581 = Q1__D._2
val x582 = x581.c_orders
val x583 = x582._1
val x585 = x583
val x590 = { val hm658 = x580.groupBy{case (x586, x587) => { val x589 = x587.c_orders
x589 } }
 x585.flatMap{x588 => hm658.get(x588._1) match {
 case Some(a) => a.map(a1 => (a1, x588._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x591 = x582._2
val x592 = x591.o_parts
val x593 = x592._1
val x595 = x593
val x601 = { val hm659 = x590.groupBy{case ((x596, x597), x598) => { val x600 = x598.o_parts
x600 } }
 x595.flatMap{x599 => hm659.get(x599._1) match {
 case Some(a) => a.map(a1 => (a1, x599._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x603 = x593
val x612 = { val hm660 = x601.groupBy{case (((x604, x605), x606), x607) => { val x609 = x606.o_parts
x609 } }
 val join1 = x603.flatMap{x608 => hm660.get(x608._1) match {
 case Some(a) => x608._2
 case _ => Nil
 }}
 val join2 = x601.groupBy{case (((x604, x605), x606), x607) => { val x611 = x607.p_name
x611 } }
 join1.flatMap(x608 => join2.get({ val x610 = x608.p_name
x610 }) match {
   case Some(a) => a.map(a1 => (a1, x608))
   case _ => Nil
 })
}
val x623 = x612.flatMap{ case ((((x613, x614), x615), x616), x617) => 
  val x618 = x616.l_qty
  val x619 = Record662(x618)
  val x620 = x617.l_qty
  val x621 = (1 to x620.asInstanceOf[Int]).map(v => x619)
  x621.map(x622 => {
  val nv661 = (((((x613, x614), x615), x616), x617), x622) 
     nv661
})}
val x632 = { val grps663 = x623.groupBy{ case (((((x624, x625), x626), x627), x628), x629) => { val x630 = (x624,x625,x626,x627)
x630 }}
 grps663.map(x631 => (x631._1, x631._2.foldLeft(0)((acc664, x631) => acc664 + 1))).toList }
val x644 = { val grps665 = x632.groupBy{ case ((x633, x634, x635, x636), x637) => { val x638 = (x633)
x638 }}
 grps665.map(x643 => (x643._1, x643._2.map{case ((x633, x634, x635, x636), x637) => val x639 = x634.c_name
val x640 = x636.p_name
val x641 = x635.o_orderdate
val x642 = Record667(x639, x640, x641, x637)
x642})).toList }
val x649 = x644.map{ case (x645, x646) => { 
  val x647 = x645.lbl
  val x648 = Record668(x647, x646)
  x648 }}
val M_flat1 = x649
val x650 = M_flat1
val x651 = (x570,x650)
x651
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
