
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Input_Q1__DFlat486(c_name: String, c_orders: Int)
case class Flat487(o_orderdate: String, o_parts: Int)
case class Flat488(p_name: String, l_qty: Double)
case class Dict487(o_parts: (List[(Int, List[Flat488])], Unit))
case class Input_Q1__DDict486(c_orders: (List[(Int, List[Flat487])], Dict487))
case class Record495(l_qty: Double)
case class Record500(c_name: String, p_name: String, month: String, t_qty: Int)
case class RecM_ctx1(lbl: Q1Flat2)
case class RecM_flat1(_1: Q1Flat2, _2: List[Record500])

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
    val Q1 = { val x401 = Q1Flat2(Q1__F)
val x402 = RecM_ctx1(x401)
val x403 = List(x402)
val M_ctx1 = x403
val x404 = M_ctx1
val x406 = M_ctx1
val x407 = Q1__D._1
val x409 = x407
val x414 = { val hm489 = x406.groupBy{case x410 => { val x412 = x410.lbl
val x413 = x412.Q1__F
x413 } }
 x409.flatMap{x411 => hm489.get(x411._1) match {
 case Some(a) => a.map(a1 => (a1, x411._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x415 = Q1__D._2
val x416 = x415.c_orders
val x417 = x416._1
val x419 = x417
val x424 = { val hm491 = x414.groupBy{case (x420, x421) => { val x423 = x421.c_orders
x423 } }
 x419.flatMap{x422 => hm491.get(x422._1) match {
 case Some(a) => a.map(a1 => (a1, x422._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x425 = x416._2
val x426 = x425.o_parts
val x427 = x426._1
val x429 = x427
val x435 = { val hm492 = x424.groupBy{case ((x430, x431), x432) => { val x434 = x432.o_parts
x434 } }
 x429.flatMap{x433 => hm492.get(x433._1) match {
 case Some(a) => a.map(a1 => (a1, x433._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x437 = x427
val x446 = { val hm493 = x435.groupBy{case (((x438, x439), x440), x441) => { val x443 = x440.o_parts
x443 } }
 val join1 = x437.flatMap{x442 => hm493.get(x442._1) match {
 case Some(a) => x442._2
 case _ => Nil
 }}
 val join2 = x435.groupBy{case (((x438, x439), x440), x441) => { val x445 = x441.p_name
x445 } }
 join1.flatMap(x442 => join2.get({ val x444 = x442.p_name
x444 }) match {
   case Some(a) => a.map(a1 => (a1, x442))
   case _ => Nil
 })
}
val x457 = x446.flatMap{ case ((((x447, x448), x449), x450), x451) => 
  val x452 = x450.l_qty
  val x453 = Record495(x452)
  val x454 = x451.l_qty
  val x455 = (1 to x454.asInstanceOf[Int]).map(v => x453)
  x455.map(x456 => {
  val nv494 = (((((x447, x448), x449), x450), x451), x456) 
     nv494
})}
val x466 = { val grps496 = x457.groupBy{ case (((((x458, x459), x460), x461), x462), x463) => { val x464 = (x458,x459,x460,x461)
x464 }}
 grps496.map(x465 => (x465._1, x465._2.foldLeft(0)((acc497, x465) => acc497 + 1))).toList }
val x478 = { val grps498 = x466.groupBy{ case ((x467, x468, x469, x470), x471) => { val x472 = (x467)
x472 }}
 grps498.map(x477 => (x477._1, x477._2.map{case ((x467, x468, x469, x470), x471) => val x473 = x468.c_name
val x474 = x470.p_name
val x475 = x469.o_orderdate
val x476 = Record500(x473, x474, x475, x471)
x476})).toList }
val x483 = x478.map{ case (x479, x480) => { 
  val x481 = x479.lbl
  val x482 = RecM_flat1(x481, x480)
  x482 }}
val M_flat1 = x483
val x484 = M_flat1
val x485 = (x404,x484)
x485 }
    var end0 = System.currentTimeMillis() - start0
    
val Q1__F = Q1._1.head.lbl
val Q1__D = (Q1._2, Input_Q1_Dict1((Q1._4, Input_Q1_Dict2((Q1._6, Unit)))))
    def f(){
      val x510 = Q1Flat2(Q1__F)
val x511 = RecM_ctx1(x510)
val x512 = List(x511)
val M_ctx1 = x512
val x513 = M_ctx1
val x515 = M_ctx1
val x516 = Q1__D._1
val x518 = x516
val x523 = { val hm596 = x515.groupBy{case x519 => { val x521 = x519.lbl
val x522 = x521.Q1__F
x522 } }
 x518.flatMap{x520 => hm596.get(x520._1) match {
 case Some(a) => a.map(a1 => (a1, x520._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x524 = Q1__D._2
val x525 = x524.c_orders
val x526 = x525._1
val x528 = x526
val x533 = { val hm598 = x523.groupBy{case (x529, x530) => { val x532 = x530.c_orders
x532 } }
 x528.flatMap{x531 => hm598.get(x531._1) match {
 case Some(a) => a.map(a1 => (a1, x531._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x534 = x525._2
val x535 = x534.o_parts
val x536 = x535._1
val x538 = x536
val x544 = { val hm599 = x533.groupBy{case ((x539, x540), x541) => { val x543 = x541.o_parts
x543 } }
 x538.flatMap{x542 => hm599.get(x542._1) match {
 case Some(a) => a.map(a1 => (a1, x542._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
}
val x546 = x536
val x555 = { val hm600 = x544.groupBy{case (((x547, x548), x549), x550) => { val x552 = x549.o_parts
x552 } }
 val join1 = x546.flatMap{x551 => hm600.get(x551._1) match {
 case Some(a) => x551._2
 case _ => Nil
 }}
 val join2 = x544.groupBy{case (((x547, x548), x549), x550) => { val x554 = x550.p_name
x554 } }
 join1.flatMap(x551 => join2.get({ val x553 = x551.p_name
x553 }) match {
   case Some(a) => a.map(a1 => (a1, x551))
   case _ => Nil
 })
}
val x566 = x555.flatMap{ case ((((x556, x557), x558), x559), x560) => 
  val x561 = x559.l_qty
  val x562 = Record495(x561)
  val x563 = x560.l_qty
  val x564 = (1 to x563.asInstanceOf[Int]).map(v => x562)
  x564.map(x565 => {
  val nv601 = (((((x556, x557), x558), x559), x560), x565) 
     nv601
})}
val x575 = { val grps602 = x566.groupBy{ case (((((x567, x568), x569), x570), x571), x572) => { val x573 = (x567,x568,x569,x570)
x573 }}
 grps602.map(x574 => (x574._1, x574._2.foldLeft(0)((acc603, x574) => acc603 + 1))).toList }
val x587 = { val grps604 = x575.groupBy{ case ((x576, x577, x578, x579), x580) => { val x581 = (x576)
x581 }}
 grps604.map(x586 => (x586._1, x586._2.map{case ((x576, x577, x578, x579), x580) => val x582 = x577.c_name
val x583 = x579.p_name
val x584 = x578.o_orderdate
val x585 = Record500(x582, x583, x584, x580)
x585})).toList }
val x592 = x587.map{ case (x588, x589) => { 
  val x590 = x588.lbl
  val x591 = RecM_flat1(x590, x589)
  x591 }}
val M_flat1 = x592
val x593 = M_flat1
val x594 = (x513,x593)
x594
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
