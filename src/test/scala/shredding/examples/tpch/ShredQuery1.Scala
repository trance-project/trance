
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
    case class Record417(P__F: Int, L__F: Int, O__F: Int, c__F: Customer, uniqueId: Long) extends CaseClassRecord
case class Record418(c_name: String, c_orders: Record417, uniqueId: Long) extends CaseClassRecord
case class Record423(o__F: Orders, P__F: Int, L__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record424(o_orderdate: String, o_parts: Record423, uniqueId: Long) extends CaseClassRecord
case class Record431(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class RecM_flat2(_1: Record417, _2: List[Record424], uniqueId: Long) extends CaseClassRecord
case class RecM_flat3(_1: Record423, _2: List[Record431], uniqueId: Long) extends CaseClassRecord
case class RecM_flat1(_1: Q1Flat, _2: List[Record418], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx3(lbl: Record423, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx2(lbl: Record417, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx1(lbl: Q1Flat, uniqueId: Long) extends CaseClassRecord
object ShredQuery1 {
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
    var end0 = System.currentTimeMillis() - start0
    def f(){
      val x280 = Q1Flat(P__F, C__F, L__F, O__F, newId) 
val x281 = RecM_ctx1(x280, newId) 
val x282 = List(x281) 
val M_ctx1 = x282
val x283 = M_ctx1
val x285 = M_ctx1 
val x286 = C__D._1 
val x288 = x286 
val x293 = { val hm414 = x285.groupBy{case x289 => { val x291 = x289.lbl 
val x292 = x291.C__F 
x292   } }
 x288.flatMap{x290 => hm414.get(x290._1) match {
 case Some(a) => a.map(a1 => (a1, x290._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
} 
val x306 = { val grps415 = x293.groupBy{ case (x294, x295) => { val x296 = (x294) 
x296  } }
 grps415.toList.map(x304 => (x304._1, x304._2.flatMap{ 
   case (x294, null) =>  Nil
   case (x294, x295) => {val x305 = (x295) 
x305 } match {
   case (null) => Nil
   case (x295) => List({val x297 = x295.c_name 
val x298 = x294.lbl 
val x299 = x298.P__F 
val x300 = x298.L__F 
val x301 = x298.O__F 
val x302 = Record417(x299, x300, x301, x295, newId) 
val x303 = Record418(x297, x302, newId) 
x303       })
 }
} ) ) } 
val x311 = x306.map{ case (x307, x308) => { 
  val x309 = x307.lbl 
  val x310 = RecM_flat1(x309, x308, newId) 
  x310 }} 
val M_flat1 = x311
val x312 = M_flat1
val x314 = M_flat1 
val x318 = x314.flatMap{ case x315 => 
  val x316 = x315._2 
  x316.flatMap(x317 => {
    List((x315, x317))
})} 
val x323 = x318.map{ case (x319, x320) => { 
  val x321 = x320.c_orders 
  val x322 = RecM_ctx2(x321, newId) 
  x322 }} 
val x324 = x323.distinct 
val M_ctx2 = x324
val x325 = M_ctx2
val x327 = M_ctx2 
val x328 = O__D._1 
val x330 = x328 
val x339 = { val hm420 = x327.groupBy{case x331 => { val x333 = x331.lbl 
val x334 = x333.O__F 
x334   } }
 val join1 = x330.flatMap{x332 => hm420.get(x332._1) match {
 case Some(a) => x332._2
 case _ => Nil
 }}
 val join2 = x327.groupBy{case x331 => { val x336 = x331.lbl 
val x337 = x336.c__F 
val x338 = x337.c_custkey 
x338    } }
 join1.flatMap(x332 => join2.get({ val x335 = x332.o_custkey 
x335  }) match {
   case Some(a) => a.map(a1 => (a1, x332))
   case _ => Nil
 })
} 
val x351 = { val grps421 = x339.groupBy{ case (x340, x341) => { val x342 = (x340) 
x342  } }
 grps421.toList.map(x349 => (x349._1, x349._2.flatMap{ 
   case (x340, null) =>  Nil
   case (x340, x341) => {val x350 = (x341) 
x350 } match {
   case (null) => Nil
   case (x341) => List({val x343 = x341.o_orderdate 
val x344 = x340.lbl 
val x345 = x344.P__F 
val x346 = x344.L__F 
val x347 = Record423(x341, x345, x346, newId) 
val x348 = Record424(x343, x347, newId) 
x348      })
 }
} ) ) } 
val x356 = x351.map{ case (x352, x353) => { 
  val x354 = x352.lbl 
  val x355 = RecM_flat2(x354, x353, newId) 
  x355 }} 
val M_flat2 = x356
val x357 = M_flat2
val x359 = M_flat2 
val x363 = x359.flatMap{ case x360 => 
  val x361 = x360._2 
  x361.flatMap(x362 => {
    List((x360, x362))
})} 
val x368 = x363.map{ case (x364, x365) => { 
  val x366 = x365.o_parts 
  val x367 = RecM_ctx3(x366, newId) 
  x367 }} 
val x369 = x368.distinct 
val M_ctx3 = x369
val x370 = M_ctx3
val x372 = M_ctx3 
val x373 = L__D._1 
val x375 = x373 
val x384 = { val hm426 = x372.groupBy{case x376 => { val x378 = x376.lbl 
val x379 = x378.L__F 
x379   } }
 val join1 = x375.flatMap{x377 => hm426.get(x377._1) match {
 case Some(a) => x377._2
 case _ => Nil
 }}
 val join2 = x372.groupBy{case x376 => { val x381 = x376.lbl 
val x382 = x381.o__F 
val x383 = x382.o_orderkey 
x383    } }
 join1.flatMap(x377 => join2.get({ val x380 = x377.l_orderkey 
x380  }) match {
   case Some(a) => a.map(a1 => (a1, x377))
   case _ => Nil
 })
} 
val x385 = P__D._1 
val x387 = x385 
val x395 = { val hm428 = x384.groupBy{case (x388, x389) => { val x391 = x388.lbl 
val x392 = x391.P__F 
x392   } }
 val join1 = x387.flatMap{x390 => hm428.get(x390._1) match {
 case Some(a) => x390._2
 case _ => Nil
 }}
 val join2 = x384.groupBy{case (x388, x389) => { val x394 = x389.l_partkey 
x394  } }
 join1.flatMap(x390 => join2.get({ val x393 = x390.p_partkey 
x393  }) match {
   case Some(a) => a.map(a1 => (a1, x390))
   case _ => Nil
 })
} 
val x405 = { val grps429 = x395.groupBy{ case ((x396, x397), x398) => { val x399 = (x396) 
x399  } }
 grps429.toList.map(x403 => (x403._1, x403._2.flatMap{ 
   case ((x396, x397), null) =>  Nil
   case ((x396, x397), x398) => {val x404 = (x397,x398) 
x404 } match {
   case (null,_) => Nil
   case (x397,x398) => List({val x400 = x398.p_name 
val x401 = x397.l_quantity 
val x402 = Record431(x400, x401, newId) 
x402   })
 }
} ) ) } 
val x410 = x405.map{ case (x406, x407) => { 
  val x408 = x406.lbl 
  val x409 = RecM_flat3(x408, x407, newId) 
  x409 }} 
val M_flat3 = x410
val x411 = M_flat3
val x412 = (x283,x312,x325,x357,x370,x411) 
x412                                 
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
