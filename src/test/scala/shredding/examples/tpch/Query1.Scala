
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
    case class Record256(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record259(o_orderdate: String, o_parts: List[Record256], uniqueId: Long) extends CaseClassRecord
case class Query1Out(c_name: String, c_orders: List[Record259], uniqueId: Long) extends CaseClassRecord
object Query1 {
 def main(args: Array[String]){
    var start0 = System.currentTimeMillis()
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    var end0 = System.currentTimeMillis() - start0
    def f(){
      val x201 = C 
val x203 = O 
val x208 = { val hm251 = x201.groupBy{ case x204 => {val x206 = x204.c_custkey 
x206 } }
x203.flatMap(x205 => hm251.get({val x207 = x205.o_custkey 
x207 }) match {
 case Some(a) => a.map(v => (v, x205))
 case _ => Nil
}) } 
val x210 = L 
val x216 = { val hm252 = x208.groupBy{ case (x211, x212) => {val x214 = x212.o_orderkey 
x214 } }
x210.flatMap(x213 => hm252.get({val x215 = x213.l_orderkey 
x215 }) match {
 case Some(a) => a.map(v => (v, x213))
 case _ => Nil
}) } 
val x218 = P 
val x225 = { val hm253 = x216.groupBy{ case ((x219, x220), x221) => {val x223 = x221.l_partkey 
x223 } }
x218.flatMap(x222 => hm253.get({val x224 = x222.p_partkey 
x224 }) match {
 case Some(a) => a.map(v => (v, x222))
 case _ => Nil
}) } 
val x236 = { val grps254 = x225.groupBy{ case (((x226, x227), x228), x229) => { val x230 = (x226,x227) 
x230  } }
 grps254.toList.map(x234 => (x234._1, x234._2.flatMap{ 
   case (((x226, x227), x228), null) =>  Nil
   case (((x226, x227), x228), x229) => {val x235 = (x228,x229) 
x235 } match {
   case (null,_) => Nil
   case (x228,x229) => List({val x231 = x229.p_name 
val x232 = x228.l_quantity 
val x233 = Record256(x231, x232, newId) 
x233   })
 }
} ) ) } 
val x245 = { val grps257 = x236.groupBy{ case ((x237, x238), x239) => { val x240 = (x237) 
x240  } }
 grps257.toList.map(x243 => (x243._1, x243._2.flatMap{ 
   case ((x237, x238), null) =>  Nil
   case ((x237, x238), x239) => {val x244 = (x238,x239) 
x244 } match {
   case (null,_) => Nil
   case (x238,x239) => List({val x241 = x238.o_orderdate 
val x242 = Record259(x241, x239, newId) 
x242  })
 }
} ) ) } 
val x250 = x245.map{ case (x246, x247) => { 
  val x248 = x246.c_name 
  val x249 = Query1Out(x248, x247, newId) 
  x249 }} 
x250          
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
