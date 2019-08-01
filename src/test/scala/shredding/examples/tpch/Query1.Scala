
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
    case class Record63(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record66(o_orderdate: String, o_parts: List[Record63], uniqueId: Long) extends CaseClassRecord
case class Query1Out(c_name: String, c_orders: List[Record66], uniqueId: Long) extends CaseClassRecord
object Query1 {
 def main(args: Array[String]){
    var start0 = System.currentTimeMillis()
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
    
import sprkloader.TPCHLoader
val tpch = TPCHLoader(spark)
val C = tpch.loadCustomers
val O = tpch.loadOrders
val L = tpch.loadLineitem
val P = tpch.loadPart
    var end0 = System.currentTimeMillis() - start0
    def f(){
      val x8 = C 
val x10 = O 
val x15 = { val hm58 = x8.groupBy{ case x11 => {val x13 = x11.c_custkey 
x13 } }
x10.flatMap(x12 => hm58.get({val x14 = x12.o_custkey 
x14 }) match {
 case Some(a) => a.map(v => (v, x12))
 case _ => Nil
}) } 
val x17 = L 
val x23 = { val hm59 = x15.groupBy{ case (x18, x19) => {val x21 = x19.o_orderkey 
x21 } }
x17.flatMap(x20 => hm59.get({val x22 = x20.l_orderkey 
x22 }) match {
 case Some(a) => a.map(v => (v, x20))
 case _ => Nil
}) } 
val x25 = P 
val x32 = { val hm60 = x23.groupBy{ case ((x26, x27), x28) => {val x30 = x28.l_partkey 
x30 } }
x25.flatMap(x29 => hm60.get({val x31 = x29.p_partkey 
x31 }) match {
 case Some(a) => a.map(v => (v, x29))
 case _ => Nil
}) } 
val x43 = { val grps61 = x32.groupBy{ case (((x33, x34), x35), x36) => { val x37 = (x33,x34) 
x37  } }
 grps61.toList.map(x41 => (x41._1, x41._2.flatMap{ 
   case (((x33, x34), x35), null) =>  Nil
   case (((x33, x34), x35), x36) => {val x42 = (x35,x36) 
x42 } match {
   case (null,_) => Nil
   case (x35,x36) => List({val x38 = x36.p_name 
val x39 = x35.l_quantity 
val x40 = Record63(x38, x39, newId) 
x40   })
 }
} ) ) } 
val x52 = { val grps64 = x43.groupBy{ case ((x44, x45), x46) => { val x47 = (x44) 
x47  } }
 grps64.toList.map(x50 => (x50._1, x50._2.flatMap{ 
   case ((x44, x45), null) =>  Nil
   case ((x44, x45), x46) => {val x51 = (x45,x46) 
x51 } match {
   case (null,_) => Nil
   case (x45,x46) => List({val x48 = x45.o_orderdate 
val x49 = Record66(x48, x46, newId) 
x49  })
 }
} ) ) } 
val x57 = x52.map{ case (x53, x54) => { 
  val x55 = x53.c_name 
  val x56 = Query1Out(x55, x54, newId) 
  x56 }} 
x57          
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
