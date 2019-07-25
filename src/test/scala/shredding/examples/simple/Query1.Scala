
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
    case class Record891(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record894(o_orderdate: String, o_parts: List[Record891], uniqueId: Long) extends CaseClassRecord
case class Query1Out(c_name: String, c_orders: List[Record894], uniqueId: Long) extends CaseClassRecord
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
      val x836 = C 
val x838 = O 
val x843 = { val hm886 = x836.groupBy{ case x839 => {val x841 = x839.c_custkey 
x841 } }
x838.flatMap(x840 => hm886.get({val x842 = x840.o_custkey 
x842 }) match {
 case Some(a) => a.map(v => (v, x840))
 case _ => Nil
}) } 
val x845 = L 
val x851 = { val hm887 = x843.groupBy{ case (x846, x847) => {val x849 = x847.o_orderkey 
x849 } }
x845.flatMap(x848 => hm887.get({val x850 = x848.l_orderkey 
x850 }) match {
 case Some(a) => a.map(v => (v, x848))
 case _ => Nil
}) } 
val x853 = P 
val x860 = { val hm888 = x851.groupBy{ case ((x854, x855), x856) => {val x858 = x856.l_partkey 
x858 } }
x853.flatMap(x857 => hm888.get({val x859 = x857.p_partkey 
x859 }) match {
 case Some(a) => a.map(v => (v, x857))
 case _ => Nil
}) } 
val x871 = { val grps889 = x860.groupBy{ case (((x861, x862), x863), x864) => { val x865 = (x861,x862) 
x865  } }
 grps889.toList.map(x869 => (x869._1, x869._2.flatMap{ 
   case (((x861, x862), x863), null) =>  Nil
   case (((x861, x862), x863), x864) => {val x870 = (x863,x864) 
x870 } match {
   case (null,_) => Nil
   case (x863,x864) => List({val x866 = x864.p_name 
val x867 = x863.l_quantity 
val x868 = Record891(x866, x867, newId) 
x868   })
 }
} ) ) } 
val x880 = { val grps892 = x871.groupBy{ case ((x872, x873), x874) => { val x875 = (x872) 
x875  } }
 grps892.toList.map(x878 => (x878._1, x878._2.flatMap{ 
   case ((x872, x873), null) =>  Nil
   case ((x872, x873), x874) => {val x879 = (x873,x874) 
x879 } match {
   case (null,_) => Nil
   case (x873,x874) => List({val x876 = x873.o_orderdate 
val x877 = Record894(x876, x874, newId) 
x877  })
 }
} ) ) } 
val x885 = x880.map{ case (x881, x882) => { 
  val x883 = x881.c_name 
  val x884 = Query1Out(x883, x882, newId) 
  x884 }} 
x885          
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
