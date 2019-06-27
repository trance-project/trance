
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record5(a: Int, b1: String, b2: String)
case class InputR(a: Int, b: String)
object Query1Calc {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
import shredding.examples.simple._
val R = List(InputR(42, "Milos"), InputR(49, "Michael"), 
                  InputR(34, "Jaclyn"), InputR(42, "Thomas"))
    def f(){
      R.flatMap(x1 => { 
    R.flatMap(x2 => { 
      if({x1.a == x2.a}) {  List(Record5(x1.a, x1.b, x2.b))} else {  Nil}})}) 
    }
    var end0 = System.currentTimeMillis() - start0
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
