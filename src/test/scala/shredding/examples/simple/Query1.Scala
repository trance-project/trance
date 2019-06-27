
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record23(a: Int, b1: String, b2: String)
case class InputR(a: Int, b: String)
object Query1 {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
import shredding.examples.simple._
val R = List(InputR(42, "Milos"), InputR(49, "Michael"), 
                  InputR(34, "Jaclyn"), InputR(42, "Thomas"))
    def f(){
      val x7 = R
val x9 = R
val x14 = { val hm22 = x7.groupBy{ case x10 => {
  val x12 = x10.a
  x12}}
x9.flatMap(x11 => hm22.get({val x13 = x11.a
x13}) match {
 case Some(a) => a.map(v => (v, x11))
 case _ => Nil
}) }
val x21 = x14.map{ case (x15, x16) => { 
  val x17 = x15.a
  val x18 = x15.b
  val x19 = x16.b
  val x20 = Record23(x17, x18, x19)
  x20 }}
x21 
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
