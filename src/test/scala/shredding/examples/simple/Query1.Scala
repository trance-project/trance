
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record24(a: Int, b1: String, b2: String)
case class InputR(a: Int, b: String)
object Query1 {
 def main(args: Array[String]){ 
    var start0 = System.currentTimeMillis()
    
import shredding.examples.simple._
val R = List(InputR(42, "Milos"), InputR(49, "Michael"), 
                  InputR(34, "Jaclyn"), InputR(42, "Thomas"))
    var end0 = System.currentTimeMillis() - start0
    def f(){
      val x7 = R
val x9 = R
val x15 = { val hm23 = x7.groupBy{ case x10 => {val x13 = x10.a
x13} }
x9.flatMap(x11 => hm23.get({val x14 = x11.a
x14}) match {
 case Some(a) => a.map(v => (v, x11))
 case _ => Nil
}) }
val x22 = x15.map{ case (x16, x17) => { 
  val x18 = x16.a
  val x19 = x16.b
  val x20 = x17.b
  val x21 = Record24(x18, x19, x20)
  x21 }}
x22 
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
