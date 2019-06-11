
package experiments
/** Generated code **/
object Query1 {
 
import shredding.queries.simple._
val R = List(InputR1(42, List(InputR2("Milos", 123, List(InputR3(123), InputR3(456), InputR3(789), InputR3(123))),
                           InputR2("Michael", 7, List(InputR3(2), InputR3(9), InputR3(1))),
                           InputR2("Jaclyn", 12, List(InputR3(14), InputR3(12))))), 
          InputR1(69, List(InputR2("Thomas", 987, List(InputR3(987), InputR3(654), InputR3(987), InputR3(987))))))
 case class Record180(o7: String, o8: Int)
case class Record181(o5: Int, o6: List[Record180])
 def main(args: Array[String]){ 
    var time = List[Long]()
    for (i <- 1 to 5) {
      var start = System.currentTimeMillis()
      println(f)
      var end = System.currentTimeMillis() - start
      time = time :+ end
    }
    val avg = (time.sum/5)
    println(avg)
 }
 
 def f(){
   val x179 = R.flatMap({x166 => 
   val x167 = x166.h
  val x168 = x166.j
  val x176 = x168.flatMap({x169 => 
     val x170 = x169.m
    val x171 = x169.k
    val x173 = x171.map({x172 => 1}).sum
    val x174 = Record180(x170, x173)
    val x175 = List(x174)
    x175})
  val x177 = Record181(x167, x176)
  val x178 = List(x177)
  x178})
x179
 }
}
