
package experiments
/** Generated code **/
object Query1 {
 
    import shredding.queries.simple._
    val R = List(InputR(42, "Milos"), InputR(49, "Michael"), InputR(34, "Jaclyn"), InputR(42, "Thomas"))
  
 case class Record169(o1: Int)
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
   val x168 = R.flatMap({x164 => 
   val x165 = x164.a
  val x166 = Record169(x165)
  val x167 = List(x166)
  x167})
x168
 }
}
