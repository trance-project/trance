package shredding.algebra

object App {

  def main(args: Array[String]){

    val compiler = new BaseCompiler {}

    val exp = {
      import compiler._

      /**
        * Query: For x1 in R Union If (x1.a > 40) Then Sng((w := x1.b))
        *
        * Comprehension Calculus:
        *
        *  { ( w := x1.b ) |  x1 <- R, x1.a > 40  }
        *
        * Algebra Plans:
        *
        * Reduce[ U / lambda(x1).( w := x1.b ), lambda(x1)."true"]
        *  Select[lambda(x1). x1.a > 40](R)
        */
      
      val data = input(List((42, "Milos"), (49, "Michael"), (34, "Jaclyn"), (42, "Thomas")))
      val ffun = (i: Rep) => gt(project(i,0), input(40))
      val pmat = (i: Rep) => project(i, 1)
      reduce(select(data, ffun), pmat, x => input(true))
    }

    val inter = new BaseScalaInterp{}
    val finalizer = new Finalizer(inter)
    // val interc = new BaseCompiler{}
    // val finalizer = new Finalizer(interc)
    println(finalizer.finalize(exp))

  }

}
