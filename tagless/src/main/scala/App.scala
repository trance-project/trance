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
      
      val data = input(List(tuple(Map("a" -> const(42), "b" -> const("Milos"))), 
                            tuple(Map("a" -> const(49), "b" -> const("Michael"))), 
                            tuple(Map("a" -> const(34), "b" -> const("Jaclyn"))), 
                            tuple(Map("a" -> const(42), "b" -> const("Thomas")))))
      val ffun = (i: Rep) => gt(project(i,"a"), const(40))
      val pmat = (i: Rep) => tuple(Map("w" -> project(i, "b")))
      reduce(select(data, ffun), pmat, x => const(true))
    }

    val inter = new BaseScalaInterp{}
    val finalizer = new Finalizer(inter)
    // val interc = new BaseCompiler{}
    // val finalizer = new Finalizer(interc)
    println(finalizer.finalize(exp))

  }

}
