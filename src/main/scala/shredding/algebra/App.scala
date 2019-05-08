package shredding.algebra

object App extends BaseCompiler with Expr {

  def main(args: Array[String]){


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
    
    val data = Input(List((42, "Milos"), (49, "Michael"), (34, "Jaclyn"), (42, "Thomas")))
    val ffun = (i: Expr) => Gt(Project(i,0), Input(40))
    val pmat = (i: Expr) => Project(i, 1)
    //val exp = Plan(Reduce(pmat, identity), Select(data, ffun))
    val exp = Reduce(Project(Select(data, ffun), 1), identity)

    //val inter = new BaseScalaInterp{}
    val interc = new BaseCompiler{}
    val finalizer = new Finalizer(interc)
    println(finalizer.finalize(exp))

  }

}
