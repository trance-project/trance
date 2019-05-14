package shredding.calc

object App {

  def main(args: Array[String]){

    val compiler = new BaseCompiler {}
  
    val exp1 = {
      import compiler._

      /**
        * For x1 in R Union If (x1.a > 40) Then Sng((w := x1.b))
        *  { ( w := x1.b ) |  x1 <- R, x1.a > 40  }
        * Reduce[ U / lambda(x1).( w := x1.b ), lambda(x1)."true"]
        *  Select[lambda(x1). x1.a > 40](R)
        */
      
      val data = input(List(tuple(Map("a" -> const(42), "b" -> const("Milos"))), 
                            tuple(Map("a" -> const(49), "b" -> const("Michael"))), 
                            tuple(Map("a" -> const(34), "b" -> const("Jaclyn"))), 
                            tuple(Map("a" -> const(42), "b" -> const("Thomas")))))
      //val ffun = (i: Rep) => gt(project(i,"a"), const(40))
      //val pmat = (i: Rep) => tuple(Map("w" -> project(i, "b")))
      //comprehension(data, ffun, pmat)
      // { (x, y) | x <- R, y <- R }
      // { { (x,y) | y <- R } | x <- R }
      comprehension(data, x => const(true), (z: Rep) => 
        comprehension(data, x => const(true), (x: Rep) => 
          comprehension(data, x => const(true), (y: Rep) => tuple(Map("x" -> x, "y" -> y)))))
    }

    /**
      * For x in R Union 
      *   Sng((o5 := x.h, o6 := For w in x.j Union 
      *                           Sng((o7 := w.m, o8 := Total(w.k)))))
      *
      * { ( o5 := x.h, o6 := { ( o7 := w.m, o8 :=  + { "1" |  v1 <- w.k  } ) |  w <- x.j  } ) |  x <- R  }
      *
      * Reduce[ U / lambda(x,v2).( o5 := x.h, o6 := v2 ), lambda(x,v2)."true"]
      * Nest[ U / lambda(x,w,v3).( o7 := w.m, o8 := v3 ) / lambda(x,w,v3).x, 
      *       lambda(x,w,v3)."true" / lambda(x,w,v3).w,v3]
      *   Nest[ + / lambda(x,w,v1)."1" / lambda(x,w,v1).x,w, lambda(x,w,v1)."true" / lambda(x,w,v1).v1]
      *     OuterUnnest[lambda(x,w).w.k, lambda(x,w)."true"]
      *        OuterUnnest[lambda(x).x.j, lambda(x)."true"]
      *          Select[lambda(x)."true"](R)
      */
    val exp2 = {
      import compiler._
      val data = input(List(tuple(Map("h" -> const(42), "j" -> 
                  input(List(tuple(Map("m" -> const("Milos"), "n" -> const(123), "k" -> 
                    input(List(tuple(Map("n" -> const(123))), 
                               tuple(Map("n" -> const(456))), 
                               tuple(Map("n" -> const(789))), 
                               tuple(Map("n" -> const(123))))))),
                      tuple(Map("m" -> const("Michael"), "n" -> const(7), "k" -> 
                    input(List(tuple(Map("n" -> const(2))), 
                               tuple(Map("n" -> const(9))), 
                               tuple(Map("n" -> const(1))))))),
                      tuple(Map("m" -> const("Jaclyn"), "n" -> const(7), "k" -> 
                    input(List(tuple(Map("n"-> const(14))), 
                               tuple(Map("n" -> const(12))))))))))),
                  tuple(Map("h" -> const(69), "j" ->
                    input(List(tuple(Map("m" -> const("Thomas"), "n" -> const(987), "k" -> 
                      input(List(tuple(Map("n" -> const(987))),
                                 tuple(Map("n" -> const(654))),
                                 tuple(Map("n" -> const(987))),
                                 tuple(Map("n" -> const(654))),
                                 tuple(Map("n" -> const(987))), 
                                 tuple(Map("n" -> const(987)))))))))))))
    
      val unnest1filt = (i: Rep) => gt(project(project(i, "value"), "n"), const(40))
      val unnest2filt = (i: Rep) => gt(project(project(i, "value"), "n"), const(700))
      
      /**val s1 = select(data, x => const(true))
      // x 
      val s2 = unnest(s1, (i: Rep) => project(i,"j"), x => const(true))
      // (x, w)
      val s3 = unnest(s2, (i: Rep) => project(project(i, "value"), "k"), x => const(true))
      // ((x, w), v1) => ((x,w), 1)
      val s4 = nest(s3, (i: Rep) => project(i, "key"), (i: Rep) => const(1), x => const(true))
      // ((x, w), v3) => (o7 := w.m, o8 := v3)
      // don't think this is working properly
      val s5 = nest(s4, (i: Rep) => project(project(i, "key"), "key"), 
                (i: Rep) => tuple(Map("o7" -> project(project(project(i, "key"), "value"), "m"), 
                  "o8" -> project(i, "value"))), x => const(true))
      s5*/
      // (x,v2) 
      //val s6 = reduce(s5, (i: Rep) => tuple(Map("o5" -> project(project(i, "key"), "h"), 
      //        "o6" -> project(i, "value"))), x => const(true))
      //s6
     }

    val inters = new BaseStringify{}
    val interu = new BaseUnnester{}
    //val inter = new BaseScalaInterp{}
    //val finalizer = new Finalizer(inter)
    val finalizers = new Finalizer(inters)
    val finalizeru = new Finalizer(interu)
    println(finalizers.finalize(exp1))
    println("")
    println(finalizers.finalize(finalizeru.finalize(exp1).asInstanceOf[Expr]))
    println("")
    //println(finalizer.finalize(exp1))

  }

}
