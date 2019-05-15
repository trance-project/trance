package shredding.algebra

object App {

  def main(args: Array[String]){
    //run1()
    run2()
  }

  def run1(){

    val compiler = new BaseCompiler {}
  
    val exp1 = {
      import compiler._

      /**
        * For x1 in R Union If (x1.a > 40) Then Sng((w := x1.b))
        *  { ( w := x1.b ) |  x1 <- R, x1.a > 40  }
        * Reduce[ U / lambda(x1).( w := x1.b ), lambda(x1)."true"]
        *  Select[lambda(x1). x1.a > 40](R)
        */
      
      val data = input(List(tuple(Map("a" -> constant(42), "b" -> constant("Milos"))), 
                            tuple(Map("a" -> constant(49), "b" -> constant("Michael"))), 
                            tuple(Map("a" -> constant(34), "b" -> constant("Jaclyn"))), 
                            tuple(Map("a" -> constant(42), "b" -> constant("Thomas")))))
      //val ffun = (i: Rep) => gt(project(i,"a"), const(40))
      //val pmat = (i: Rep) => tuple(Map("w" -> project(i, "b")))
      //comprehension(data, ffun, pmat)
      // { (x, y) | x <- R, y <- R }
      // { { (x,y) | y <- R } | x <- R }
      /**comprehension(data, x => const(true), (z: Rep) => 
        comprehension(data, x => const(true), (x: Rep) => 
          comprehension(data, x => const(true), (y: Rep) => tuple(Map("x" -> x, "y" -> y)))))**/
      bind(data, (i: Rep) => comprehension(i, x => constant(true), (x: Rep) => tuple(Map("x" -> project(x, "a")))))
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
      val data = input(List(tuple(Map("h" -> constant(42), "j" -> 
                  input(List(tuple(Map("m" -> constant("Milos"), "n" -> constant(123), "k" -> 
                    input(List(tuple(Map("n" -> constant(123))), 
                               tuple(Map("n" -> constant(456))), 
                               tuple(Map("n" -> constant(789))), 
                               tuple(Map("n" -> constant(123))))))),
                      tuple(Map("m" -> constant("Michael"), "n" -> constant(7), "k" -> 
                    input(List(tuple(Map("n" -> constant(2))), 
                               tuple(Map("n" -> constant(9))), 
                               tuple(Map("n" -> constant(1))))))),
                      tuple(Map("m" -> constant("Jaclyn"), "n" -> constant(7), "k" -> 
                    input(List(tuple(Map("n"-> constant(14))), 
                               tuple(Map("n" -> constant(12))))))))))),
                  tuple(Map("h" -> constant(69), "j" ->
                    input(List(tuple(Map("m" -> constant("Thomas"), "n" -> constant(987), "k" -> 
                      input(List(tuple(Map("n" -> constant(987))),
                                 tuple(Map("n" -> constant(654))),
                                 tuple(Map("n" -> constant(987))),
                                 tuple(Map("n" -> constant(654))),
                                 tuple(Map("n" -> constant(987))), 
                                 tuple(Map("n" -> constant(987)))))))))))))
    
      val unnest1filt = (i: Rep) => gt(project(project(i, "value"), "n"), constant(40))
      val unnest2filt = (i: Rep) => gt(project(project(i, "value"), "n"), constant(700))
      
      val s1 = select(data, x => constant(true))
      // x 
      val s2 = unnest(s1, (i: Rep) => project(i,"j"), x => constant(true))
      // (x, w)
      val s3 = unnest(s2, (i: Rep) => project(project(i, "value"), "k"), x => constant(true))
      // ((x, w), v1) => ((x,w), 1)
      val s4 = nest(s3, (i: Rep) => project(i, "key"), (i: Rep) => constant(1), x => constant(true))
      // ((x, w), v3) => (o7 := w.m, o8 := v3)
      // don't think this is working properly
      val s5 = nest(s4, (i: Rep) => project(project(i, "key"), "key"), 
                (i: Rep) => tuple(Map("o7" -> project(project(project(i, "key"), "value"), "m"), 
                  "o8" -> project(i, "value"))), x => constant(true))
      s5
      // (x,v2) 
      //val s6 = reduce(s5, (i: Rep) => tuple(Map("o5" -> project(project(i, "key"), "h"), 
      //        "o6" -> project(i, "value"))), x => const(true))
      //s6
     }

    val inters = new BaseStringify{}
    val interu = new BaseUnnester{}
    val inter = new BaseScalaInterp{}
    val finalizer = new Finalizer(inter)
    val finalizers = new Finalizer(inters)
    val finalizeru = new Finalizer(interu)
    println(finalizers.finalize(exp1))
    println(finalizer.finalize(exp1))
    println("")
    //println(finalizers.finalize(finalizeru.finalize(exp1).asInstanceOf[Expr]))
    //println("")
    //println(finalizer.finalize(exp1))
  }

  /**def run2(){
    val compiler = new BaseNRCTranslator{}

    val exp1 = {
      import compiler._
      import shredding.core._

      val btp = TupleType("t" -> IntType)
      val rtp = TupleType("a" -> BagType(btp), "b" -> IntType)
      val r = VarDef("R", BagType(rtp))
      val relationR = compiler.nrc.BagVarRef(r)
      val xdef = VarDef("x", rtp)
      val ydef = VarDef("y", BagType(rtp))
      //compiler.nrc.Let(ydef, relationR, 
        compiler.nrc.ForeachUnion(xdef, relationR, 
          compiler.nrc.Singleton(compiler.nrc.Tuple("a" -> 
            compiler.nrc.PrimitiveProject(compiler.nrc.TupleVarRef(xdef), "b"))))
          //compiler.nrc.BagProject(compiler.nrc.TupleVarRef(xdef), ("a")))//)
    }

    val inters = new BaseStringify{}
    val fins = new Finalizer(inters)
    val exp2 = compiler.translate(exp1)
    val printer = new shredding.nrc.Printer{}
    
    println(printer.quote(exp1.asInstanceOf[printer.Expr]))
    println(exp2)
    
    println(fins.finalize(exp2))

  }**/

  def run2(){
    val translator = new NRCTranslator{}
    val exp1 = {
      import translator._
      import shredding.core._
      val itemTp = TupleType("a" -> IntType, "b" -> StringType)
      val relationR = BagVarRef(VarDef("R", BagType(itemTp)))
      val xdef = VarDef("x", itemTp)
      val q = ForeachUnion(xdef, relationR,
                IfThenElse(Cond(OpGt, TupleVarRef(xdef)("a"), Const(40, IntType)),
                  Singleton(Tuple("w" -> TupleVarRef(xdef)("b")))))
      val printer = new shredding.nrc.Printer{}
      println(printer.quote(q.asInstanceOf[printer.Expr]))
      translate(q)   
    }

    val inters = new BaseStringify{}
    val fins = new Finalizer(inters)
    println(fins.finalize(exp1)) 
  }

}
