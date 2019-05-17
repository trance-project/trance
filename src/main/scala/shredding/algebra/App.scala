package shredding.algebra

import shredding.core._

object App {

  def main(args: Array[String]){
    run1()
    run2()
    run3()
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
  }


  def run2(){
    val translator = new NRCTranslator{}
    val exp1 = {
      import translator._
      val itemTp = TupleType("a" -> IntType, "b" -> StringType)
      val relationR = BagVarRef(VarDef("R", BagType(itemTp)))
      val xdef = VarDef("x", itemTp)
      val r = Union(Singleton(Tuple("a" -> Const(42, IntType), "b" -> Const("Milos", StringType))),
                Union(Singleton(Tuple("a" -> Const(49, IntType), "b" -> Const("Michael", StringType))), 
                  Union(Singleton(Tuple("a" -> Const(34, IntType), "b" -> Const("Jaclyn", StringType))),
                    Singleton(Tuple("a" -> Const(42, IntType), "b" -> Const("Thomas", StringType)))))) 
      val q = ForeachUnion(xdef, r,//relationR,
                IfThenElse(Cond(OpGt, TupleVarRef(xdef)("a"), Const(40, IntType)),
                  Singleton(Tuple("w" -> TupleVarRef(xdef)("b")))))
      val printer = new shredding.nrc.Printer{}
      println(printer.quote(q.asInstanceOf[printer.Expr]))
      translate(q)   
    }

    val inters = new BaseStringify{}
    val fins = new Finalizer(inters) 
    val ctx = Map(Variable("R",BagType(TupleType("a" -> IntType, "b" -> StringType))) -> "R")
    println(fins.finalize(exp1))
    
    val intere = new BaseScalaInterp{}
    intere.ctx("R") = List(Map("a" -> 42, "b" -> "Milos"), Map("a" -> 49, "b" -> "Michael"), 
                           Map("a" -> 34, "b" -> "Jaclyn"), Map("a" -> 42, "b" -> "Thomas"))
    val finse = new Finalizer(intere)
    println(finse.finalize(exp1)) 
  
    val interu = new BaseUnnester{}
    val finsu = new Finalizer(interu)
    println(fins.finalize(finsu.finalize(exp1).asInstanceOf[CExpr]))

    val exp2 = {
      import translator._
      val itemTp = TupleType("a" -> IntType, "b" -> StringType)
      val relationR = BagVarRef(VarDef("R", BagType(itemTp)))
      val xdef = VarDef("x", itemTp)
      val r1 = Union(Singleton(Tuple("a" -> Const(42, IntType), "b" -> Const("Milos", StringType))),
                Union(Singleton(Tuple("a" -> Const(49, IntType), "b" -> Const("Michael", StringType))), 
                  Union(Singleton(Tuple("a" -> Const(34, IntType), "b" -> Const("Jaclyn", StringType))),
                    Singleton(Tuple("a" -> Const(42, IntType), "b" -> Const("Thomas", StringType)))))) 
  
      val r2 = Singleton(Tuple("a" -> Const(42, IntType), "b" -> Const("SINGLETON", StringType)))
  
      val r = Singleton(Tuple(Map[String,TupleAttributeExpr]()))
  
      val v = VarDef("v", TupleType(Map[String,TupleAttributeType]()))
  
      val q2 = ForeachUnion(v, r, Singleton(TupleVarRef(v)))
  
      val q0 = ForeachUnion(xdef, r2,
                ForeachUnion(v, r,
                IfThenElse(Cond(OpGt, TupleVarRef(xdef)("a"), Const(40, IntType)),
                  Singleton(Tuple("w" -> TupleVarRef(xdef)("b"))))))
  
      val xvd = VarDef("x", TupleType("a" -> IntType))
  
      val q1 = Let(xvd, Tuple("a" -> Const(42, IntType)), TupleVarRef(xvd)("a"))
  
      val ydef = VarDef("y", itemTp)
      val q = ForeachUnion(xdef, relationR,
                Singleton(Tuple("w1" -> TupleVarRef(xdef)("a"), 
                  "w2" -> Total(ForeachUnion(ydef, relationR, 
                    Singleton(Tuple("w3" -> TupleVarRef(ydef)("b"))))))))
  
      val q3 = Total(ForeachUnion(xdef, relationR, Singleton(Tuple("w3" -> TupleVarRef(xdef)("b")))))
      val printer = new shredding.nrc.Printer{}
  
      println(printer.quote(q.asInstanceOf[printer.Expr]))
      translate(q)   
    }

    println("")
    println("calculus: ")
    println(fins.finalize(exp2))
    println(finse.finalize(exp2))
    println("")
    val intern = new BaseNormalized{}
    val finsn = new Finalizer(intern)
    val normalized = finsn.finalize(exp2).asInstanceOf[CExpr]
    println("normalized: ")
    println(fins.finalize(normalized))
    println(finse.finalize(normalized))
  }

  def run3(){
      val relationRValues = List(Map(
          "h" -> 42,
          "j" -> List(
            Map(
              "m" -> "Milos",
              "n" -> 123,
              "k" -> List(
                Map("n" -> 123),
                Map("n" -> 456),
                Map("n" -> 789),
                Map("n" -> 123)
              )
            ),
            Map(
              "m" -> "Michael",
              "n" -> 7,
              "k" -> List(
                Map("n" -> 2),
                Map("n" -> 9),
               Map("n" -> 1)
              )
            ),
            Map(
              "m" -> "Jaclyn",
              "n" -> 12,
              "k" -> List(
                Map("n" -> 14),
                Map("n" -> 12)
              )
            )
          )
        ),
        Map(
          "h" -> 69,
          "j" -> List(
            Map(
              "m" -> "Thomas",
              "n" -> 987,
              "k" -> List(
                Map("n" -> 987),
                Map("n" -> 654),
                Map("n" -> 987),
                Map("n" -> 654),
                Map("n" -> 987),
                Map("n" -> 987)
              )
            )
          )
        )
      )
    
    val translator = new NRCTranslator{}
        
    val exp1  = {
      import translator._ 

      val nested2ItemTp = TupleType(Map("n" -> IntType))

      val nestedItemTp = TupleType(Map(
        "m" -> StringType,
        "n" -> IntType,
        "k" -> BagType(nested2ItemTp)
      ))
      val itemTp = TupleType(Map(
        "h" -> IntType,
        "j" -> BagType(nestedItemTp)
      ))

      val relationR = BagVarRef(VarDef("R", BagType(itemTp)))
      
      val xdef = VarDef("x", itemTp)
      val xref = TupleVarRef(xdef)
      val wdef = VarDef("w", nestedItemTp)
      val wref = TupleVarRef(wdef)
      val ndef = VarDef("y", TupleType("n" -> IntType))

      // { (w := x.j) | x <- R, x.h > 60 }
      // Reduce (w := x1.j) <--- x1 --- Select (x1.h < 60)(R)
      val q0 = ForeachUnion(xdef, relationR, 
                IfThenElse(Cond(OpGt, TupleVarRef(xdef)("h"), Const(60, IntType)),
                  Singleton(Tuple("w" -> TupleVarRef(xdef)("j")))))
      
      // { ( w := { 1 | x1 <- x.j }) | x0 <- R, x0 > 60 }
      // Reduce (w := v0) <-- v0 -- Nest(1, x1) <-- x1 -- Outerunnest(x0.j) <-- x0 -- Select (x0 > 60 )(R)
      val q1 = ForeachUnion(xdef, relationR, 
                IfThenElse(Cond(OpGt, TupleVarRef(xdef)("h"), Const(60, IntType)),
                  Singleton(Tuple("w" -> Total(TupleVarRef(xdef)("j").asInstanceOf[BagExpr])))))
      
      val x2def = VarDef("x2", itemTp)
      val q2 = ForeachUnion(xdef, relationR, 
                ForeachUnion(x2def, relationR, 
                  IfThenElse(Cond(OpEq, TupleVarRef(xdef)("h"), TupleVarRef(x2def)("h")), 
                    Singleton(Tuple("x" -> TupleVarRef(xdef)("j"), "x2" -> TupleVarRef(x2def)("j"))))))
        
      val q4 = ForeachUnion(xdef, relationR,
        Singleton(Tuple(
          "o5" -> xref("h"),
          "o6" ->
            ForeachUnion(wdef, BagProject(xref, "j"),
              Singleton(Tuple(
                "o7" -> wref("m"),
                "o8" -> Total(BagProject(wref, "k"))
              ))
            )
        ))) 
      translate(q4) 
   }
    
   val inters = new BaseStringify{}
   val fins = new Finalizer(inters) 
   println(fins.finalize(exp1))
    
   val intere = new BaseScalaInterp{}
   intere.ctx("R") = relationRValues
   
    val finse = new Finalizer(intere)
  
    println("")
    println("calculus: ")
    println(fins.finalize(exp1))
    println("")
    println(finse.finalize(exp1))
    println("")
    val intern = new BaseNormalized{}
    val finsn = new Finalizer(intern)
    val normalized = finsn.finalize(exp1).asInstanceOf[CExpr]
    
    println("normalized: ")
    println(fins.finalize(normalized))
    println("")
    println(finse.finalize(normalized))
    println("")
    /** { ( o5 := x.h, o6 := { ( o7 := w.m, o8 :=  + { "1" |  v1 <- w.k  } ) |  w <- x.j  } ) |  x <- R  }
      *
      * Reduce[ U / lambda(x,v2).( o5 := x.h, o6 := v2 ), lambda(x,v2)."true"]
      * Nest[ U / lambda(x,w,v3).( o7 := w.m, o8 := v3 ) / lambda(x,w,v3).x,
      *       lambda(x,w,v3)."true" / lambda(x,w,v3).w,v3]
      *   Nest[ + / lambda(x,w,v1)."1" / lambda(x,w,v1).x,w, lambda(x,w,v1)."true" / lambda(x,w,v1).v1]
      *     OuterUnnest[lambda(x,w).w.k, lambda(x,w)."true"]
      *        OuterUnnest[lambda(x).x.j, lambda(x)."true"]
      *          Select[lambda(x)."true"](R)
      */
       
    val interu = new BaseUnnester{}
    val finsu = new Finalizer(interu)
    val plan = finsu.finalize(normalized)
    println(plan)
    println("")
    println("plan:")
    println(fins.finalize(plan.asInstanceOf[CExpr]))
  }

}
