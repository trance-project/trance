package shredding.algebra

import shredding.core._

object FlatTest{
  val compiler = new BaseCompiler{}
  import compiler._
  val cdata = input(List(record(Map("a" -> constant(42), "b" -> constant("Milos"))), 
                            record(Map("a" -> constant(49), "b" -> constant("Michael"))), 
                            record(Map("a" -> constant(34), "b" -> constant("Jaclyn"))), 
                            record(Map("a" -> constant(42), "b" -> constant("Thomas")))))

  val relationRType = BagType(TupleType("a" -> IntType, "b" -> StringType))
  val relationRValues = List(Map("a" -> 42, "b" -> "Milos"), Map("a" -> 49, "b" -> "Michael"),
                           Map("a" -> 34, "b" -> "Jaclyn"), Map("a" -> 42, "b" -> "Thomas"))
  val rF = Label.fresh()
  val rDc = (List((rF, relationRValues)), ())
  val rDu = relationRValues
}

object NestedTest {
     val compiler = new BaseCompiler{} 
     import compiler._

     val cdata = input(List(record(Map("h" -> constant(42), "j" -> 
                  input(List(record(Map("m" -> constant("Milos"), "n" -> constant(123), "k" -> 
                    input(List(record(Map("n" -> constant(123))), 
                               record(Map("n" -> constant(456))), 
                               record(Map("n" -> constant(789))), 
                               record(Map("n" -> constant(123))))))),
                      record(Map("m" -> constant("Michael"), "n" -> constant(7), "k" -> 
                    input(List(record(Map("n" -> constant(2))), 
                               record(Map("n" -> constant(9))), 
                               record(Map("n" -> constant(1))))))),
                      record(Map("m" -> constant("Jaclyn"), "n" -> constant(7), "k" -> 
                    input(List(record(Map("n"-> constant(14))), 
                               record(Map("n" -> constant(12))))))))))),
                  record(Map("h" -> constant(69), "j" ->
                    input(List(record(Map("m" -> constant("Thomas"), "n" -> constant(987), "k" -> 
                      input(List(record(Map("n" -> constant(987))),
                                 record(Map("n" -> constant(654))),
                                 record(Map("n" -> constant(987))),
                                 record(Map("n" -> constant(654))),
                                 record(Map("n" -> constant(987))), 
                                 record(Map("n" -> constant(987))))))))))))) 

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


      val inputRelationType = BagType(itemTp)
      val inputRelation = List(Map(
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

      val rF = Label.fresh()
      val rD1 = inputRelation.map{m => (m("h"), Label.fresh(), m("j").asInstanceOf[List[Map[String,Any]]]) }
      val rD2 = rD1.map{ case (h, l, j) => (Label.fresh(), j.map{ m => 
                  (m("m"), m("n"), Label.fresh(), m("k").asInstanceOf[List[_]])})}
      val rD2j1 = rD2.map{ case (l, j) => (l, j.map{ case (m, n, l2, k) => Map("m" -> m, "n" -> n, "k" -> l2) }) }  
      val rD2j2k1 = rD2.flatMap{ case (l, j) => j.map{ case (m, n, l2, k) => (l2, k) } }
      val rDflat = rD1.map{ case (h,l,j) => Map("h" -> h, "j" -> l)}

      
      val tupleDict0 = scala.collection.mutable.Map[String, (Any, Any)]()
      val tupleDict = scala.collection.mutable.Map[String, (Any, Any)]()
      tupleDict0("k") = (rD2j2k1, ())
      tupleDict("j") = (rD2j1, tupleDict0)
      val rDc = (List((rF, rDflat)), tupleDict)
      // not sure about this yet 
      val rDu = (rDflat, tupleDict)
}



object App {

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
      val data = FlatTest.cdata 
      //val ffun = (i: Rep) => gt(project(i,"a"), const(40))
      //val pmat = (i: Rep) => record(Map("w" -> project(i, "b")))
      //comprehension(data, ffun, pmat)
      // { (x, y) | x <- R, y <- R }
      // { { (x,y) | y <- R } | x <- R }
      /**comprehension(data, x => const(true), (z: Rep) => 
        comprehension(data, x => const(true), (x: Rep) => 
          comprehension(data, x => const(true), (y: Rep) => record(Map("x" -> x, "y" -> y)))))**/
      bind(data, (i: Rep) => comprehension(i, x => constant(true), (x: Rep) => record(Map("x" -> project(x, "a")))))
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
      val data = NestedTest.cdata 
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
                (i: Rep) => record(Map("o7" -> project(project(project(i, "key"), "value"), "m"), 
                  "o8" -> project(i, "value"))), x => constant(true))
      s5
      // (x,v2) 
      //val s6 = reduce(s5, (i: Rep) => record(Map("o5" -> project(project(i, "key"), "h"), 
      //        "o6" -> project(i, "value"))), x => const(true))
      //s6
     }

    val inters = new BaseStringify{}
    val inter = new BaseScalaInterp{}
    val finalizer = new Finalizer(inter)
    val finalizers = new Finalizer(inters)
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
    val ctx = Map(Variable("R",BagCType(RecordCType("a" -> IntType, "b" -> StringType))) -> "R")
    println(fins.finalize(exp1))
    
    val intere = new BaseScalaInterp{}
    intere.ctx("R") = FlatTest.cdata
    val finse = new Finalizer(intere)
    println(finse.finalize(exp1)) 
  

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
      val ydef = VarDef("y", itemTp)
 
      // { v | v <- {} }
      val q2 = ForeachUnion(v, r, Singleton(TupleVarRef(v)))
      
      // { ( w := v.a ) | v <- {e1}, p(v) } 
      val q4 = ForeachUnion(xdef, r2, 
                IfThenElse(Cond(OpEq, TupleVarRef(xdef)("a"), Const(42, IntType)), 
                  Singleton(Tuple("w" -> TupleVarRef(xdef)("a")))))
      // { ( w1 := v.a, w2 := { v2.b | v2 <- {e2}, p(v2) } ) } v <- {e1}, p(v) }
      val q4b = ForeachUnion(xdef, r2, 
                IfThenElse(Cond(OpEq, TupleVarRef(xdef)("a"), Const(42, IntType)), 
                  Singleton(Tuple("w1" -> TupleVarRef(xdef)("a"),
                    "w2" -> ForeachUnion(ydef, r2, 
                              IfThenElse(Cond(OpGt, TupleVarRef(ydef)("a"), Const(45, IntType)), 
                                Singleton(Tuple("w2" -> TupleVarRef(ydef)("b")))))))))

      // { ( w1 := v.a, w2 := { v2.b | v2 <- R } ) } v <- {e1}, p(v) }
      val q4c = ForeachUnion(xdef, r2, 
                IfThenElse(Cond(OpEq, TupleVarRef(xdef)("a"), Const(42, IntType)), 
                  Singleton(Tuple("w1" -> TupleVarRef(xdef)("a"),
                    "w2" -> ForeachUnion(ydef, relationR,
                              ForeachUnion(xdef, r2,  
                                Singleton(Tuple("w2" -> TupleVarRef(ydef)("b")))))))))

      val r3 = Singleton(Tuple("a" -> Const(42, IntType), "b" -> 
                ForeachUnion(xdef, relationR, Singleton(Tuple("c" -> TupleVarRef(xdef)("b"))))))
      val x2 = VarDef("x2", TupleType(Map("a" -> IntType, "b" -> BagType(TupleType("c" -> StringType)))))
      val q4d = ForeachUnion(x2, r3, TupleVarRef(x2)("b").asInstanceOf[BagExpr])

      val q0 = ForeachUnion(xdef, r2,
                ForeachUnion(v, r,
                IfThenElse(Cond(OpGt, TupleVarRef(xdef)("a"), Const(40, IntType)),
                  Singleton(Tuple("w" -> TupleVarRef(xdef)("b"))))))
  
      val xvd = VarDef("x", TupleType("a" -> IntType))
  
      val q1 = Let(xvd, Tuple("a" -> Const(42, IntType)), TupleVarRef(xvd)("a"))
  
      val q = ForeachUnion(xdef, relationR,
                Singleton(Tuple("w1" -> TupleVarRef(xdef)("a"), 
                  "w2" -> Total(ForeachUnion(ydef, relationR, 
                    Singleton(Tuple("w3" -> TupleVarRef(ydef)("b"))))))))
  
      // broken case
      val q6 = ForeachUnion(xdef, relationR,
                ForeachUnion(ydef, relationR,
                  Singleton(Tuple("o1" -> TupleVarRef(xdef)("a"), "o2" -> TupleVarRef(ydef)("a")))))
      val q3 = Total(ForeachUnion(xdef, relationR, Singleton(Tuple("w3" -> TupleVarRef(xdef)("b")))))

      val q5 = ForeachUnion(xdef, ForeachUnion(ydef, relationR, Singleton(TupleVarRef(ydef))),
                  Singleton(Tuple("o1" -> TupleVarRef(xdef)("a"))))
      val q7 = Total(ForeachUnion(ydef, relationR, 
                Singleton(Tuple("c" -> Total(ForeachUnion(xdef, relationR, Singleton(TupleVarRef(xdef))))))))
      val printer = new shredding.nrc.Printer{}
  
      println(printer.quote(q.asInstanceOf[printer.Expr]))
      translate(q7)   
    }

    println("")
    println("calculus: ")
    println(fins.finalize(exp2))
    println(finse.finalize(exp2))
    println("")
    val intern = new BaseNormalizer{}
    val finsn = new Finalizer(intern)
    val normalized = finsn.finalize(exp2).asInstanceOf[CExpr]
    println("normalized: ")
    println(fins.finalize(normalized))
    println(finse.finalize(normalized))
  }

  def run3(){
    
    val translator = new NRCTranslator{}
    val relationRValues = NestedTest.inputRelation
    val itemTp = NestedTest.itemTp

    val exp1  = {
      import translator._ 

      val relationR = BagVarRef(VarDef("R", NestedTest.inputRelationType))
      
      val xdef = VarDef("x", NestedTest.itemTp)
      val xref = TupleVarRef(xdef)
      val wdef = VarDef("w", NestedTest.nestedItemTp)
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
    val intern = new BaseNormalizer{}
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
       
    /**val interu = new BaseUnnester{}
    val finsu = new Finalizer(interu)
    val plan = finsu.finalize(normalized)
    println(plan)
    println("")
    println("plan:")
    println(fins.finalize(plan.asInstanceOf[CExpr]))**/
  }

  def runNormalizationTests(){
    val translator = new NRCTranslator{}
    val oldcalc = new BackTranslator{}
    
    import translator._
     
    val inters = new BaseStringify{}
    val fins = new Finalizer(inters) 
    val ctx = Map(Variable("R",BagCType(RecordCType("a" -> IntType, "b" -> StringType))) -> "R")
   
    val intere = new BaseScalaInterp{}
    intere.ctx("R") = FlatTest.relationRValues
    val finse = new Finalizer(intere)

    val intern = new BaseNormalizer{}
    val finsn = new Finalizer(intern)
    
    val itemTp = TupleType("a" -> IntType, "b" -> StringType)
    val relationR = BagVarRef(VarDef("R", FlatTest.relationRType))  
    val xdef = VarDef("x", itemTp)
    val rdef = VarDef("r", FlatTest.relationRType)
 
    // N3
    val q1 = translate(Let(xdef, Tuple("a" -> Const(42, IntType), "b" -> Const("Test", StringType)), 
                TupleVarRef(xdef)("a")))
    val fmq1 = {
      import oldcalc._
      translate(q1).quote
    }
    println("Old version:")
    println(fmq1)
    println("")
    // { x1.a | x1 := (a := 42,b := Test) } 
    println("Comprehension calculus:")
    println(fins.finalize(q1))
    println(finse.finalize(q1))
    println("") 
    // 42
    println("Normalized:")
    val nq1 = finsn.finalize(q1).asInstanceOf[CExpr]
    println(fins.finalize(nq1))
    println(finse.finalize(nq1))
    println("")

    // { (o1 := x.b) | x <- R }
    val q2 = translate(Let(rdef, relationR, 
                ForeachUnion(xdef, BagVarRef(rdef), Singleton(Tuple("o1" -> TupleVarRef(xdef)("b")))))) 
    //val nrcq2 = Let(rdef, Singleton(Tuple("a" -> Const(1, IntType), "b" -> Const("one", StringType))),
    //          ForeachUnion(xdef, BagVarRef(rdef), Singleton(Tuple("o1" -> TupleVarRef(xdef)("b")))))
    //val q2 = translate(nrcq2)
    val fmq2 = {
      import oldcalc._
      translate(q2).quote
    }
    println("Old version:")
    println(fmq2)
    println("")
    //{ { (o1 := x3.b) | x3 <- x2 } | x2 := { (a := 1,b := one) } }
    println("Comprehension calculus:")
    println(fins.finalize(q2))
    println(finse.finalize(q2))
    println("") 
    // { (o1 := one) }
    println("Normalized:")
    val nq2 = finsn.finalize(q2).asInstanceOf[CExpr]
    println(fins.finalize(nq2))
    println(finse.finalize(nq2))
    println("")

    // N4
    val ydef = VarDef("y", itemTp)

    // for y in R union
    //   for x in [ if (y.a > 40) then sng(y) else sng(y.a, "null) ]
    //     if (x.a > 40) then sng(o1 := y.b)
    // complex example
    val sq3 = ForeachUnion(ydef, relationR, 
                ForeachUnion(xdef, IfThenElse(Cond(OpGe, TupleVarRef(ydef)("a"), Const(40, IntType)),
                                    Singleton(TupleVarRef(ydef)), Singleton(Tuple("a" -> TupleVarRef(ydef)("a"), 
                                        "b" -> Const("null", StringType)))).asInstanceOf[BagExpr],
                  IfThenElse(Cond(OpGt, TupleVarRef(xdef)("a"), Const(41, IntType)), 
                Singleton(Tuple("o1" -> TupleVarRef(xdef)("b"))))))
    val q3 = translate(sq3)
    // { { (o1 := x6.b) | x7 <- if x6.a > 40 
    //                          then { x6 } else { (a := x6.a,b := null) }, x7.a > 50 } 
    //    | x6 <- R }
    val fmq3 = {
      import oldcalc._
      (translate(q3).quote, translate(q3).normalize.quote)
    }
    println("Old version:")
    println(fmq3._1)
    println(fmq3._2)
    println("")
    println("Comprehension calculus:")
    println(fins.finalize(q3))
    println(finse.finalize(q3))
    println("") 
    // { if (x9.a >= 40)
    //   then if (x9.a > 40) then { ( o1 := x9.b) } 
    //   else if (x9.a > 40) then { (o1 := null) } 
    //  | x9 <- R }
    println("Normalized:")
    val nq3 = finsn.finalize(q3).asInstanceOf[CExpr]
    println(fins.finalize(nq3))
    println(finse.finalize(nq3))
    println("")

    val sq4 = ForeachUnion(ydef, relationR, 
                ForeachUnion(xdef, IfThenElse(Cond(OpGe, TupleVarRef(ydef)("a"), Const(40, IntType)),
                                    Singleton(TupleVarRef(ydef)), Singleton(Tuple("a" -> TupleVarRef(ydef)("a"), 
                                        "b" -> Const("null", StringType)))).asInstanceOf[BagExpr],
                  Singleton(Tuple("o1" -> TupleVarRef(xdef)("b")))))
    val q4 = translate(sq4)
    // { { (o1 := x6.b) | x7 <- if x6.a > 40 
    //                          then { x6 } else { (a := x6.a,b := null) } } 
    //    | x6 <- R }
    val fmq4 = {
      import oldcalc._
      (translate(q3).quote, translate(q3).normalize.quote)
    }
    println("Old version:")
    println(fmq4._1)
    println(fmq4._2)
    println("")
    println("Comprehension calculus:")
    println(fins.finalize(q4))
    println(finse.finalize(q4))
    println("") 
    // { if (x9.a >= 40) then { (o1 := x9.b) } else { (o1 := null) } | x9 <- R |
    println("Normalized:")
    val nq4 = finsn.finalize(q4).asInstanceOf[CExpr]
    println(fins.finalize(nq4))
    println(finse.finalize(nq4))
    println("")

    val sq5 = ForeachUnion(xdef, relationR, 
              ForeachUnion(ydef, relationR,
                Singleton(Tuple("o1" -> TupleVarRef(xdef)("a"), "o2" -> TupleVarRef(ydef)("a")))))
    val q5 = translate(sq5)
    val fmq5 = {
      import oldcalc._
      (translate(q4).quote, translate(q4).normalize.quote)
    }
    println("Old version:")
    println(fmq5._1)
    println(fmq5._2)
    println("")
    println("Comprehension calculus:")
    println(fins.finalize(q5))
    println(finse.finalize(q5))
    println("") 
    println("Normalized:")
    val nq5 = finsn.finalize(q5).asInstanceOf[CExpr]
    println(fins.finalize(nq5))
    println(finse.finalize(nq5))
    println("")
   
  }

  def runShred(){
    val shredder = new ShredPipeline{};
    val exp1 = {
      import shredder._
      val itemTp = TupleType("a" -> IntType, "b" -> StringType)
      val relationR = BagVarRef(VarDef("R", BagType(itemTp))) 
      val x = VarDef("x", itemTp)
      val q = ForeachUnion(x, relationR, Singleton(Tuple("o1" -> TupleVarRef(x)("a"))))
      shredPipeline(q)
    }

    val bstr = new BaseStringify{}
    val str = new Finalizer(bstr)
    val bnorm = new BaseNormalizer{}
    val norm = new Finalizer(bnorm)
    println("\nTranslated:")
    println(str.finalize(exp1))
    println("\nNormalized:")
    val nexp1 = norm.finalize(exp1).asInstanceOf[CExpr]
    println(str.finalize(nexp1))
    val plan = Unnester.unnest(nexp1)((Nil, Nil, None))
    println("\nPlan:")
    println(str.finalize(plan))
    val plan1 = norm.finalize(plan).asInstanceOf[CExpr]
    println("\nOptimized:")
    println(str.finalize(plan1))
   
    /**val beval = new BaseScalaInterp{}
    beval.ctx("R^F") = FlatTest.rF
    beval.ctx("R^D") = FlatTest.rDc
    val eval = new Finalizer(beval)

    println("\nEvaluated:")
    eval.finalize(nexp1)**///.asInstanceOf[List[_]].foreach(println(_))

    val exp2 = {
      import shredder._
      val itemTp = NestedTest.itemTp
      val relationR = BagVarRef(VarDef("R", NestedTest.inputRelationType)) 

      val xdef = VarDef("x", NestedTest.itemTp)
      val xref = TupleVarRef(xdef)
      val wdef = VarDef("w", NestedTest.nestedItemTp)
      val wref = TupleVarRef(wdef)
      val ndef = VarDef("y", TupleType("n" -> IntType))

      val q = ForeachUnion(xdef, relationR,
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
      val q2 = ForeachUnion(xdef, relationR, 
                Singleton(Tuple("o5" -> xref("h"), "o6" -> Total(xref("j").asInstanceOf[BagExpr]))))
      shredPipeline(q)
    }

    /**beval.ctx.clear
    beval.ctx("R^F") = NestedTest.rF
    beval.ctx("R^D") = NestedTest.rDc**/

    println("\nTranslated:")
    println(str.finalize(exp2))
    println("\nNormalized:")
    val nexp2 = norm.finalize(exp2).asInstanceOf[CExpr]
    println(str.finalize(nexp2))
    val plan2 = Unnester.unnest(nexp2)((Nil, Nil, None))
    println("\nPlan:")
    println(str.finalize(plan2))
    val plan2a = norm.finalize(plan2).asInstanceOf[CExpr]
    println("\nOptimized:")
    println(str.finalize(plan2a))
 
    val exp3 = {
      import shredder._
      val ytp = TupleType("b" -> IntType, "c" -> IntType)
      val xtp = TupleType("a" -> IntType, "s1" -> BagType(ytp), "s2" -> BagType(ytp))
      val xdef = VarDef("x", xtp)
      val ydef = VarDef("y", ytp)

      val r = BagVarRef(VarDef("R", BagType(xtp)))
      val q = ForeachUnion(xdef, r,
              Singleton(Tuple("a'" -> TupleVarRef(xdef)("a"),
                "s1'" -> ForeachUnion(ydef, BagProject(TupleVarRef(xdef), "s1"),
                          IfThenElse(Cond(OpGt, Const(5, IntType), TupleVarRef(ydef)("c")), Singleton(TupleVarRef(ydef)))),
                "s2'" -> ForeachUnion(ydef, BagProject(TupleVarRef(xdef), "s2"),
                          IfThenElse(Cond(OpGt, TupleVarRef(ydef)("c"), Const(6, IntType)),
                            Singleton(TupleVarRef(ydef)))))))
      shredPipeline(q)
    }
 
    /**println("\nTranslated:")
    println(str.finalize(exp3))
    println("\nNormalized:")
    val nexp3 = norm.finalize(exp3).asInstanceOf[CExpr]
    println(str.finalize(nexp3))
 
    val exp4 = {
      import shredder._
      val ytp = TupleType("b" -> IntType, "c" -> IntType)
      val xtp = TupleType("a" -> IntType, "s1" -> BagType(ytp))
      val xdef = VarDef("x", xtp)
      val ydef = VarDef("y", ytp)

      val r = BagVarRef(VarDef("R", BagType(xtp)))
      val q = ForeachUnion(xdef, r,
              Singleton(Tuple("a'" -> TupleVarRef(xdef)("a"),
                "s1'" -> ForeachUnion(ydef, BagProject(TupleVarRef(xdef), "s1"),
                          IfThenElse(Cond(OpGt, Const(5, IntType), TupleVarRef(ydef)("c")), Singleton(TupleVarRef(ydef)))))))
      shredPipeline(q)
    }
 
    println("\nTranslated:")
    println(str.finalize(exp4))
    println("\nNormalized:")
    val nexp4 = norm.finalize(exp4).asInstanceOf[CExpr]
    println(str.finalize(nexp4))**/
  }

  def runBase(){
    val translator = new NRCTranslator{}

    val exp1 = {
      import translator._
      val itemTp = TupleType("a" -> IntType, "b" -> BagType(TupleType("c" -> IntType)))
      val relationR = BagVarRef(VarDef("R", BagType(itemTp)))
      val x = VarDef("x", itemTp)
      val x1 = VarDef("x1", itemTp)
      val y = VarDef("y", TupleType("c" -> IntType))
      val q = ForeachUnion(x, relationR, 
                ForeachUnion(x1, relationR,
                  ForeachUnion(y, BagProject(TupleVarRef(x), "b"), 
                    Singleton(Tuple("o1" -> TupleVarRef(x1)("a"), "o2" -> Total(BagProject(TupleVarRef(x), "b")))))))
      translate(q)
    }

    val exp2 = {
      import translator._
      val itemTp = NestedTest.itemTp
      val relationR = BagVarRef(VarDef("R", NestedTest.inputRelationType)) 

      val xdef = VarDef("x", NestedTest.itemTp)
      val xref = TupleVarRef(xdef)
      val wdef = VarDef("w", NestedTest.nestedItemTp)
      val wref = TupleVarRef(wdef)
      val ndef = VarDef("y", TupleType("n" -> IntType))

      val q = ForeachUnion(xdef, relationR,
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
      translate(q)
    }



    val bstr = new BaseStringify{}
    val str = new Finalizer(bstr)
    val bnorm = new BaseNormalizer{}
    val norm = new Finalizer(bnorm)
    
    val normalized = norm.finalize(exp1).asInstanceOf[CExpr]
    println(str.finalize(normalized))
    val unnested = Unnester.unnest(normalized)((Nil, Nil, None))
    val unnestedOpt = norm.finalize(unnested).asInstanceOf[CExpr] // call bind
    println(str.finalize(unnestedOpt))

    println("")
    val normalized2 = norm.finalize(exp2).asInstanceOf[CExpr]
    println(str.finalize(normalized2))
    val unnested2 = Unnester.unnest(normalized2)((Nil, Nil, None))
    println("Plan")
    println(str.finalize(unnested2))
    println("Optimized Plan")
    val unnestedOpt2 = norm.finalize(unnested2).asInstanceOf[CExpr] // call bind
    println(str.finalize(unnestedOpt2))

  }

  def main(args: Array[String]){
    // run1()
    //run2()
    //run3()
    //runNormlizationTests()
    runShred()
    //runBase()
  }


}
