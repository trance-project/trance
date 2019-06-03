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
  val relationRValues2 = List(List(42, "Milos"), List(49, "Michael"),
                           List(34, "Jaclyn"), List(42, "Thomas"))

  val relationRType3 = RecordCType("a" -> IntType, "b" -> BagCType(RecordCType("c" -> IntType)))
  val relationRValues3 = List(Map("a" -> 42, "b" -> List(Map("c" -> 1), Map("c" -> 2), Map("c" -> 4))), 
                              Map("a" -> 49, "b" -> List(Map("c" -> 3), Map("c" -> 2))),
                              Map("a" -> 34, "b" -> List(Map("c" ->5))))

  val rF = Label.fresh()
  val rFType = rF.tp
  val rDc = (List((rF, relationRValues)), ())
  val rDType = BagDictCType(BagCType(TTupleType(List(rF.tp, relationRType))), EmptyDictCType)
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
      val rFType = rF.tp
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
      val tdictt = TupleDictCType(Map("h" -> EmptyDictCType, "j" -> 
        BagDictCType(BagCType(TTupleType(List(rF.tp, 
          BagCType(RecordCType("m" -> StringType, "n" -> IntType, "j" -> rF.tp))))), 
            TupleDictCType(Map("m" -> EmptyDictCType, "n" -> EmptyDictCType, "k" -> 
              BagDictCType(BagCType(TTupleType(List(rF.tp, 
                BagCType(RecordCType("n" -> IntType))))), EmptyDictCType))))))
      val rDc = (List((rF, rDflat)), tupleDict)
      val rDType = BagDictCType(BagCType(TTupleType(List(rF.tp, BagCType(RecordCType("h" -> IntType, "j" -> rF.tp))))), tdictt)
      // not sure about this yet 
      val rDu = (rDflat, tupleDict)
}



object App {

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
                IfThenElse(Cmp(OpGt, TupleVarRef(xdef)("a"), Const(40, IntType)),
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
                IfThenElse(Cmp(OpEq, TupleVarRef(xdef)("a"), Const(42, IntType)), 
                  Singleton(Tuple("w" -> TupleVarRef(xdef)("a")))))
      // { ( w1 := v.a, w2 := { v2.b | v2 <- {e2}, p(v2) } ) } v <- {e1}, p(v) }
      val q4b = ForeachUnion(xdef, r2, 
                IfThenElse(Cmp(OpEq, TupleVarRef(xdef)("a"), Const(42, IntType)), 
                  Singleton(Tuple("w1" -> TupleVarRef(xdef)("a"),
                    "w2" -> ForeachUnion(ydef, r2, 
                              IfThenElse(Cmp(OpGt, TupleVarRef(ydef)("a"), Const(45, IntType)), 
                                Singleton(Tuple("w2" -> TupleVarRef(ydef)("b")))))))))

      // { ( w1 := v.a, w2 := { v2.b | v2 <- R } ) } v <- {e1}, p(v) }
      val q4c = ForeachUnion(xdef, r2, 
                IfThenElse(Cmp(OpEq, TupleVarRef(xdef)("a"), Const(42, IntType)), 
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
                IfThenElse(Cmp(OpGt, TupleVarRef(xdef)("a"), Const(40, IntType)),
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
                IfThenElse(Cmp(OpGt, TupleVarRef(xdef)("h"), Const(60, IntType)),
                  Singleton(Tuple("w" -> TupleVarRef(xdef)("j")))))
      
      // { ( w := { 1 | x1 <- x.j }) | x0 <- R, x0 > 60 }
      // Reduce (w := v0) <-- v0 -- Nest(1, x1) <-- x1 -- Outerunnest(x0.j) <-- x0 -- Select (x0 > 60 )(R)
      val q1 = ForeachUnion(xdef, relationR, 
                IfThenElse(Cmp(OpGt, TupleVarRef(xdef)("h"), Const(60, IntType)),
                  Singleton(Tuple("w" -> Total(TupleVarRef(xdef)("j").asInstanceOf[BagExpr])))))
      
      val x2def = VarDef("x2", itemTp)
      val q2 = ForeachUnion(xdef, relationR, 
                ForeachUnion(x2def, relationR, 
                  IfThenElse(Cmp(OpEq, TupleVarRef(xdef)("h"), TupleVarRef(x2def)("h")), 
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
                ForeachUnion(xdef, IfThenElse(Cmp(OpGe, TupleVarRef(ydef)("a"), Const(40, IntType)),
                                    Singleton(TupleVarRef(ydef)), Singleton(Tuple("a" -> TupleVarRef(ydef)("a"), 
                                        "b" -> Const("null", StringType)))).asInstanceOf[BagExpr],
                  IfThenElse(Cmp(OpGt, TupleVarRef(xdef)("a"), Const(41, IntType)), 
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
                ForeachUnion(xdef, IfThenElse(Cmp(OpGe, TupleVarRef(ydef)("a"), Const(40, IntType)),
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
    val str = Printer
    val bnorm = new BaseNormalizer{}
    val norm = new Finalizer(bnorm)

    val exp1 = {
      import shredder._
      val itemTp = TupleType("a" -> IntType, "b" -> StringType)
      val relationR = BagVarRef(VarDef("R", BagType(itemTp))) 
      val x = VarDef("x", itemTp)
      val q = ForeachUnion(x, relationR, Singleton(Tuple("o1" -> TupleVarRef(x)("a"))))
      shredPipeline(q)
    }

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

    val beval = new BaseScalaInterp{}
    val eval = new Finalizer(beval)

    println("\nTranslated:")
    println(str.quote(exp1))
    println("\nNormalized:")
    val nexp1 = norm.finalize(exp1).asInstanceOf[CExpr]
    println(str.quote(nexp1))
    beval.ctx("RF") = FlatTest.rF
    beval.ctx("RD") = FlatTest.rDc
    println("\n\nEvaluated:\n")
    eval.finalize(nexp1).asInstanceOf[List[_]].foreach(println(_))

    val nsgen = new ScalaGenerator{}
    nsgen.ctx("RF") = FlatTest.rFType
    nsgen.ctx("RD") = FlatTest.rDType
    val sgen = new Finalizer(nsgen)

    // exp0
    println("")
    val ccode = s"""
      | import shredding.algebra.FlatTest._
      | val RF = rF
      | val RD = rDc
      ${sgen.finalize(nexp1)}""".stripMargin
    println(ccode)

    println("")
    beval.ctx.clear
    beval.ctx("RF") = NestedTest.rF
    beval.ctx("RD") = NestedTest.rDc

    println("\nTranslated:")
    println(str.quote(exp2))
    println("\nNormalized:")
    val nexp2 = norm.finalize(exp2).asInstanceOf[CExpr]
    println(str.quote(nexp2))
    println("\nEvaluated:\n")
    eval.finalize(nexp2)
 
    nsgen.ctx.clear
    nsgen.ctx("RF") = NestedTest.rFType
    nsgen.ctx("RD") = NestedTest.rDType
 
    // exp1
    println("")
    val ccode2 = s"""
      | import shredding.algebra.NestedTest._
      | var ctx = scala.collection.mutable.Map[String,Any]()
      | val RF = rF
      | val RD = rDc
      ${sgen.finalize(nexp2)}""".stripMargin
    println(ccode2)
 
    /**val plan = Unnester.unnest(nexp1)((Nil, Nil, None))
    println("\nPlan:")
    println(str.finalize(plan))
    val plan1 = norm.finalize(plan).asInstanceOf[CExpr]
    println("\nOptimized:")
    println(str.finalize(plan1))
    println(plan1)
     


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
                          IfThenElse(Cmp(OpGt, Const(5, IntType), TupleVarRef(ydef)("c")), Singleton(TupleVarRef(ydef)))),
                "s2'" -> ForeachUnion(ydef, BagProject(TupleVarRef(xdef), "s2"),
                          IfThenElse(Cmp(OpGt, TupleVarRef(ydef)("c"), Const(6, IntType)),
                            Singleton(TupleVarRef(ydef)))))))
      shredPipeline(q)
    }
 
    println("\nTranslated:")
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
                          IfThenElse(Cmp(OpGt, Const(5, IntType), TupleVarRef(ydef)("c")), Singleton(TupleVarRef(ydef)))))))
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

    val exp0 = {
      import translator._
      val itemTp = TupleType("a" -> IntType, "b" -> BagType(TupleType("c" -> IntType)))
      val relationR = BagVarRef(VarDef("R", BagType(itemTp)))
      val x = VarDef("x", itemTp)
      val x1 = VarDef("x1", itemTp)
      val y = VarDef("y", TupleType("c" -> IntType))
      val q = ForeachUnion(x, relationR, 
                IfThenElse(Cmp(OpGt, TupleVarRef(x)("a"), Const(40, IntType)),
                  Singleton(Tuple("o1" -> TupleVarRef(x)("a"), "o2" -> Total(BagProject(TupleVarRef(x), "b"))))))
      translate(q)
    }

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

    val str = Printer
    val bnorm = new BaseNormalizer{}
    val norm = new Finalizer(bnorm)
    val beval = new BaseScalaInterp{}
    beval.ctx("R") = FlatTest.relationRValues3
    val eval = new Finalizer(beval)
    val nsgen = new ScalaGenerator{}
    nsgen.ctx("R") = FlatTest.relationRType3
    val sgen = new Finalizer(nsgen)
 
    // exp0
    val normalized0 = norm.finalize(exp0).asInstanceOf[CExpr]
    println(str.quote(normalized0))

    println("")
    val ccode = s"""
      | import shredding.algebra.FlatTest._
      | val R = relationRValues3
      | ${sgen.finalize(normalized0)}""".stripMargin

    println(ccode)

    println("")
    val anfBase = new BaseANF {}
    val anfed = new Finalizer(anfBase).finalize(normalized0)
    val anfExp = anfBase.anf(anfed.asInstanceOf[anfBase.Rep])
    println(str.quote(anfExp))
    val sanfgen = new Finalizer(new ScalaANFGenerator {})
    println(sanfgen.finalize(anfExp))

    println("")
    // exp2
    val normalized2 = norm.finalize(exp2).asInstanceOf[CExpr]
    println(str.quote(normalized2))
    println("")

    val anfed2 = new Finalizer(anfBase).finalize(normalized2)
    val anfExp2 = anfBase.anf(anfed2.asInstanceOf[anfBase.Rep])
    println(str.quote(anfExp2))
    println(sanfgen.finalize(anfExp2))


    // println(anfBase.vars)
    // println(anfBase.state)
    /**eval.finalize(normalized0).asInstanceOf[List[_]].foreach(println(_))
    println("")
    // exp1 
    val normalized = norm.finalize(exp1).asInstanceOf[CExpr]
    println(str.quote(normalized))
    eval.finalize(normalized).asInstanceOf[List[_]].foreach(println(_))
    println("")
    // exp2
    beval.ctx("R") = NestedTest.inputRelation
    val normalized2 = norm.finalize(exp2).asInstanceOf[CExpr]
    println(str.quote(normalized2))
    eval.finalize(normalized2).asInstanceOf[List[_]].foreach(println(_))
    
    
    /**val unnested = Unnester.unnest(normalized)((Nil, Nil, None))
    println(str.quote(unnested))
    val optimizer = new Finalizer(new PlanOptimizer{})
    val unnestedOpt = optimizer.finalize(unnested).asInstanceOf[CExpr] // call bind
    println(str.quote(unnestedOpt))

    //println(eval.finalize(unnested))

    println("\nEvaluated:")
    eval.finalize(unnestedOpt)///.asInstanceOf[List[_]].foreach(println(_))

    val unnested2 = Unnester.unnest(normalized2)((Nil, Nil, None))
    println("Plan")
    println(str.quote(unnested2))
    println("Optimized Plan")
    val unnestedOpt2 = optimizer.finalize(unnested2).asInstanceOf[CExpr] // call bind
    println(str.quote(unnestedOpt2))


    println("\nEvaluated:")
    eval.finalize(unnestedOpt2)///.asInstanceOf[List[_]].foreach(println(_))**/
    */
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
