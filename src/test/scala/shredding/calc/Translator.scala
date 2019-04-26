package shredding.calc

import org.scalatest.FunSuite
import shredding.core._
import shredding.nrc.NRC
import shredding.Utils.Symbol

class TranslatorTest extends FunSuite with NRC with NRCTranslator {

   def print(e: CompCalc) = println(calc.quote(e.asInstanceOf[calc.CompCalc]))

   /**
     * Flat relation tests
     */
   val itemTp = TupleType("a" -> IntType, "b" -> StringType)
   // flat relation
   val relationR = BagVarRef(VarDef("R", BagType(itemTp)))
//          InputBag("R", List(
//                    Map("a" -> "42", "b" -> "Milos"),
//                    Map("a" -> "69", "b" -> "Michael"),
//                    Map("a" -> "34", "b" -> "Jaclyn"),
//                    Map("a" -> "42", "b" -> "Thomas")
//                  ), BagType(itemTp))

   /**
     * sng( w := "one" ) union sng( w := "two" )
     *
     * translates to:
     * {(w := "one")} U {(w := "one")}
     */
   test("Translator.translate.Union"){
     val q = Union(Singleton(Tuple("w" -> Const("one", StringType))), 
              Singleton(Tuple("w" -> Const("two", StringType))))

     val cq = Merge(Sng(Tup("w" -> Constant("one", StringType))),
                Sng(Tup("w" -> Constant("two", StringType))))

     assert(Translator.translate(q) == cq)

   }

   /**
     * Tests the translation of a basic input query 
     * 
     * For x8 in R Union
     *  sng(( w := x8.b ))
     * 
     * { ( w := x8.b ) | x8 <- R }
     */
   test("Translator.translate.ForeachUnion") {
    val x = VarDef("x", itemTp)
    val q1 = ForeachUnion(x, relationR, Singleton(Tuple("w" -> TupleVarRef(x)("b"))))
    val tq1 = Translator.translate(q1)

    assert(tq1.tp == BagType(TupleType("w" -> StringType)))

    val cq1 = BagComp(Tup("w" -> Proj(TupleVar(x), "b")), List(
                Generator(x, BagVar(relationR.varDef))))

    assert(tq1 === cq1)

   }

   /**
     * For x1 in R Union
     *  For y22 in R Union
     *    If ((x1.b = y22.b))
     *    Then sng(( w1 := x1.a ))
     *
     * { v1 | x <- R, v1 <- { ( w1 := x1.a ) | x2 <- R, x1.b = x2.b } }
     */
   test("Translator.translate.ForeachUnionPred"){
    val x = VarDef("x", itemTp)
    val x2 = VarDef("y", itemTp)

    val q10 = ForeachUnion(x, relationR,
                ForeachUnion(x2, relationR,
                  IfThenElse(Cond(OpEq, TupleVarRef(x)("b"), TupleVarRef(x2)("b")),
                             Singleton(Tuple("w1" -> TupleVarRef(x)("a"))))))
    val tq10 = Translator.translate(q10)
    assert(tq10.tp == BagType(TupleType("w1" -> IntType)))

    val v1 = VarDef("v" + Symbol.getId("v"), TupleType("w1" -> IntType))
    val cq10 = BagComp(TupleVar(v1), List(Generator(x, BagVar(relationR.varDef)),
                Generator(v1, BagComp(Tup("w1" ->Proj(TupleVar(x), "a")), 
                  List(Generator(x2, BagVar(relationR.varDef)),
                    Conditional(OpEq, Proj(TupleVar(x), "b"), Proj(TupleVar(x2), "b")))))))
    assert(tq10 == cq10)

   }

   /**
     *
     * For x1 in R Union
     *  For y22 in R Union
     *    If ((x1.b = y22.b))
     *    Then sng(( w1 := x1.a ))
     *    Else sng(( w1 := -1 ))
     *
     * { v0 |  x1 <- R ,  v0 <- { v1 | y22 <- R,  v1 <- if (x1.b = y22.b) then (w1 := x1.a) else (w1 := -1) } }
     */
   test("Translator.translate.ForeachUnionIfStmtMerge"){
    
    val x = VarDef("x", itemTp)
    val x2 = VarDef("y", itemTp)
    
    val q11 = ForeachUnion(x, relationR,
                ForeachUnion(x2, relationR,
                  BagIfThenElse(Cond(OpEq, TupleVarRef(x)("b"), TupleVarRef(x2)("b")),
                                Singleton(Tuple("w1" -> TupleVarRef(x)("a"))),
                                Some(Singleton(Tuple("w1" -> Const("-1", IntType)))))))
    
    val tq11 = Translator.translate(q11).asInstanceOf[BagComp]
    assert(tq11.tp == BagType(TupleType("w1" -> IntType)))

    val v0 = VarDef("v" + Symbol.getId("v"), TupleType("w1" -> IntType))
    val v1 = VarDef("v" + (Symbol.getId("v")-1), TupleType("w1" -> IntType))
    
    val cq11 = BagComp(TupleVar(v0), List(Generator(x, BagVar(relationR.varDef)),
                  Generator(v0, BagComp(TupleVar(v1), List(Generator(x2, BagVar(relationR.varDef)),
                    Generator(v1, IfStmt(Conditional(OpEq, Proj(TupleVar(x), "b"), Proj(TupleVar(x2), "b")), 
                      Sng(Tup("w1" -> Proj(TupleVar(x), "a"))), Some(Sng(Tup("w1" -> Constant(-1, IntType)))))))))))
    
    assert(tq11.e == cq11.e)
    assert(tq11.qs(0).asInstanceOf[Generator].x == cq11.qs(0).asInstanceOf[Generator].x)
    assert(tq11.qs(0).asInstanceOf[Generator].e == cq11.qs(0).asInstanceOf[Generator].e)

   }

   /**
     * Tests total multiplicity calculation of
     * a simple input query 
     * 
     * For x8 in R Union
     *  sng(( w := x8.b ))
     * 
     * +{ ( w := x8.b ) | x8 <- R }
   test("Translator.translate.TotalMultForeach") {

    val q1 = TotalMult(ForeachUnion(x, relationR, Singleton(Tuple("w" -> VarRef(x, "b")))))
    
    val cq1 = CountComp(Constant("1", IntType), List(Generator(x, InputR("R", relationR.b))))
    println(Printer.quote(q1))
    println(Printer.quote(cq1))
    assert(Translator.translate(q1) === cq1)

   }*/

   /**
     * Nested relation tests 
     */
   val nstype = TupleType("c" -> IntType)
   val stype = TupleType("a" -> IntType, "b" -> StringType, "c" -> BagType(nstype))
   val relationS = BagVarRef(VarDef("S", BagType(stype)))
//      InputBag("S", List(
//                    Map("a" -> 42, "b" -> "Milos", "c" -> List(Map("c" -> 42), Map("c" -> 42), Map("c" -> 30))),
//                    Map("a" -> 69, "b" -> "Michael", "c" -> List(Map("c" -> 100), Map("c" -> 69), Map("c" -> 42))),
//                    Map("a" -> 34, "b" -> "Jaclyn", "c" -> List(Map("c" -> 34), Map("c" -> 100), Map("c" -> 12))),
//                    Map("a" -> 42, "b" -> "Thomas", "c" -> List(Map("c" -> 50), Map("c" -> 32), Map("c" -> 30)))), BagType(stype))
   
    /**
      * Tests a simple loop through a nested relation
      *
      * For x3 in R Union
      *  For y4 in x3.c Union
      *    sng(( w1 := x3.a, w2 := y4.c ))
      *
      * { v0 | x3 <- R, v0 <- { ( w1 := x3.a, w2 := y4.c ) | y4 <- x3.c } 
      */
     test("Translator.translate.ForeachUnionForeach"){
       val x1 = VarDef("x", stype)
       val y1 = VarDef("y", nstype)
     
       val q3 = ForeachUnion(x1, relationS,
                ForeachUnion(y1, TupleVarRef(x1)("c").asInstanceOf[BagExpr],
                  Singleton(Tuple("w1" -> TupleVarRef(x1)("a"), "w2" -> TupleVarRef(y1)("c")))))
      val tq3 = Translator.translate(q3)

      val v = VarDef("v" + Symbol.getId("v"), TupleType("w1" -> IntType, "w2" -> IntType))
      val cq3 = BagComp(TupleVar(v), List(Generator(x1, BagVar(relationS.varDef)),
                        Generator(v, BagComp(Tup("w1" -> Proj(TupleVar(x1), "a"), "w2" -> Proj(TupleVar(y1), "c")), 
                          List(Generator(y1, Proj(TupleVar(x1), "c").asInstanceOf[BagCalc]))))))

      assert(tq3 === cq3)
      
    }

    /**
      * Tests a simple loop through a nested relation
      *
      * For x3 in R Union
      *  sng( w1 := x3.a, w2 := For y4 in x3.c Union
      *                           sng (( w2 := y4.c ))
      *
      * { ( w1 := x0.a, w2 := { (w2 := y0.c) | y0 <- x0.c ) } ) | x0 <- S } 
      */
     test("Translator.translate.ForeachUnionSingletonForeach"){
       val x1 = VarDef("x", stype)
       val y1 = VarDef("y", nstype)
     
       val q3 = ForeachUnion(x1, relationS,
                  Singleton(Tuple("w1" -> TupleVarRef(x1)("a"), "w2" ->
                    ForeachUnion(y1, TupleVarRef(x1)("c").asInstanceOf[BagExpr],
                      Singleton(Tuple("w2" -> TupleVarRef(y1)("c")))))))
      val tq3 = Translator.translate(q3)

      val v = VarDef(Symbol.fresh("v"), TupleType("w1" -> IntType, "w2" -> IntType))
      val cq3 = BagComp(Tup("w1" -> Proj(TupleVar(x1), "a"), "w2" -> 
                  BagComp(Tup("w2" -> Proj(TupleVar(y1), "c")), 
                    List(Generator(y1, Proj(TupleVar(x1), "c").asInstanceOf[BagCalc])))), 
                      List(Generator(x1, BagVar(relationS.varDef))))
      assert(tq3 === cq3)
      
    }

    /**
      * let x3 := ("a" -> "one") in 
      * foreach y4 in sng(("a" -> x3.a, "b" -> x3.a)) union 
      *    sng(("a" -> y4.a))
      *
      * 
      */
    test("Translator.translate.LetInForeach"){
      val x3 = VarDef("x", TupleType("a" -> StringType))
      val y4 = VarDef("y", TupleType("a" -> StringType, "b" -> StringType))

      val q4 = Let(x3, Tuple("a" -> Const("one", StringType)),
                ForeachUnion(y4, Singleton(Tuple("a" -> TupleVarRef(x3)("a"), "b" -> TupleVarRef(x3)("a"))),
                  Singleton(Tuple("a" -> TupleVarRef(y4)("a")))))
      val tq4 = Translator.translate(q4)

      val v = VarDef("v" + Symbol.getId("v"), TupleType("a" -> StringType))
      val cq4 = BagComp(TupleVar(v), List(Bind(x3, Tup("a" -> Constant("one", StringType))),
                  Generator(v, BagComp(Tup("a" -> Proj(TupleVar(y4), "a")),
                    List(Generator(y4, Sng(Tup("a" -> Proj(TupleVar(x3), "a"), "b" -> Proj(TupleVar(x3), "a")))))))))
      assert(tq4 == cq4)
    }

    /**
      * This tests tuple projection normalization rule
      * let x3 := (a := "one") in x3.a 
      *
      * { v7 | x0 := ( a := "one" ),  v7 <- {( a := x0.a )}  }
      */
    test("Translator.translate.LetInTupleProjection"){
      val x3 = VarDef("x", TupleType("a" -> StringType))
      val q5 = Let(x3, Tuple("a" -> Const("one", StringType)), Singleton(Tuple("a" -> TupleVarRef(x3)("a"))))
      val tq5 = Translator.translate(q5)
      val v = VarDef("v" + Symbol.getId("v"), TupleType("a" -> StringType))
      val cq5 = BagComp(TupleVar(v), List(Bind(x3, Tup("a" -> Constant("one", StringType))),
                  Generator(v, Sng(Tup("a" -> Proj(TupleVar(x3), "a"))))))
      assert(tq5 == cq5)

    }

    /** 
      * Tests substitution in a complex expression
      * 
      * let x3 := ( a := "Jaclyn") in 
      * For x1 in S Union
      *  For y1 in x1.c Union
      *    if ( x3.a = x1.a ) 
      *    then sng(( w1 := x1.a, w2 := y1.c ))
      *
      * { v0 | x3 := (a := "Jaclyn"), v0 <- { v1 | x1 <- S, 
      *                                 v1 <- { ( w1 := x1.a, w2 := y1.c ) | y1 <- x1.c, x3.a = x1.a } }
      */
    test("Translator.translate.LetInForeachPred"){
      val x1 = VarDef("x", stype)
      val y1 = VarDef("y", nstype)
      val x3 = VarDef("x", TupleType("a" -> StringType))
      val q6 = Let(x3, Tuple("a" -> Const("Jaclyn", StringType)),
                ForeachUnion(x1, relationS,
                  ForeachUnion(y1, TupleVarRef(x1)("c").asInstanceOf[BagExpr],
                    IfThenElse(Cond(OpEq, TupleVarRef(x1)("a"), TupleVarRef(x3)("a")),
                             Singleton(Tuple("w1" -> TupleVarRef(x1)("a"), "w2" -> TupleVarRef(y1)("c")))))))

      val tq6 = Translator.translate(q6)
      val v0 = VarDef("v" + Symbol.getId("v"), TupleType("w1" -> IntType, "w2" -> IntType))
      val v1 = VarDef("v" + (Symbol.getId("v") - 1), TupleType("w1" -> IntType, "w2" -> IntType))
      val cq6 = BagComp(TupleVar(v0), List(Bind(x3, Tup("a" -> Constant("Jaclyn", StringType))),
                  Generator(v0, BagComp(TupleVar(v1), List(Generator(x1, BagVar(relationS.varDef)),
                    Generator(v1, BagComp(Tup("w1" -> Proj(TupleVar(x1), "a"), "w2" -> Proj(TupleVar(y1), "c")),
                      List(Generator(y1, Proj(TupleVar(x1), "c").asInstanceOf[BagCalc]),
                        Conditional(OpEq, Proj(TupleVar(x1), "a"), Proj(TupleVar(x3), "a"))))))))))
      assert(tq6 == cq6)
    }

}
