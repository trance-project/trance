package shredding.nrc

import org.scalatest.FunSuite

class TranslatorTest extends FunSuite{
   
 
   def vari(x: String) = x+(VarCnt.currId+1)

   /**
     * Flat relation tests
     */
   val itemTp = TupleType("a" -> IntType, "b" -> StringType)
   val x = VarDef("x", itemTp)
   val x2 = VarDef("y2", itemTp)
   // flat relation
   val relationR = Relation("R", PhysicalBag(itemTp,
                    Tuple("a" -> Const("42", IntType), "b" -> Const("Milos", StringType)),
                    Tuple("a" -> Const("69", IntType), "b" -> Const("Michael", StringType)),
                    Tuple("a" -> Const("34", IntType), "b" -> Const("Jaclyn", StringType)),
                    Tuple("a" -> Const("42", IntType), "b" -> Const("Thomas", StringType))
                  ))

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

    val q1 = ForeachUnion(x, relationR, Singleton(Tuple("w" -> VarRef(x, "b"))))
    
    val cq1 = BagComp(Tup("w" -> Var(Var(x), "b")), List(Generator(x, InputR("R", relationR.b))))

    assert(Translator.translate(q1) === cq1)

   }

   /**
     * For x1 in R Union
     *  For y22 in R Union
     *    If ((x1.b = y22.b))
     *    Then sng(( w1 := x1.a ))
     *
     * { ( w1 := x1.a ) |  x1 <- R ,  y22 <- R ,  x1.b = y22.b  }
     */
   test("Translator.translate.ForeachUnionPred"){
    
    val q10 = ForeachUnion(x, relationR,
                ForeachUnion(x2, relationR,
                  IfThenElse(Cond(OpEq, VarRef(x, "b"), VarRef(x2, "b")), 
                             Singleton(Tuple("w1" -> VarRef(x, "a"))))))
    
    val cq10 = BagComp(Tup("w1" -> Var(Var(x), "a")), List(Generator(x, InputR("R", relationR.b)), 
                Generator(x2, InputR("R", relationR.b)), Conditional(OpEq, 
                  Var(Var(x), "b"), Var(Var(x2), "b"))))
    
    assert(Translator.translate(q10) == cq10)

   }

   /**
     *
     * For x1 in R Union
     *  For y22 in R Union
     *    If ((x1.b = y22.b))
     *    Then sng(( w1 := x1.a ))
     *    Else sng(( w1 := -1 ))
     *
     * { ( w1 := x1.a ) |  x1 <- R ,  y22 <- R ,  x1.b = y22.b  } U
     * { ( w1 := -1 ) | x1 <- R, y22 <- R, x1.b != y22.b }
     */
   test("Translator.translate.ForeachUnionIfStmtMerge"){
    val q10 = ForeachUnion(x, relationR,
                ForeachUnion(x2, relationR,
                  IfThenElse(Cond(OpEq, VarRef(x, "b"), VarRef(x2, "b")), 
                             Singleton(Tuple("w1" -> VarRef(x, "a"))),
                             Option(Singleton(Tuple("w1" -> Const("-1", IntType)))))))
    
    val cq10a = BagComp(Tup("w1" -> Var(Var(x), "a")), List(Generator(x, InputR("R", relationR.b)), 
                Generator(x2, InputR("R", relationR.b)), Conditional(OpEq, 
                  Var(Var(x), "b"), Var(Var(x2), "b"))))
    val cq10b = BagComp(Tup("w1" -> Constant("-1", IntType)), List(Generator(x, InputR("R", relationR.b)), 
                Generator(x2, InputR("R", relationR.b)), NotCondition(Conditional(OpEq, 
                  Var(Var(x), "b"), Var(Var(x2), "b")))))

    assert(Translator.translate(q10) == Merge(cq10a, cq10b))
   }


   /**
     * Nested relation tests 
     */
   val nstype = TupleType("c" -> IntType)
   val stype = TupleType("a" -> IntType, "b" -> StringType, "c" -> BagType(nstype))
   val x1 = VarDef("x", stype)
   val y1 = VarDef("y", nstype)
   val relationS = Relation("S", PhysicalBag(stype,
                    Tuple("a" -> Const("42", IntType), "b" -> Const("Milos", StringType), "c" -> PhysicalBag(nstype,
                      Tuple("c" -> Const("42", IntType)), Tuple("c" -> Const("42", IntType)), Tuple("c" -> Const("30", IntType)))),
                    Tuple("a" -> Const("69", IntType), "b" -> Const("Michael", StringType), "c" -> PhysicalBag(nstype,
                      Tuple("c" -> Const("100", IntType)), Tuple("c" -> Const("69", IntType)), Tuple("c" -> Const("42", IntType)))),
                    Tuple("a" -> Const("34", IntType), "b" -> Const("Jaclyn", StringType), "c" -> PhysicalBag(nstype,
                      Tuple("c" -> Const("34", IntType)), Tuple("c" -> Const("100", IntType)), Tuple("c" -> Const("12", IntType)))),
                    Tuple("a" -> Const("42", IntType), "b" -> Const("Thomas", StringType), "c" -> PhysicalBag(nstype,
                      Tuple("c" -> Const("50", IntType)), Tuple("c" -> Const("32", IntType)), Tuple("c" -> Const("42", IntType))))
                  ))
    
    /**
      * Tests a simple loop through a nested relation
      *
      * For x3 in R Union
      *  For y4 in x3.c Union
      *    sng(( w1 := x3.a, w2 := y4.c ))
      *
      * { ( w1 := x3.a, w2 := y4.c ) |  x3 <- R ,  y4 <- x3.c  }
      */
    test("Translator.translate.ForeachUnionForeach"){
      
      val q3 = ForeachUnion(x1, relationS,
                ForeachUnion(y1, VarRef(x1, "c").asInstanceOf[BagExpr],
                  Singleton(Tuple("w1" -> VarRef(x1, "a"), "w2" -> VarRef(y1, "c")))))
      
      val cq3 = BagComp(Tup("w1" -> Var(Var(x1), "a"), "w2" -> Var(Var(y1), "c")), 
                  List(Generator(x1, InputR("S", relationS.b)), 
                        Generator(y1, Var(Var(x1), "c").asInstanceOf[BagCalc])))

      assert(Translator.translate(q3) === cq3)
      
    }
  
    /**
      * let x3 := ("a" -> "one") in 
      * foreach y4 in sng(("a" -> x3.a, "b" -> x3.a)) union 
      *    sng(("a" -> y4.a))
      *
      * before normalization:
      * { ( a : = y4.a ) | x3 := (a := "one"), y4 <- { ( a := x3.a, b := x3.a ) } }
      *
      * after normalization:
      * { ( a := y4.a ) | x3 ::= (a := "one"), y4 := ( a := "one", b := "one") }
      * 
      */
    test("Translator.translate.LetInForeach"){
      val x3 = VarDef("x", TupleType("a" -> StringType))
      val y4 = VarDef("y", TupleType("a" -> StringType, "b" -> StringType))
      
      val q4 = Let(x3, Tuple("a" -> Const("one", StringType)),
                ForeachUnion(y4, Singleton(Tuple("a" -> VarRef(x3, "a"), "b" -> VarRef(x3, "a"))),
                  Singleton(Tuple("a" -> VarRef(y4, "a")))))
      
      Translator.normalize = false
      assert(Translator.translate(q4) == BagComp(Tup("a" -> Var(Var(y4), "a")), List(
                                          Bind(x3, Tup("a" -> Constant("one", StringType))),
                                          Generator(y4, Sng(Tup("a" -> Constant("one", StringType), "b" -> Constant("one", StringType)))))))
      Translator.normalize = true
      assert(Translator.translate(q4) == BagComp(Tup("a" -> Var(Var(y4), "a")), List(
                                          Bind(x3, Tup("a" -> Constant("one", StringType))),
                                          Bind(y4, Tup("a" -> Constant("one", StringType), "b" -> Constant("one", StringType))))))
    }

    /**
      * This tests tuple projection normalization rule
      * let x3 := (a := "one") in x3.a 
      *
      * v := "one"
      */
    test("Translator.translate.LetInTupleProjection"){
      val x3 = VarDef("x", TupleType("a" -> StringType))
      val v = vari("v")
      val q5 = Let(x3, Tuple("a" -> Const("one", StringType)), VarRef(x3, "a"))
      assert(Translator.translate(q5) == Bind(VarDef("v", StringType, v.replace("v", "").toInt), Constant("one", StringType))) 
      assert(Translator.translate(q5).tp == StringType)

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
      * { ( w1 := x1.a, w2 := y1.c ) | x1 <- S, y1 <- x1.c, "Jaclyn" = x1.a }
      */
    test("Translator.translate.LetInForeachPred"){
      val x3 = VarDef("x", TupleType("a" -> StringType))
      val q6 = Let(x3, Tuple("a" -> Const("Jaclyn", StringType)), 
                ForeachUnion(x1, relationS,
                  ForeachUnion(y1, VarRef(x1, "c").asInstanceOf[BagExpr],
                    IfThenElse(Cond(OpEq, VarRef(x1, "a"), VarRef(x3, "a")), 
                             Singleton(Tuple("w1" -> VarRef(x1, "a"), "w2" -> VarRef(y1, "c")))))))
      
      assert(Translator.translate(q6) == BagComp(Tup("w1" -> Var(Var(x1), "a"), "w2" -> Var(Var(y1), "c")),
                                          List(Bind(x3, Tup("a" -> Constant("Jaclyn", StringType))),
                                            Generator(x1, InputR(relationS.n, relationS.b)), 
                                            Generator(y1, Var(Var(x1), "c").asInstanceOf[BagCalc]),
                                            Conditional(OpEq, Var(Var(x1), "a"), Constant("Jaclyn", StringType)))))
    }
}
