package shredding.nrc

import org.scalatest.FunSuite

class TranslatorTest extends FunSuite{
   
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
     * Tests the translation of a basic input query 
     * to comprehension calculus, and validates 
     * this is equivalent to normalization.
     */
   test("Translator.translate.1") {
    // For x8 in R Union
    //  sng(( w := x8.b )) 
    val q1 = ForeachUnion(x, relationR, Singleton(Tuple("w" -> VarRef(x, "b"))))
    
    // { ( w := x8.b ) |  x8 <- R  }
    val cq1 = BagComp(Tup("w" -> Var(Var(x), "b")), List(Generator(x, InputR("R", relationR.b))))

    assert(Translator.translate(q1) === cq1)
    // validate normalization
    assert(Translator.translate(q1) === cq1.normalize)
   }

   test("Translator.translate.2"){
    // For x1 in R Union
    //  For y22 in R Union
    //    If ((x1.b = y22.b))
    //    Then sng(( w1 := x1.a ))
    val q10 = ForeachUnion(x, relationR,
                ForeachUnion(x2, relationR,
                  IfThenElse(List(Cond(OpEq, VarRef(x, "b"), VarRef(x2, "b"))), 
                             Singleton(Tuple("w1" -> VarRef(x, "a"))))))
    
    // { ( w1 := x1.a ) |  x1 <- R ,  y22 <- R ,  x1.b = y22.b  }
    val cq10 = BagComp(Tup("w1" -> Var(Var(x), "a")), List(Generator(x, InputR("R", relationR.b)), 
                Generator(x2, InputR("R", relationR.b)), Pred(Conditional(OpEq, 
                  Var(Var(x), "b"), Var(Var(x2), "b")))))
    
    assert(Translator.translate(q10) == cq10)
    assert(Translator.translate(q10) == cq10.normalize)

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
      */
    test("Translator.translate.3"){
      
      // For x3 in R Union
      //  For y4 in x3.c Union
      //    sng(( w1 := x3.a, w2 := y4.c ))
      val q3 = ForeachUnion(x1, relationS,
                ForeachUnion(y1, VarRef(x1, "c").asInstanceOf[BagExpr],
                  Singleton(Tuple("w1" -> VarRef(x1, "a"), "w2" -> VarRef(y1, "c")))))
      
      // { ( w1 := x3.a, w2 := y4.c ) |  x3 <- R ,  y4 <- x3.c  }
      println(x1.tp)
      println(relationS.tp)
      println(y1.tp)
      println(relationS.tp)
      val cq3 = BagComp(Tup("w1" -> Var(Var(x1), "a"), "w2" -> Var(Var(y1), "c")), 
                  List(Generator(x1, InputR("S", relationS.b)), 
                        Generator(y1, Var(Var(x1), "c").asInstanceOf[BagCalc])))

      assert(Translator.translate(q3) === cq3)
      assert(Translator.translate(q3) === cq3.normalize)
      
    }
  

}
