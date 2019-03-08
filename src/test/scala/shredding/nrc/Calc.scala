package shredding.nrc

import org.scalatest.FunSuite

class CalcTest extends FunSuite{
   
   val itemTp = TupleType("a" -> IntType, "b" -> StringType)
   val relationR = Relation("R", PhysicalBag(itemTp,
                    Tuple("a" -> Const("42", IntType), "b" -> Const("Milos", StringType)),
                    Tuple("a" -> Const("69", IntType), "b" -> Const("Michael", StringType)),
                    Tuple("a" -> Const("34", IntType), "b" -> Const("Jaclyn", StringType)),
                    Tuple("a" -> Const("42", IntType), "b" -> Const("Thomas", StringType))
                  ))

  test("Calc.Constant"){
    assert(Constant("one", StringType).x == "one")
    assert(Constant("1", IntType).x == "1")
  }

  test("Calc.Var"){

    assert(Var(VarDef("x", IntType)) == PrimitiveVar("x5", None, IntType))
    assert(Var(VarDef("x", StringType)) == PrimitiveVar("x6", None, StringType))
   
    assert(Var(VarDef("x", TupleType("a" -> IntType, "b" -> StringType))) == 
            TupleVar("x7", None, TupleType("a" -> IntType, "b" -> StringType)))
    
    assert(Var(VarDef("x", BagType(TupleType("a" -> IntType, "b" -> StringType)))) == 
            BagVar("x8", None, BagType(TupleType("a" -> IntType, "b" -> StringType))))
  }

  test("Calc.Generator"){
    
    val gen = Generator(VarDef("x", TupleType("a" -> StringType)), Sng(Tup(Map("a" -> Constant("one", StringType)))))
    
    // assert types properly set
    assert(gen.tp == BagType(TupleType("a" -> StringType)))
    
    // variable assigned must have tuple type 
    assertThrows[AssertionError](
      Generator(VarDef("x", TupleType("a" -> IntType)), Sng(Tup(Map("a" -> Constant("one", StringType))))))
    
    // test printing
    assert(Printer.quote(gen).replace(" ", "") == "x9<-{(a:=\"one\")}")
  }

  test("Calc.Bind"){
    val bnd = Bind(VarDef("x", TupleType("a"-> StringType)), Tup(Map("a" -> Constant("one", StringType))))
    assert(bnd.tp == TupleType("a"-> StringType))
  }

  /**
    * Tests the Calc => Calc normalization 
    *
    */
  test("Calc.normalize"){
    val x = VarDef("x", TupleType("a" -> StringType))
    val y = VarDef("y", itemTp)
    val z = VarDef("z", TupleType())
    val gen1 = Generator(x, Sng(Tup(Map("a" -> Constant("one", StringType)))))
    val gen2 = Generator(y, InputR(relationR.n, relationR.b))
    val gen3 = Generator(z, Zero())
    val pred1 = Pred(Conditional(OpEq, Var(Var(x), "a"), Constant("one", StringType)))
    val bind1 = Bind(x, Tup(Map("a" -> Constant("one", StringType))))
   
    // N6
    // { ( w := x.a ) | x <- { ( a := "one" ) } }
    // { ( w := x.a ) | x := ( a: = "one" ) } } 
    val cq1 = BagComp(Tup("w" -> Var(Var(x), "a")), List(gen1))
    assert(cq1.normalize == BagComp(Tup("w" -> Var(Var(x), "a")), List(bind1)))

    // N6, preserves predicate
    // { ( w := x.a ) | x <- { ( a := "one" ) }, x.a = "one" }
    // { ( w := x.a ) | x := ( a: = "one" ), x.a = "one" }
    val cq2 = BagComp(Tup("w" -> Var(Var(x), "a")), List(gen1, pred1))
    assert(cq2.normalize == BagComp(Tup("w" -> Var(Var(x), "a")), List(bind1, pred1)))

    // N6, preserves qualifiers before and after
    // { ( w := x.a ) | y <- R, x <- { ( a := "one" ) }, x.a = "one" }
    // { ( w := x.a ) | y <- R, x := ( a: = "one" ), x.a = "one" }
    val cq3 = BagComp(Tup("w" -> Var(Var(x), "a")), List(gen2, gen1, pred1))
    assert(cq3.normalize == BagComp(Tup("w" -> Var(Var(x), "a")), List(gen2, bind1, pred1)))

    // N5
    // { ( w := x.a ) | x <- { } } 
    // { }
    val cq4 = BagComp(Tup("w" -> Var(Var(x), "a")), List(gen3))
    assert(cq4.normalize == Zero())

    // N5, zero regardless of qualifiers in the comprehension
    val cq5 = BagComp(Tup("w" -> Var(Var(x), "a")), List(gen1, gen2, gen3, pred1))
    assert(cq5.normalize == Zero())

    val x2 = VarDef("x",  TupleType("a" -> StringType))
    val gen4 = Generator(x2, BagComp(Tup("a" -> Var(Var(x),"a")), List(gen1)))
    // N8
    // { ( w := x13.a ) |  x13 <- { ( a := x10.a ) |  x10 <- {( a := "one" )}  }  }
    // { ( w := x13.a ) | x10 := ( a := "one" ), x13 := ( a := x10.a ) }
    val cq6 = BagComp(Tup("w" -> Var(Var(x2), "a")), List(gen4))
    assert(cq6.normalize == BagComp(Tup("w" -> Var(Var(x2), "a")), List(bind1, Bind(x2, Tup("a" -> Var(Var(x),"a")))))) 

    // N8, a more complex expression and preserves qualifiers
    // { ( w := x13.a ) |  x13 <- { ( a := y11.b ) |  y11 <- R  } ,  x13.a = "one"  }
    // { ( w := x13.a ) |  y11 <- R , x13 := ( a := y11.b ),  x13.a = "one"  }
    val gen5 = Generator(x2, BagComp(Tup("a" -> Var(Var(y), "b")), List(gen2)))
    val pred2 = Pred(Conditional(OpEq, Var(Var(x2), "a"), Constant("one", StringType)))
    val cq7 = BagComp(Tup("w"-> Var(Var(x2), "a")), List(gen5, pred2))
    println(Printer.quote(BagComp(Tup("w"-> Var(Var(x2), "a")), List(gen2, Bind(x2, Tup("a" -> Var(Var(y), "b"))), pred2))))
    assert(cq7.normalize == BagComp(Tup("w"-> Var(Var(x2), "a")), List(gen2, Bind(x2, Tup("a" -> Var(Var(y), "b"))), pred2)))
     
  }

}
