package shredding.nrc

import org.scalatest.FunSuite

class CalcTest extends FunSuite{
   
  def vari(x: String) = x+(VarCnt.currId)

   val nstype = TupleType("c" -> IntType)
   val stype = TupleType("a" -> IntType, "b" -> StringType, "c" -> BagType(nstype))
   val x1 = VarDef("x", stype)
   val y1 = VarDef("y", nstype)
   val relationS = Relation("S", PhysicalBag(stype,
                    Tuple("a" -> Const("42", IntType), "b" -> Const("Milos", StringType), "c" -> PhysicalBag(nstype,
                      Tuple("c" -> Const("42", IntType)), Tuple("c" -> Const("42", IntType)), 
                        Tuple("c" -> Const("30", IntType)))),
                    Tuple("a" -> Const("69", IntType), "b" -> Const("Michael", StringType), "c" -> PhysicalBag(nstype,
                      Tuple("c" -> Const("100", IntType)), Tuple("c" -> Const("69", IntType)), 
                        Tuple("c" -> Const("42", IntType)))),
                    Tuple("a" -> Const("34", IntType), "b" -> Const("Jaclyn", StringType), "c" -> PhysicalBag(nstype,
                      Tuple("c" -> Const("34", IntType)), Tuple("c" -> Const("100", IntType)), 
                        Tuple("c" -> Const("12", IntType)))),
                    Tuple("a" -> Const("42", IntType), "b" -> Const("Thomas", StringType), "c" -> PhysicalBag(nstype,
                      Tuple("c" -> Const("50", IntType)), Tuple("c" -> Const("32", IntType)), 
                        Tuple("c" -> Const("42", IntType))))
                  ))

  test("Calc.Constant"){
    assert(Constant("one", StringType).x == "one")
    assert(Constant("1", IntType).x == "1")
    assert(Constant("1", IntType).normalize == Constant("1", IntType))
  }

  test("Calc.Var"){
    assert(Var(VarDef("x", IntType)) == PrimitiveVar(vari("x"), None, IntType))
    val v1 = Var(VarDef("x", StringType))
    assert(v1 == PrimitiveVar(vari("x"), None, StringType))
    assert(v1.normalize == PrimitiveVar(vari("x"), None, StringType))
    
    val v2 = Var(VarDef("x", TupleType("a" -> IntType, "b" -> StringType))) 
    assert(v2 == TupleVar(vari("x"), None, TupleType("a" -> IntType, "b" -> StringType)))
    assert(v2.normalize == TupleVar(vari("x"), None, TupleType("a" -> IntType, "b" -> StringType)))
    
    val v3 = Var(VarDef("x", BagType(TupleType("a" -> IntType, "b" -> StringType))))
    assert(v3 == BagVar(vari("x"), None, BagType(TupleType("a" -> IntType, "b" -> StringType))))
    assert(v3.normalize == BagVar(vari("x"), None, BagType(TupleType("a" -> IntType, "b" -> StringType))))
  }

  test("Calc.Generator"){
    
    val gen = Generator(VarDef("x", TupleType("a" -> StringType)), Sng(Tup(Map("a" -> Constant("one", StringType)))))
    assert(gen.tp == BagType(TupleType("a" -> StringType)))
    assertThrows[AssertionError](
      Generator(VarDef("x", TupleType("a" -> IntType)), Sng(Tup(Map("a" -> Constant("one", StringType))))))

  }

  test("Calc.Bind"){
    val vb1 = VarDef("x", TupleType("a"-> StringType))
    val bnd1 = Bind(vb1, Tup(Map("a" -> Constant("one", StringType))))
    assert(bnd1 == BindTuple(vb1, Tup(Map("a" -> Constant("one", StringType)))))
    assert(bnd1.tp == TupleType("a"-> StringType))
    assert(bnd1.normalize == BindTuple(vb1, Tup(Map("a" -> Constant("one", StringType)))))
    assertThrows[AssertionError](
      BindTuple(VarDef("x", IntType), Tup(Map("a" -> Constant("one", StringType))))
    )

    val vb2 = VarDef("x", IntType)
    val bnd2 = Bind(vb2, Constant("1", IntType))
    assert(bnd2 == BindPrimitive(vb2, Constant("1", IntType)))
    assert(bnd2.tp == IntType)
    assert(bnd2.normalize == BindPrimitive(vb2, Constant("1", IntType)))
    assertThrows[AssertionError](
      BindPrimitive(VarDef("x", TupleType("a" -> StringType)), Constant("1", IntType))
    )
  }

  test("Calc.Conditional"){
    val c1 = Conditional(OpEq, Constant("one", StringType), Constant("one", StringType))
    assert(c1.tp == BoolType)

    // are we supporting bag equality?
    val c2 = Conditional(OpEq, Sng(Tup("a" -> Constant("one", StringType))), Sng(Tup("b" -> Constant("1", IntType))))
    assert(c2.tp == BoolType)

    val c3 = NotCondition(c1)
    assert(c3.tp == BoolType)
    assertThrows[AssertionError](NotCondition(Constant("one", StringType)))

    val c4 = AndCondition(c1, c2)
    assert(c4.tp == BoolType)
    assertThrows[AssertionError](AndCondition(Constant("one", StringType), c2))
    assertThrows[AssertionError](AndCondition(c2, Constant("one", StringType)))

    val c5 = OrCondition(c1, c2)
    assert(c5.tp == BoolType)
    assertThrows[AssertionError](OrCondition(Constant("one", StringType), c2))
    assertThrows[AssertionError](OrCondition(c2, Constant("one", StringType)))

  }

  test("Calc.Sng"){

    val zero = Zero()
    assert(zero.normalize == zero)

    val esng = Sng(Tup(Map[String, AttributeCalc]()))
    assert(esng.tp == zero.tp)
    assert(esng.normalize == Zero())

    val sng = Sng(Tup("a" -> Constant("one", StringType)))
    assert(sng.tp == BagType(TupleType("a" -> StringType)))
    assert(sng.normalize == sng)
    
  }
  
  test("Calc.BagComp"){
   
    // basic generator comprehension
    // { x | x <- S } normalized to itself 
    val bc1 = BagComp(Tup("w" -> Var(Var(x1), "b")), List(Generator(x1, InputR("R", relationS.b))))
    assert(bc1.tp == BagType(TupleType("w" -> StringType)))
    assert(bc1.normalize == bc1)
    
    val x = VarDef("x", TupleType("a" -> StringType))
    val y = VarDef("y", stype)
    val z = VarDef("z", TupleType())
    val gen1 = Generator(x, Sng(Tup(Map("a" -> Constant("one", StringType)))))
    val gen2 = Generator(y, InputR(relationS.n, relationS.b))
    val gen3 = Generator(z, Zero())
    val pred1 = Conditional(OpEq, Var(Var(x), "a"), Constant("one", StringType))
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
    assert(cq6.normalize == BagComp(Tup("w" -> Var(Var(x2), "a")), 
                              List(bind1, Bind(x2, Tup("a" -> Var(Var(x),"a")))))) 

    // N8, a more complex expression and preserves qualifiers
    // { ( w := x13.a ) |  x13 <- { ( a := y11.b ) |  y11 <- R  } ,  x13.a = "one"  }
    // { ( w := x13.a ) |  y11 <- R , x13 := ( a := y11.b ),  x13.a = "one"  }
    val gen5 = Generator(x2, BagComp(Tup("a" -> Var(Var(y), "b")), List(gen2)))
    val pred2 = Conditional(OpEq, Var(Var(x2), "a"), Constant("one", StringType))
    val cq7 = BagComp(Tup("w"-> Var(Var(x2), "a")), List(gen5, pred2))
    assert(cq7.normalize == BagComp(Tup("w"-> Var(Var(x2), "a")), 
                              List(gen2, Bind(x2, Tup("a" -> Var(Var(y), "b"))), pred2)))

    // N8, an even more complex expression and preserves qualifiers inside and out
    // { ( w := x13.a ) |  x13 <- { ( a := y11.b ) |  y11 <- R  } ,  x13.a = "one"  }
    // { ( w := x13.a ) |  y11 <- R , x13 := ( a := y11.b ),  x13.a = "one"  }
    val pred3 = Conditional(OpGt, Var(Var(y), "a"), Constant("15", IntType))
    val gen6 = Generator(x2, BagComp(Tup("a" -> Var(Var(y), "b")), List(gen2, pred3)))
    val cq8 = BagComp(Tup("w"-> Var(Var(x2), "a")), List(gen6, pred2))
    assert(cq8.normalize == BagComp(Tup("w"-> Var(Var(x2), "a")), 
                              List(gen2, pred3, Bind(x2, Tup("a" -> Var(Var(y), "b"))), pred2)))

  }

  test("Calc.Merge"){ 

    val m1 = Merge(Sng(Tup("a" -> Constant("one", StringType))), Sng(Tup("a" -> Constant("two", StringType))))
    assert(m1.tp == BagType(TupleType("a" -> StringType)))
    assert(m1.tp == Sng(Tup("a" -> Constant("one", StringType))).tp) 
    assert(m1.normalize == m1) 

    // N7, generator with a merge
    val x2 = VarDef("x",  TupleType("a" -> StringType))
    val m2 = BagComp(Tup("w" -> Var(Var(x2), "a")), List(Generator(x2, m1)))
    assert(m2.normalize == Merge(BagComp(Tup("w" -> Var(Var(x2), "a")), 
                                  List(Bind(x2, Tup("a" -> Constant("one", StringType))))),
                                  BagComp(Tup("w" -> Var(Var(x2), "a")),
                                  List(Bind(x2, Tup("a" -> Constant("two", StringType)))))))
  }

  test("Calc.IfStmt"){
    val ifs = IfStmt(Conditional(OpEq, Constant("one", StringType), Constant("two", StringType)),
                     Sng(Tup("w" -> Constant("one", StringType))), Option(Sng(Tup("w" -> Constant("two", StringType)))))
    assert(ifs.tp == BagType(TupleType("w" -> StringType)))

    val ifs2 = IfStmt(Conditional(OpEq, Constant("one", StringType), Constant("two", StringType)),
                     Sng(Tup("w" -> Constant("one", StringType))), None)
    assert(ifs.tp == BagType(TupleType("w" -> StringType)))

    // { ( w := x.w ) | x <- if ( "one" = "two" ) then sng((w := "one")) else sng((w := "two")) }
    // { ( w := x.w ) | ( "one" = "two" ), x := ( w := "one") } U { ( w := x.w ) | not( "one" = "two" ), x := ( w := "two" ) }
    val x2 = VarDef("x", TupleType("w" -> StringType))
    val ifs3 = BagComp(Tup("w" -> Var(Var(x2), "w")), List(Generator(x2, ifs)))
    assert(ifs3.tp == ifs.tp)
    assert(ifs3.normalize == Merge(BagComp(Tup("w" -> Var(Var(x2), "w")), List(
                                    Conditional(OpEq, Constant("one", StringType), Constant("two", StringType)),
                                    Bind(x2, Tup("w" -> Constant("one", StringType))))),
                                   BagComp(Tup("w" -> Var(Var(x2), "w")), List(
                                    NotCondition(Conditional(OpEq, Constant("one", StringType), Constant("two", StringType))),
                                    Bind(x2, Tup("w" -> Constant("two", StringType)))))))    

  }

  test("Calc.Tup"){
    
    val t1 = Tup("a" -> Constant("one", StringType), "b" -> Constant("two", StringType))
    assert(t1.tp == TupleType("a" -> StringType, "b" -> StringType))
    assert(t1.normalize == t1)

    val t2 = Tup("a" -> Constant("one", StringType), "b" -> Sng(Tup("b" -> Constant("two", StringType))))
    assert(t2.tp == TupleType("a" -> StringType, "b" -> BagType(TupleType("b" -> StringType))))
    assert(t2.normalize == t2)
    
  }

  test("Calc.InputR"){
    val i1 = InputR(relationS.n, relationS.b)
    assert(i1.tp == relationS.tp)
  }

}
