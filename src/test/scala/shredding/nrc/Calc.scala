package shredding.nrc

import org.scalatest.FunSuite

class CalcTest extends FunSuite{
   
  test("Calc.Constant"){
    assert(Constant("one", StringType).x == "one")
    assert(Constant("1", IntType).x == "1")
  }

  test("Calc.Var"){

    assert(Var(VarDef("x", IntType)) == PrimitiveVar("x3", None, IntType))
    assert(Var(VarDef("x", StringType)) == PrimitiveVar("x4", None, StringType))
   
    assert(Var(VarDef("x", TupleType("a" -> IntType, "b" -> StringType))) == 
            TupleVar("x5", None, TupleType("a" -> IntType, "b" -> StringType)))
    
    assert(Var(VarDef("x", BagType(TupleType("a" -> IntType, "b" -> StringType)))) == 
            BagVar("x6", None, BagType(TupleType("a" -> IntType, "b" -> StringType))))
  }

  test("Calc.Generator"){
    
    val gen = Generator(VarDef("x", TupleType("a" -> StringType)), Sng(Tup(Map("a" -> Constant("one", StringType)))))
    
    // assert types properly set
    assert(gen.tp == BagType(TupleType("a" -> StringType)))
    
    // variable assigned must have tuple type 
    assertThrows[AssertionError](
      Generator(VarDef("x", TupleType("a" -> IntType)), Sng(Tup(Map("a" -> Constant("one", StringType))))))
    
    // test printing
    assert(Printer.quote(gen).replace(" ", "") == "x7<-{(a:=\"one\")}")
  }

  test("Calc.Bind"){
    val bnd = Bind(VarDef("x", TupleType("a"-> StringType)), Tup(Map("a" -> Constant("one", StringType))))
    assert(bnd.tp == TupleType("a"-> StringType))
  }

}
