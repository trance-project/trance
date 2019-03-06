package shredding.nrc

import org.scalatest.FunSuite

class TranslatorTest extends FunSuite{
   
   val itemTp = TupleType("a" -> IntType, "b" -> StringType)
   val x = VarDef("x", itemTp)
   val x2 = VarDef("y2", itemTp)
   val relationR = Relation("R", PhysicalBag(itemTp,
                    Tuple("a" -> Const("42", IntType), "b" -> Const("Milos", StringType)),
                    Tuple("a" -> Const("69", IntType), "b" -> Const("Michael", StringType)),
                    Tuple("a" -> Const("34", IntType), "b" -> Const("Jaclyn", StringType)),
                    Tuple("a" -> Const("42", IntType), "b" -> Const("Thomas", StringType))
                  ))

   val q1 = ForeachUnion(x, relationR, Singleton(Tuple("w" -> VarRef(x, "b"))))
   val cq1 = BagComp(Tup("w" -> Var(Var(x), "b")), List(Generator(x, InputR("R", relationR.b))))

   test("Translator.translate") {
      assert(Translator.translate(q1) === cq1)
   }

}
