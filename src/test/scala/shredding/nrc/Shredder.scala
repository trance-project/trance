package shredding.nrc

import org.scalatest.FunSuite

class ShredderTest extends FunSuite with NRCTransforms{
   
   val c = Relation[TTuple2[String,Double]]('C,
        List[TTuple2[String,Double]](("one", 1.0), ("two", 1.0), ("three", 0.0), ("four", 0.0)))
   
   val v = Relation[TTuple3[String, Int, TBag[TTuple2[String, Int]]]]('V,
        List[TTuple3[String, Int, TBag[TTuple2[String, Int]]]](("1", 100, 
          List(("one", 0), ("two", 1), ("three", 2), ("four", 0))),
          ("1", 101, List(("one", 1), ("two", 2), ("three", 2), ("four", 0)))))

   val q1 = Singleton(TupleStruct1(Const(1)))

  test("Shredder.isShreddable") {
    assert(Shredder.isShreddable(EmptySet()) === true)
  }

  test("Shredder.shredRelation") {
    assert(Shredder.shredRelation(c).b == c.b)
    assert(Shredder.shredRelation(v).b != v.b)
  }

  test("Shredder.shredQueryBag") {
    var sq1 = Shredder.shredQueryBag(q1, Sym('D))
    // todo
  }
}
