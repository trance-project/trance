package shredding.examples.normalize

import shredding.core._
import shredding.nrc.MaterializeNRC

object NormalizationTests {
  
  val nrc = new MaterializeNRC{}
  val itemTp = TupleType("a" -> IntType, "b" -> StringType)

  // N3
  // Let x = (a = 42, b = "Test") In x.a
  val query1 = {
    import nrc._
    val relationR = BagVarRef("R", BagType(itemTp))
    val xref = TupleVarRef("x", itemTp)
    Let(xref, Tuple("a" -> Const(42, IntType), "b" -> Const("Test", StringType)),
      xref("a"))
  }

  val query1a = {
    import nrc._
    val relationR = BagVarRef("R", BagType(itemTp))
    val xref = TupleVarRef("x", itemTp)
    val rref = BagVarRef("r", BagType(itemTp))
    Let(rref, relationR,
      ForeachUnion(xref, rref, Singleton(Tuple("o1" -> xref("b")))))
  }

  // N4
  val query2 = {
    import nrc._
    val relationR = BagVarRef("R", BagType(itemTp))
    val xref = TupleVarRef("x", itemTp)
    val yref = TupleVarRef("y", itemTp)
    // for y in R union
    //   for x in [ if (y.a > 40) then sng(y) else sng(y.a, "null) ]
    //     if (x.a > 40) then sng(o1 := y.b)
    // complex example
    ForeachUnion(yref, relationR,
                ForeachUnion(xref, IfThenElse(Cmp(OpGe, yref("a"), Const(40, IntType)),
                                    Singleton(yref), Singleton(Tuple("a" -> yref("a"),
                                        "b" -> Const("null", StringType)))).asInstanceOf[BagExpr],
                  IfThenElse(Cmp(OpGt, xref("a"), Const(41, IntType)),
                Singleton(Tuple("o1" -> xref("b"))))))
  }

  val query3 = {
    import nrc._
    val relationR = BagVarRef("R", BagType(itemTp))
    val xref = TupleVarRef("x", itemTp)
    val yref = TupleVarRef("y", itemTp)
    ForeachUnion(xref, relationR,
              ForeachUnion(yref, relationR,
                Singleton(Tuple("o1" -> xref("a"), "o2" -> yref("a")))))
  }

}
