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
    val relationR = BagVarRef(VarDef("R", itemTp))
    val xdef = VarDef("x", itemTp)
    Let(xdef, Tuple("a" -> Const(42, IntType), "b" -> Const("Test", StringType)),
                TupleVarRef(xdef)("a"))
  }

  val query1a = {
    import nrc._
    val relationR = BagVarRef(VarDef("R", itemTp))
    val xdef = VarDef("x", itemTp)
    val rdef = VarDef("r", BagType(itemTp))
    Let(rdef, relationR,
                ForeachUnion(xdef, BagVarRef(rdef), Singleton(Tuple("o1" -> TupleVarRef(xdef)("b")))))
  }

  // N4
  val query2 = {
    import nrc._
    val relationR = BagVarRef(VarDef("R", itemTp))
    val xdef = VarDef("x", itemTp)
    val ydef = VarDef("y", itemTp)
    // for y in R union
    //   for x in [ if (y.a > 40) then sng(y) else sng(y.a, "null) ]
    //     if (x.a > 40) then sng(o1 := y.b)
    // complex example
    ForeachUnion(ydef, relationR,
                ForeachUnion(xdef, IfThenElse(Cmp(OpGe, TupleVarRef(ydef)("a"), Const(40, IntType)),
                                    Singleton(TupleVarRef(ydef)), Singleton(Tuple("a" -> TupleVarRef(ydef)("a"),
                                        "b" -> Const("null", StringType)))).asInstanceOf[BagExpr],
                  IfThenElse(Cmp(OpGt, TupleVarRef(xdef)("a"), Const(41, IntType)),
                Singleton(Tuple("o1" -> TupleVarRef(xdef)("b"))))))
  }

  val query3 = {
    import nrc._
    val relationR = BagVarRef(VarDef("R", itemTp))
    val xdef = VarDef("x", itemTp)
    val ydef = VarDef("y", itemTp)
    ForeachUnion(xdef, relationR,
              ForeachUnion(ydef, relationR,
                Singleton(Tuple("o1" -> TupleVarRef(xdef)("a"), "o2" -> TupleVarRef(ydef)("a")))))
  }

}
