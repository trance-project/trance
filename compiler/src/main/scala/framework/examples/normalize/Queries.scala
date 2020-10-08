package framework.examples.normalize

import framework.common._
import framework.nrc.MaterializeNRC

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

  // N5
  val query5 = {
    import nrc._
    val relationR = BagVarRef("R", BagType(itemTp))
    val xref = TupleVarRef("x", itemTp)
    val ms = Map.empty[String, TupleAttributeExpr]
    ForeachUnion(xref, relationR, Singleton(Tuple(ms)))
  }

  val query6 = {
    import nrc._
    val relationR = BagVarRef("R", BagType(itemTp))
    val xref = TupleVarRef("x", itemTp)
    val yref = TupleVarRef("y", itemTp)
    ForeachUnion(xref, relationR, 
      ForeachUnion(yref, Singleton(Tuple("a" -> Const(1, IntType), "b" -> Const("2", StringType))), 
        IfThenElse(Cmp(OpEq, xref("a"), yref("a")), 
          Singleton(Tuple("c" -> xref("b"), "d" -> yref("b"))))))
  }

  // build example for query 8
  val query8 = {
    import nrc._
    val relationR = BagVarRef("R", BagType(itemTp))
    val relationS = BagVarRef("S", BagType(itemTp))
    val relationT = BagVarRef("T", BagType(itemTp))
    val wref = TupleVarRef("w", itemTp)
    val xref = TupleVarRef("x", itemTp)
    val yref = TupleVarRef("y", itemTp)
    val zref = TupleVarRef("z", itemTp)
    ForeachUnion(xref, relationR, 
      ForeachUnion(yref, ForeachUnion(zref, relationS, 
                          ForeachUnion(wref, relationT, 
                            IfThenElse(Cmp(OpEq, zref("a"), wref("a")), 
                              Singleton(Tuple("a" -> zref("a"), "b" -> wref("b")))))),
        IfThenElse(Cmp(OpEq, xref("a"), yref("a")), 
          Singleton(Tuple("c" -> xref("b"), "d" -> yref("b"))))))
  }









}
