package shredding.queries.simple

import shredding.core._
import shredding.nrc.LinearizedNRC

case class InputR3(n: Int)
case class InputR2(m: String, n: Int, k: List[InputR3])
case class InputR1(h: Int, j: List[InputR2])

object NestedTests {

  val nrc = new LinearizedNRC{}
  val datat3 = TupleType(Map("n" -> IntType))
  val datat2 = TupleType(Map("m" -> StringType, "n" -> IntType, "k" -> BagType(datat3)))
  val datat = TupleType(Map("h" -> IntType, "j" -> BagType(datat2)))

  val inputRelation = List(Map(
          "h" -> 42,
          "j" -> List(
            Map(
              "m" -> "Milos",
              "n" -> 123,
              "k" -> List(
                Map("n" -> 123),
                Map("n" -> 456),
                Map("n" -> 789),
                Map("n" -> 123)
              )
            ),
            Map(
              "m" -> "Michael",
              "n" -> 7,
              "k" -> List(
                Map("n" -> 2),
                Map("n" -> 9),
               Map("n" -> 1)
              )
            ),
            Map(
              "m" -> "Jaclyn",
              "n" -> 12,
              "k" -> List(
                Map("n" -> 14),
                Map("n" -> 12)
              )
            )
          )
        ),
        Map(
          "h" -> 69,
          "j" -> List(
            Map(
              "m" -> "Thomas",
              "n" -> 987,
              "k" -> List(
                Map("n" -> 987),
                Map("n" -> 654),
                Map("n" -> 987),
                Map("n" -> 654),
                Map("n" -> 987),
                Map("n" -> 987)
              )
            )
          )
        )
      )
    
    val nestedInputs:Map[Type,String] = Map(datat -> "InputR1", datat2 -> "InputR2", datat3 -> "InputR3")
    
    val R = List(InputR1(42, List(InputR2("Milos", 123, List(InputR3(123), InputR3(456), InputR3(789), InputR3(123))),
                                 InputR2("Michael", 7, List(InputR3(2), InputR3(9), InputR3(1))),
                                 InputR2("Jaclyn", 12, List(InputR3(14), InputR3(12))))), 
                InputR1(69, List(InputR2("Thomas", 987, List(InputR3(987), InputR3(654), InputR3(987), InputR3(987))))))
    val header = s"""
      |import shredding.queries.simple._
      |val R = List(InputR1(42, List(InputR2("Milos", 123, List(InputR3(123), InputR3(456), InputR3(789), InputR3(123))),
      |                           InputR2("Michael", 7, List(InputR3(2), InputR3(9), InputR3(1))),
      |                           InputR2("Jaclyn", 12, List(InputR3(14), InputR3(12))))), 
      |          InputR1(69, List(InputR2("Thomas", 987, List(InputR3(987), InputR3(654), InputR3(987), InputR3(987))))))""".stripMargin
     
    /**val R__F = 1
    case class InputR1Flat(a: Int, b: Int)
    case class InputR2Flat(m: String, n: Int, k: Int)
    case class InputR3Flat(n: Int)
    case class InputR2Dict(k: (List[(Int, List[InputR3Flat])], Unit))
    case class InputR1Dict(j: (List[(Int, List[InputR2Flat])], InputR2Dict))
    val R__D = (List((R__F, List(InputR1Flat(42, 2), InputR1Flat(69, 3)))), 
      InputR1Dict(j: (List((2, List(InputR2Flat("Milos", 123, 4), InputR2Flat("Michael", 7, 5), 
                                    InputR2Flat("Jaclyn", 12, 6))),
                            (3, List(InputR2Flat("Thomas", 987, 7)))), 
      InputR2Dict(k: (List((4, List(InputR3Flat(123), InputR3Flat(456), InputR3Flat(789), InputR3Flat(123))), 
                            (5, List(InputR3Flat(2), InputR3Flat(9), InputR3Flat(1))), 
                            (6, List(InputR3Flat(14), InputR3Flat(12))), 
                            (7, List(InputR3Flat(987), InputR3Flat(654), InputR3Flat(987), InputR3Flat(987)))), ())))))
    **/
    val shredHeader = s"""
      |import shredding.queries.simple_
      |val R__F = 1
      """.stripMargin

    // michael's first grouping example
    val query1 = {
      import nrc._
      val relationR = BagVarRef(VarDef("R", BagType(datat)))
      val xdef = VarDef("x", datat)
      val xref = TupleVarRef(xdef)
      val wdef = VarDef("w", datat2)
      val wref = TupleVarRef(wdef)
      val ndef = VarDef("y", datat3)

      ForeachUnion(xdef, relationR,
        Singleton(Tuple(
          "o5" -> xref("h"),
          "o6" ->
            ForeachUnion(wdef, BagProject(xref, "j"),
              Singleton(Tuple(
                "o7" -> wref("m"),
                "o8" -> Total(BagProject(wref, "k"))
              ))
            )
        )))
    }

    // a more shallow version of query1
    val query1a = {
      import nrc._
      val relationR = BagVarRef(VarDef("R", BagType(datat)))
      val xdef = VarDef("x", datat)
      val xref = TupleVarRef(xdef)
      ForeachUnion(xdef, relationR,
        Singleton(Tuple("o5" -> xref("h"), "o6" -> Total(xref("j").asInstanceOf[BagExpr]))))
    }
       
    val query1b = {
      import nrc._
      val relationR = BagVarRef(VarDef("R", BagType(datat)))
      val xdef = VarDef("x", datat)
      val xref = TupleVarRef(xdef)
      ForeachUnion(xdef, relationR,
                IfThenElse(Cmp(OpGt, TupleVarRef(xdef)("h"), Const(60, IntType)),
                  Singleton(Tuple("w" -> TupleVarRef(xdef)("j")))))
    }
    
    val query1c = {
      import nrc._
      val relationR = BagVarRef(VarDef("R", BagType(datat)))
      val xdef = VarDef("x", datat)
      val xref = TupleVarRef(xdef)
      ForeachUnion(xdef, relationR,
                IfThenElse(Cmp(OpGt, TupleVarRef(xdef)("h"), Const(60, IntType)),
                  Singleton(Tuple("w" -> Total(TupleVarRef(xdef)("j").asInstanceOf[BagExpr])))))
    }
    
    val query1d = {
      import nrc._
      val relationR = BagVarRef(VarDef("R", BagType(datat)))
      val xdef = VarDef("x", datat)
      val xref = TupleVarRef(xdef)
      val x2def = VarDef("x2", datat)
      ForeachUnion(xdef, relationR,
                ForeachUnion(x2def, relationR,
                  IfThenElse(Cmp(OpEq, TupleVarRef(xdef)("h"), TupleVarRef(x2def)("h")),
                    Singleton(Tuple("x" -> TupleVarRef(xdef)("j"), "x2" -> TupleVarRef(x2def)("j"))))))
    }

    
    val data2 = List(Map("a" -> 42, "b" -> List(Map("c" -> 1), Map("c" -> 2), Map("c" -> 4))),
                              Map("a" -> 49, "b" -> List(Map("c" -> 3), Map("c" -> 2))),
                              Map("a" -> 34, "b" -> List(Map("c" ->5))))

    val query2 = {
      import nrc._
      val itemTp = TupleType("a" -> IntType, "b" -> BagType(TupleType("c" -> IntType)))
      val relationR = BagVarRef(VarDef("R", BagType(itemTp)))
      val x = VarDef("x", itemTp)
      val x1 = VarDef("x1", itemTp)
      val y = VarDef("y", TupleType("c" -> IntType))
      ForeachUnion(x, relationR,
                IfThenElse(Cmp(OpGt, TupleVarRef(x)("a"), Const(40, IntType)),
                  Singleton(Tuple("o1" -> TupleVarRef(x)("a"), "o2" -> Total(BagProject(TupleVarRef(x), "b"))))))      
    } 

    val query2a = {
      import nrc._
      val itemTp = TupleType("a" -> IntType, "b" -> BagType(TupleType("c" -> IntType)))
      val relationR = BagVarRef(VarDef("R", BagType(itemTp)))
      val x = VarDef("x", itemTp)
      val x1 = VarDef("x1", itemTp)
      val y = VarDef("y", TupleType("c" -> IntType))
      ForeachUnion(x, relationR,
                ForeachUnion(x1, relationR,
                  ForeachUnion(y, BagProject(TupleVarRef(x), "b"),
                    Singleton(Tuple("o1" -> TupleVarRef(x1)("a"), "o2" -> Total(BagProject(TupleVarRef(x), "b")))))))
    }
    
    // michael's filter inner bag example
    val query3 = {
      import nrc._
      val ytp = TupleType("b" -> IntType, "c" -> IntType)
      val xtp = TupleType("a" -> IntType, "s1" -> BagType(ytp))
      val xdef = VarDef("x", xtp)
      val ydef = VarDef("y", ytp)
      val r = BagVarRef(VarDef("R", BagType(xtp)))
      ForeachUnion(xdef, r,
        Singleton(Tuple("a'" -> TupleVarRef(xdef)("a"),
          "s1'" -> ForeachUnion(ydef, BagProject(TupleVarRef(xdef), "s1"),
          IfThenElse(Cmp(OpGt, Const(5, IntType), TupleVarRef(ydef)("c")), Singleton(TupleVarRef(ydef)))))))
    }

    // filter inner bag when let is shared
    val query4 = {
      import nrc._
      val ytp = TupleType("b" -> IntType, "c" -> IntType)
      val xtp = TupleType("a" -> IntType, "s1" -> BagType(ytp), "s2" -> BagType(ytp))
      val xdef = VarDef("x", xtp)
      val ydef = VarDef("y", ytp)

      val r = BagVarRef(VarDef("R", BagType(xtp)))
      ForeachUnion(xdef, r,
              Singleton(Tuple("a'" -> TupleVarRef(xdef)("a"),
                "s1'" -> ForeachUnion(ydef, BagProject(TupleVarRef(xdef), "s1"),
                          IfThenElse(Cmp(OpGt, Const(5, IntType), TupleVarRef(ydef)("c")), Singleton(TupleVarRef(ydef)))),
                "s2'" -> ForeachUnion(ydef, BagProject(TupleVarRef(xdef), "s2"),
                          IfThenElse(Cmp(OpGt, TupleVarRef(ydef)("c"), Const(6, IntType)),
                            Singleton(TupleVarRef(ydef)))))))
    }

}
