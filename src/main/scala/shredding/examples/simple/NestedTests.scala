package shredding.examples.simple

import shredding.core._
import shredding.nrc.LinearizedNRC

object NestedTests {

  val nrc = new LinearizedNRC{}
  import nrc._
  
  // Relation 1

  val relR = BagVarRef(VarDef("R", BagType(NestedRelations.type1a)))    
  val xdef = VarDef("x", NestedRelations.type1a)
  val xref = TupleVarRef(xdef)
  val wdef = VarDef("w", NestedRelations.type2a)
  val wref = TupleVarRef(wdef)
  val ndef = VarDef("y", NestedRelations.type3a)

  // michael's first grouping example
  val q1 = ForeachUnion(xdef, relR,
            Singleton(Tuple(
              "o5" -> xref("h"),
              "o6" -> ForeachUnion(wdef, BagProject(xref, "j"),
                        Singleton(Tuple(
                          "o7" -> wref("m"),
                          "o8" -> Total(BagProject(wref, "k"))))))))
  
  // shallow version of q1
  val q2 = ForeachUnion(xdef, relR,
            Singleton(Tuple(
              "o5" -> xref("h"), 
              "o6" -> Total(xref("j").asInstanceOf[BagExpr]))))
  
  // filter at top level
  val q3 = ForeachUnion(xdef, relR,
            IfThenElse(Cmp(OpGt, TupleVarRef(xdef)("h"), Const(60, IntType)),
              Singleton(Tuple("w" -> TupleVarRef(xdef)("j")))))
  
  // filter at top level and take multiplicity
  val q4 = ForeachUnion(xdef, relR,
            IfThenElse(Cmp(OpGt, TupleVarRef(xdef)("h"), Const(60, IntType)),
              Singleton(Tuple("w" -> Total(TupleVarRef(xdef)("j").asInstanceOf[BagExpr])))))
  
  // self join and project to a bag  
  val x2def = VarDef("x2", NestedRelations.type1a)
  val q5 = ForeachUnion(xdef, relR,
            ForeachUnion(x2def, relR,
              IfThenElse(Cmp(OpEq, TupleVarRef(xdef)("h"), TupleVarRef(x2def)("h")),
                Singleton(Tuple("x" -> TupleVarRef(xdef)("j"), "x2" -> TupleVarRef(x2def)("j"))))))

  // Relation 2
  
  val x3 = VarDef("x", NestedRelations.type21a)
  val x4 = VarDef("x1", NestedRelations.type21a)
  val x5 = VarDef("x2", NestedRelations.type22a)
  val relR2 = BagVarRef(VarDef("R", BagType(NestedRelations.type21a)))

  // filter and multiplicity
  val q6 = ForeachUnion(x3, relR2,
            IfThenElse(Cmp(OpGt, TupleVarRef(x3)("a"), Const(40, IntType)),
              Singleton(Tuple("o1" -> TupleVarRef(x3)("a"), "o2" -> Total(BagProject(TupleVarRef(x3), "b"))))))      

  // self join, and loop over a bag project 
  val q7 = ForeachUnion(x3, relR2,
            ForeachUnion(x4, relR2,
              ForeachUnion(x5, BagProject(TupleVarRef(x3), "b"),
                Singleton(Tuple("o1" -> TupleVarRef(x3)("a"), "o2" -> Total(BagProject(TupleVarRef(x3), "b")))))))
    
   // michael's filter inner bag example
   val q8 = ForeachUnion(x3, relR2,
              Singleton(Tuple("a'" -> TupleVarRef(x3)("a"),
                "s1'" -> ForeachUnion(x5, BagProject(TupleVarRef(x3), "b"),
                 IfThenElse(Cmp(OpGt, Const(2, IntType), TupleVarRef(x5)("c")), Singleton(TupleVarRef(x5)))))))
   // Relation 3
  
   // filter inner bag when let is shared
   val x6 = VarDef("x", NestedRelations.type31a)
   val x7 = VarDef("x1", NestedRelations.type32a)
   val relR3 = BagVarRef(VarDef("R", BagType(NestedRelations.type31a)))

   val q9 = ForeachUnion(x6, relR3,
              Singleton(Tuple("a'" -> TupleVarRef(x6)("a"),
                "s1'" -> ForeachUnion(x7, BagProject(TupleVarRef(x6), "s1"),
                          IfThenElse(Cmp(OpGt, Const(5, IntType), TupleVarRef(x7)("c")), Singleton(TupleVarRef(x7)))),
                "s2'" -> ForeachUnion(x7, BagProject(TupleVarRef(x6), "s2"),
                          IfThenElse(Cmp(OpGt, TupleVarRef(x7)("c"), Const(6, IntType)),
                            Singleton(TupleVarRef(x7)))))))
}