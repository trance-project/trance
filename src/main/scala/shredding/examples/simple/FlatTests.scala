package shredding.examples.simple

import shredding.core._
import shredding.nrc.LinearizedNRC

object FlatTests {
  val nrc = new LinearizedNRC{}
  import nrc._

  // source NRC reference format
  val relR = BagVarRef(VarDef("R", BagType(FlatRelations.type1a)))
  val x = VarDef("x", FlatRelations.type1a)
 
  // For x in R union { ( o1 := x.a ) }
  val q1 = ForeachUnion(x, relR, 
            Singleton(Tuple("o1" -> TupleVarRef(x)("a"))))

  // TotalMult(For x in R union { x }
  val q2 = Total(ForeachUnion(x, relR, Singleton(TupleVarRef(x))))

  // For x in R union
  //  if x.a > 40 
  //  then { ( o1 := x.b ) }
  val q3 = ForeachUnion(x, relR, 
            IfThenElse(Cmp(OpGt, TupleVarRef(x)("a"), Const(40, IntType)),
              Singleton(Tuple("o1" -> TupleVarRef(x)("b")))))
  
  val y = VarDef("y", FlatRelations.type1a)
  val q4 = ForeachUnion(x, relR, 
            ForeachUnion(y, relR, 
              IfThenElse(Cmp(OpEq, TupleVarRef(x)("a"), TupleVarRef(y)("a")), 
                Singleton(Tuple("a" -> TupleVarRef(x)("a"), 
                  "b1" -> TupleVarRef(x)("b"), "b2" -> TupleVarRef(y)("b")))))) 

}
