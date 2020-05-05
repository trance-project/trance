package framework.examples.simple

import framework.core._
import framework.nrc.MaterializeNRC

object FlatTests {
  val nrc = new MaterializeNRC{}
  import nrc._

  // source NRC reference format
  val relR = BagVarRef("R", BagType(FlatRelations.type1a))
  val xr = TupleVarRef("x", FlatRelations.type1a)
 
  // For x in R union { ( o1 := x.a ) }
  val q1 = ForeachUnion(xr, relR,
            Singleton(Tuple("o1" -> xr("a"))))

  // TotalMult(For x in R union { x }
  val q2 = Count(ForeachUnion(xr, relR, Singleton(xr)))

  // For x in R union
  //  if x.a > 40 
  //  then { ( o1 := x.b ) }
  val q3 = ForeachUnion(xr, relR,
            IfThenElse(Cmp(OpGt, xr("a"), Const(40, IntType)),
              Singleton(Tuple("o1" -> xr("b")))))
  
  val yr = TupleVarRef("y", FlatRelations.type1a)
  val q4 = ForeachUnion(xr, relR,
            ForeachUnion(yr, relR,
              IfThenElse(Cmp(OpEq, xr("a"), yr("a")),
                Singleton(Tuple("a" -> xr("a"),
                  "b1" -> xr("b"), "b2" -> yr("b"))))))
}
