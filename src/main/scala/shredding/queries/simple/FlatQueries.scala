package shredding.queries.simple

import shredding.core._
import shredding.nrc.LinearizedNRC

case class InputR(a: Int, b: String)

object FlatTests {
  
  val nrc = new LinearizedNRC{}
  val data1 = List(Map("a" -> 42, "b" -> "Milos"), Map("a" -> 49, "b" -> "Michael"),
                           Map("a" -> 34, "b" -> "Jaclyn"), Map("a" -> 42, "b" -> "Thomas"))
  val data1c = s"""
    import shredding.queries.simple._
    val R = List(InputR(42, "Milos"), InputR(49, "Michael"), InputR(34, "Jaclyn"), InputR(42, "Thomas"))
  """
  val dtype = TupleType("a" -> IntType, "b" -> StringType)
  
  val query1 = {
    import nrc._
    val relR = BagVarRef(VarDef("R", BagType(dtype)))
    val x = VarDef("x", dtype)
    ForeachUnion(x, relR, Singleton(Tuple("o1" -> TupleVarRef(x)("a"))))
  }

  val query2 = {
    import nrc._
    val relR = BagVarRef(VarDef("R", BagType(dtype)))
    val x = VarDef("x", dtype)
    Total(ForeachUnion(x, relR, Singleton(TupleVarRef(x))))
  }

}

