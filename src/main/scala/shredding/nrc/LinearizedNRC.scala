package shredding.nrc

import shredding.core.{TupleType, Type}

/**
  * Linearized NRC extensions
  */
trait LinearizedNRC extends ShredNRC {

  case class Named(n: String, e: Expr) extends Expr {
    val tp: Type = TupleType()    // unit type
  }

  case class Sequence(exprs: List[Expr]) extends Expr {
    val tp: Type = TupleType()    // unit type
  }
}
