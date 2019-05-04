package shredding.nrc

import shredding.core.{TupleType, Type, VarDef}

/**
  * Linearized NRC extensions
  */
trait LinearizedNRC extends ShredNRC {

  case class Named(v: VarDef, e: Expr) extends Expr {
    def tp: Type = TupleType()    // unit type
  }

  case class Sequence(exprs: List[Expr]) extends Expr {
    val tp: Type = exprs.lastOption.map(_.tp).getOrElse(TupleType())
  }
}