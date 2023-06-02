package framework.library

import framework.nrc.{BaseExpr, NRC}

// TODO -> Handle casting here
case object Singleton extends BaseExpr {
  def apply(x: TupleExpr): BagExpr = {
    val b = Singleton(x)
    b
  }
}
