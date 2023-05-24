package framework.library

import framework.nrc.NRC

case object Singleton extends NRC {
  def apply(x: TupleExpr): BagExpr = Singleton(x)
}
