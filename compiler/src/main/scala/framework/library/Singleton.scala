package framework.library

import framework.nrc.NRC

case object Singleton extends NRC {
  def apply(x: Any): Any = {
    Singleton(x.asInstanceOf[TupleExpr])
  }
}
