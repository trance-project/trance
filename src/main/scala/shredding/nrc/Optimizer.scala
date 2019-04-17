package shredding.nrc

import shredding.core.{TupleType, VarDef}

trait Optimizer extends NRCImplicits {
  this: ShreddedNRC =>

  import shredding.Utils.Symbol

  def betaReduce(e: Expr, fullRecursion: Boolean = true): Expr = e.replace {
    case Lookup(l1, OutputBagDict(l2, flatBag, _)) if l1 == l2 =>
      if (fullRecursion) betaReduce(flatBag) else flatBag

    case Lookup(l1, OutputBagDict(l2, flatBag, _)) /* if l1 == l2 */ =>
      Let(
        VarDef(Symbol.fresh("l"), TupleType("lbl" -> l1.tp)), Tuple("lbl" -> l1),
        if (fullRecursion) betaReduce(flatBag) else flatBag
      )
  }
}
