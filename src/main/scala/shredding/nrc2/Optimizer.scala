package shredding.nrc2

trait Optimizer extends NRCImplicits {
  this: ShreddedNRC =>

  def betaReduce(e: Expr, fullRecursion: Boolean = true): Expr = e.replace {
    case Lookup(l1, OutputBagDict(l2, flatBag, _)) if l1 == l2 =>
      if (fullRecursion) betaReduce(flatBag) else flatBag
      flatBag
  }
}
