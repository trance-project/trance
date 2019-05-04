package shredding.nrc

import shredding.core.{VarDef, TupleType}
import shredding.Utils.Symbol

/**
  * Simple optimization:
  *   - dead code elimination
  *   - beta reduction
  */
trait Optimizer extends Extensions {
  this: ShredNRC =>

  def optimize(e: ShredExpr): ShredExpr =
    ShredExpr(optimize(e.flat), optimize(e.dict).asInstanceOf[DictExpr])

  def optimize(e: Expr): Expr = deadCodeElimination(betaReduce(e))

  def deadCodeElimination(e: Expr): Expr = replace(e, {
    case l: Let =>
      val ivars = inputVars(l.e2).map(v => v.name -> v.tp).toMap
      if (ivars.contains(l.x.name)) {
        // Sanity check
        assert(ivars(l.x.name) == l.x.tp)
        l
      }
      else deadCodeElimination(l.e2)
  })

  def betaReduce(e: Expr): Expr = replace(e, {
    case Lookup(l1, BagDict(l2, flatBag, _)) if l1.tp == l2.tp =>
      betaReduce(flatBag)

    case Lookup(l1, BagDict(_, flatBag, _)) /* if l1 != l2 */ =>
      BagLet(
        VarDef(Symbol.fresh("l"), TupleType("lbl" -> l1.tp)), Tuple("lbl" -> l1),
        betaReduce(flatBag).asInstanceOf[BagExpr])
  })

}
