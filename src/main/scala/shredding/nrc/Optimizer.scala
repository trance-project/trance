package shredding.nrc

import shredding.core.{VarDef, TupleType}
import shredding.utils.Utils.Symbol

/**
  * Simple optimizations:
  *
  *   - let inlining:
  *         let X = e1 in X => e1
  *
  *   - dead code elimination
  *         let X = e1 in e2 => e2 if not using X
  *
  *   - beta reduction
  *         Lookup + BagDict => flatBag
  */
trait Optimizer extends Extensions {
  this: ShredNRC =>

  def optimize(e: ShredExpr): ShredExpr =
    ShredExpr(optimize(e.flat), optimize(e.dict).asInstanceOf[DictExpr])

  def optimize(e: ShredNamed): ShredNamed = {
    val se = ShredExpr(optimize(e.e.flat), optimize(e.e.dict).asInstanceOf[DictExpr])
    ShredNamed(VarDef(e.v.name, se.flat.tp), se)
  }

  def optimize(e: ShredSequence): ShredSequence = ShredSequence(e.exprs.map(s => optimize(s)))

  def optimize(e: Expr): Expr = inline(deadCodeElimination(betaReduce(e)))

  def inline(e: Expr): Expr = replace(e, {
    // let X = e1 in X => e1
    case l: Let if (l.e2 match {
      case v2: VarRef if v2.varDef == l.x => true
      case _ => false
    }) => inline(l.e1)
  })

  def deadCodeElimination(e: Expr): Expr = replace(e, {
    // let X = e1 in e2 => e2 if not using X
    case l: Let if {
      val ivars = inputVars (l.e2).map (v => v.name -> v.tp).toMap
      !ivars.contains (l.x.name) ||
        {
          // Sanity check
          assert (ivars (l.x.name) == l.x.tp)
          false
        }
    } => deadCodeElimination(l.e2)
  })

  def betaReduce(e: Expr): Expr = replace(e, {
    
    case Lookup(l1, BagDict(l2, flatBag, _)) if l1.tp == l2.tp =>
      betaReduce(flatBag)

    case Lookup(l1, BagDict(_, flatBag, _)) /* if l1 != l2 */ =>
      BagLet(
        VarDef(Symbol.fresh("l"), TupleType("lbl" -> l1.tp)), Tuple("lbl" -> l1),
        betaReduce(flatBag).asInstanceOf[BagExpr])

    /* TODO: Doesn't work nested lets */
    case Lookup(l1, BagDictLet(x, e1, BagDict(l2, flatBag, _))) if l1.tp == l2.tp =>
      betaReduce(Let(x, e1, flatBag))

    case Lookup(l1, BagDictIfThenElse(c, BagDict(l2, f2, _), BagDict(l3, f3, _)))
        if l1.tp == l2.tp && l1.tp == l3.tp =>
      betaReduce(BagIfThenElse(c, f2, Some(f3)))
  })

}
