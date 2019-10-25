package shredding.nrc

import shredding.core.{VarDef, TupleType, OpEq}
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

  def nestingRewrite(e: Expr): Expr = replace(e, {
    case f @ ForeachUnion(x, b1,
      BagIfThenElse(
        Cmp(OpEq,
          p1 @ PrimitiveProject(t1: TupleVarRef, f1),
          p2 @ PrimitiveProject(t2: TupleVarRef, f2)),
        b2, None)) =>

      val bag1 = nestingRewrite(b1).asInstanceOf[BagExpr]
      val bag2 = nestingRewrite(b2).asInstanceOf[BagExpr]

      val ivars = inputVars(f)
      if (ivars.contains(t1) && !ivars.contains(t2)) {
        val gb = VarDef(Symbol.fresh("gb"), TupleType("key" -> t2.tp(f2), "value" -> bag2.tp))
        val gbr = TupleVarRef(gb)
        ForeachUnion(gb,
          ForeachUnion(x, bag1, Singleton(Tuple("key" -> p2, "value" -> bag2))),
          BagIfThenElse(Cmp(OpEq, p1, gbr("key")), gbr("value").asInstanceOf[BagExpr], None)
        )
      }
      else if (!ivars.contains(t1) && ivars.contains(t2)) {
        val gb = VarDef(Symbol.fresh("gb"), TupleType("key" -> t1.tp(f1), "value" -> bag2.tp))
        val gbr = TupleVarRef(gb)
        ForeachUnion(gb,
          ForeachUnion(x, bag1, Singleton(Tuple("key" -> p1, "value" -> bag2))),
          BagIfThenElse(Cmp(OpEq, p2, gbr("key")), gbr("value").asInstanceOf[BagExpr], None)
        )
      }
      else ForeachUnion(x, bag1, BagIfThenElse(Cmp(OpEq, p1, p2), bag2, None))
  })

  def nestingRewriteLossy(e: Expr): Expr = replace(e, {
    case f @ ForeachUnion(x, b1,
      BagIfThenElse(
        Cmp(OpEq,
          p1 @ PrimitiveProject(t1: TupleVarRef, f1),
          p2 @ PrimitiveVarRef(t2)), // label has been replaced by it's variable representation
        b2, None)) =>

      val bag1 = nestingRewrite(b1).asInstanceOf[BagExpr]
      val bag2 = nestingRewrite(b2).asInstanceOf[BagExpr]

      val ivars = inputVars(f)
      if (ivars.contains(t1) && !ivars.contains(p2)) {
        ForeachUnion(x, bag1, Singleton(Tuple("key" -> p2, "value" -> bag2)))
      }
      else if (!ivars.contains(t1) && ivars.contains(p2)) {
        ForeachUnion(x, bag1, Singleton(Tuple("key" -> p1, "value" -> bag2)))
      }
      else ForeachUnion(x, bag1, BagIfThenElse(Cmp(OpEq, p1, p2), bag2, None))
  })
}
