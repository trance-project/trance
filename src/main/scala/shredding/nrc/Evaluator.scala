package shredding.nrc

import shredding.core._

/**
  * Simple Scala evaluator
  */
trait NRCEvaluator {
  this: NRC =>

  def eval(e: Expr, ctx: Context): Any = e match {
    case Const(v, _) => v
    case v: VarRef => ctx(v.name)
    case p: Project => evalTuple(p.tuple, ctx)(p.field)
    case ForeachUnion(x, e1, e2) =>
      val v1 = evalBag(e1, ctx)
      val v = v1.flatMap { xv => ctx.add(x.name, xv, e1.tp.tp); evalBag(e2, ctx) }
      ctx.remove(x.name, e1.tp.tp)
      v
    case Union(e1, e2) => evalBag(e1, ctx) ++ evalBag(e2, ctx)
    case Singleton(e1) => List(evalTuple(e1, ctx))
    case Tuple(fs) => fs.map(x => x._1 -> eval(x._2, ctx))
    case l: Let =>
      ctx.add(l.x.name, eval(l.e1, ctx), l.e1.tp)
      val v = eval(l.e2, ctx)
      ctx.remove(l.x.name, l.e1.tp)
      v
    case Total(e1) => evalBag(e1, ctx).size
    case i: IfThenElse =>
      val vl = eval(i.cond.e1, ctx)
      val vr = eval(i.cond.e2, ctx)
      i.cond.op match {
        case OpEq =>
          if (vl == vr) evalBag(i.e1, ctx)
          else i.e2.map(evalBag(_, ctx)).getOrElse(Nil)
        case OpNe =>
          if (vl != vr) evalBag(i.e1, ctx)
          else i.e2.map(evalBag(_, ctx)).getOrElse(Nil)
        case op => sys.error("Unsupported comparison operator: " + op)
      }
    case _ => sys.error("Cannot evaluate unknown expression " + e)
  }

  protected def evalBag(e: Expr, ctx: Context): List[_] =
    eval(e, ctx).asInstanceOf[List[_]]

  protected def evalTuple(e: Expr, ctx: Context): Map[String, _] =
    eval(e, ctx).asInstanceOf[Map[String, _]]

}

//trait ShreddedEvaluator extends Evaluator {
//  this: ShreddedNRC =>
//
//  override def eval(e: Expr, ctx: Context): Any = e match {
//    case Lookup(l: Label, d: InputBagDict) => d.f(l)
//    case Lookup(l: LabelProject, d: InputBagDict) =>
//      val prj = evalTuple(l.tuple, ctx)(l.field).asInstanceOf[Label]
//      d.f(prj)
//    case Lookup(l1: Label, OutputBagDict(l2, flatBag, _)) if l1 == l2 =>
//      eval(flatBag, ctx)
//    case Label(as) => as.map(a => a.name -> eval(a, ctx)).toMap
//    case _ => super.eval(e, ctx)
//  }
//}

trait LinearizedNRCEvaluator extends NRCEvaluator {
  this: LinearizedNRC =>

  override def eval(e: Expr, ctx: Context): Any = e match {
    case Named(n, e1) =>
      val v = evalBag(e1, ctx)
      ctx.add(n, v, e1.tp)
      v
    case Sequence(ee) => ee.map(eval(_, ctx))
    case _ => super.eval(e, ctx)
  }
}