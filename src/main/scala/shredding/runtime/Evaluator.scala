package shredding.runtime

import shredding.core._
import shredding.nrc.{LinearizedNRC, Printer}

/**
  * Simple Scala evaluator
  */
trait Evaluator extends LinearizedNRC with ScalaRuntime with Printer {

  def eval(e: Expr, ctx: Context): Any = e match {
    case Const(v, _) => v
    case v: VarRef => ctx(v.varDef)
    case p: Project =>
      evalTuple(p.tuple, ctx)(p.field)
    case ForeachUnion(x, e1, e2) =>
      val v1 = evalBag(e1, ctx)
      val v = v1.flatMap { xv => ctx.add(x, xv); evalBag(e2, ctx) }
      ctx.remove(x)
      v
    case Union(e1, e2) => evalBag(e1, ctx) ++ evalBag(e2, ctx)
    case Singleton(e1) => List(evalTuple(e1, ctx))
    case WeightedSingleton(e1, w1) =>
//      List(evalTuple(e1, ctx))
      sys.error("Unsupported evaluation of WeightedSingleton")
    case Tuple(fs) => fs.map(x => x._1 -> eval(x._2, ctx))
    case l: Let =>
      ctx.add(l.x, eval(l.e1, ctx))
      val v = eval(l.e2, ctx)
      ctx.remove(l.x)
      v
    case Total(e1) => evalBag(e1, ctx).size
    case DeDup(e1) => evalBag(e1, ctx).distinct
    case i: IfThenElse =>
      val vl = eval(i.cond.e1, ctx)
      val vr = eval(i.cond.e2, ctx)
      i.cond.op match {
        case OpEq =>
          if (vl == vr) eval(i.e1, ctx)
          else i.e2.map(eval(_, ctx)).getOrElse(Nil)
        case OpNe =>
          if (vl != vr) eval(i.e1, ctx)
          else i.e2.map(eval(_, ctx)).getOrElse(Nil)
        case op => sys.error("Unsupported comparison operator: " + op)
      }

    case NewLabel(as) =>
      ROutLabel(as.map(a => a.varDef -> ctx(a.varDef)).toMap)
    case Lookup(l, BagDict(_, f, _)) =>
      val dictFn = new DictFn(ctx, c => evalBag(f, c))
      ROutBagDict(dictFn, f.tp, null)(evalLabel(l, ctx))
    case Lookup(l, d) =>
      evalBagDict(d, ctx)(evalLabel(l, ctx))
    case EmptyDict => REmptyDict
    case BagDict(_, flat, dict) =>
      val dictFn = new DictFn(ctx, c => evalBag(flat, c))
      ROutBagDict(dictFn, flat.tp, evalTupleDict(dict, ctx))
    case TupleDict(fs) =>
      RTupleDict(fs.map(f => f._1 -> eval(f._2, ctx).asInstanceOf[RTupleDictAttribute]))
    case BagDictProject(t, f) =>
      evalTupleDict(t, ctx).fields(f)
    case TupleDictProject(b) =>
      evalBagDict(b, ctx).dict
    case DictUnion(d1, d2) =>
      evalDict(d1, ctx).union(evalDict(d2, ctx))

    case Named(v, e1) =>
      val b = eval(e1, ctx)
      ctx.add(v, b)
      b
    case Sequence(ee) => ee.map(eval(_, ctx))

    case _ => sys.error("Cannot evaluate unknown expression " + e)
  }

  protected def evalBag(e: Expr, ctx: Context): List[_] =
    eval(e, ctx).asInstanceOf[List[_]]

  protected def evalTuple(e: Expr, ctx: Context): Map[String, _] =
    eval(e, ctx).asInstanceOf[Map[String, _]]

  protected def evalLabel(e: LabelExpr, ctx: Context): RLabel =
    eval(e, ctx).asInstanceOf[RLabel]

  protected def evalDict(e: DictExpr, ctx: Context): RDict =
    eval(e, ctx).asInstanceOf[RDict]

  protected def evalBagDict(e: BagDictExpr, ctx: Context): RBagDict =
    eval(e, ctx).asInstanceOf[RBagDict]

  protected def evalTupleDict(e: TupleDictExpr, ctx: Context): RTupleDict =
    eval(e, ctx).asInstanceOf[RTupleDict]
}
