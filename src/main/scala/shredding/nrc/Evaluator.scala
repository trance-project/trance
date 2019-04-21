package shredding.nrc

import shredding.core._

/**
  * Simple Scala evaluator
  */
trait Evaluator {
  this: NRC =>

  class Context {
    val ctx: collection.mutable.HashMap[String, Any] =
      collection.mutable.HashMap[String, Any]()

    def apply(n: String): Any = ctx(n)

    def add(n: String, v: Any, tp: Type): Unit = tp match {
      case TupleType(tas) if tas.contains("lbl") =>
        // Extract variables encapsulated by the label
        val m = v.asInstanceOf[Map[String, Map[String, Any]]]("lbl")
        val LabelType(las) = tas("lbl")
        las.foreach { case (n2, t2) => add(n2, m(n2), t2) }
        ctx(n) = v
      case _ => ctx(n) = v
    }

    def remove(n: String, tp: Type): Unit = tp match {
      case LabelType(as) =>
        // Remove variables encapsulated by the label
        as.foreach { case (n2, tp2) => remove(n2, tp2) }
      case _ => ctx.remove(n)
    }

    override def toString: String = ctx.toString
  }

  def eval(e: Expr): Any = eval(e, new Context())

  protected def evalBag(e: Expr, ctx: Context): List[_] =
    eval(e, ctx).asInstanceOf[List[_]]

  protected def evalTuple(e: Expr, ctx: Context): Map[String, _] =
    eval(e, ctx).asInstanceOf[Map[String, _]]

  protected def eval(e: Expr, ctx: Context): Any = e match {
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
    case IfThenElse(Cond(op, l, r), e1, e2) =>
      val vl = eval(l, ctx)
      val vr = eval(r, ctx)
      op match {
        case OpEq =>
          if (vl == vr) evalBag(e1, ctx)
          else e2.map(evalBag(_, ctx)).getOrElse(Nil)
        case OpNe =>
          if (vl != vr) evalBag(e1, ctx)
          else e2.map(evalBag(_, ctx)).getOrElse(Nil)
        case _ => sys.error("Unsupported comparison operator: " + op)
      }
    case InputBag(_, b, _) => b
    case Named(n, e1) =>
      val v = evalBag(e1, ctx)
      ctx.add(n, v, e1.tp)
      v
    case Sequence(ee) => ee.map(eval(_, ctx))
    case _ => sys.error("Cannot evaluate unknown expression " + e)
  }
}

trait ShreddedEvaluator extends Evaluator {
  this: ShreddedNRC =>

  protected override def eval(e: Expr, ctx: Context): Any = e match {
    case Lookup(l: Label, d: InputBagDict) => d.f(l)
    case Lookup(l: LabelProject, d: InputBagDict) =>
      val prj = evalTuple(l.tuple, ctx)(l.field).asInstanceOf[Label]
      d.f(prj)
    case Lookup(l1: Label, OutputBagDict(l2, flatBag, _)) if l1 == l2 =>
      eval(flatBag, ctx)
    case Label(as) => as.map(a => a.name -> eval(a, ctx)).toMap
    case _ => super.eval(e, ctx)
  }
}
