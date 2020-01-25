package shredding.runtime

import shredding.core._
import shredding.nrc.ShredNRC

/**
  * Simple Scala evaluator
  */
trait Evaluator extends ShredNRC with ScalaRuntime {

  def eval(e: Expr, ctx: Context): Any = e match {
    case c: Const => c.v
    case v: VarRef => ctx(v.varDef)
    case p: Project =>
      try{ 
        evalTuple(p.tuple, ctx)(p.field)
      }catch{
        case e:Exception => ctx(VarDef(p.tuple.asInstanceOf[TupleVarRef].varDef.name+"."+p.field, p.tp))
      }
    case ForeachUnion(x, e1, e2) =>
      val v1 = evalBag(e1, ctx)
      val v = v1.flatMap { xv => ctx.add(x, xv); evalBag(e2, ctx) }
      ctx.remove(x)
      v
    case Union(e1, e2) => evalBag(e1, ctx) ++ evalBag(e2, ctx)
    case Singleton(e1) => List(evalTuple(e1, ctx))
    case WeightedSingleton(e1, w1) => w1.tp match {
      case IntType => (1 to eval(w1, ctx).asInstanceOf[Int]).map(w => evalTuple(e1, ctx))
      case DoubleType => (1 to eval(w1, ctx).asInstanceOf[Double].toInt).map(w => evalTuple(e1, ctx))
    }
      //sys.error("Unsupported evaluation of WeightedSingleton")
    case Tuple(fs) => fs.map(x => x._1 -> eval(x._2, ctx))
    case l: Let =>
      ctx.add(l.x, eval(l.e1, ctx))
      val v = eval(l.e2, ctx)
      ctx.remove(l.x)
      v
    case Total(e1) => evalBag(e1, ctx).size
    case DeDup(e1) => evalBag(e1, ctx).distinct
    case c: Cmp => c.op match {
      case OpEq => eval(c.e1, ctx) == eval(c.e2, ctx)
      case OpNe => eval(c.e1, ctx) != eval(c.e2, ctx)
      case _ => sys.error("Unsupported comparison operator: " + c.op)
    }
    case And(e1, e2) => evalBool(e1, ctx) && evalBool(e2, ctx)
    case Or(e1, e2) => evalBool(e1, ctx) || evalBool(e2, ctx)
    case Not(e1) => !evalBool(e1, ctx)
    case i: IfThenElse =>
      if (evalBool(i.cond, ctx)) eval(i.e1, ctx)
      else i.e2.map(eval(_, ctx)).getOrElse(Nil)

    case x: ExtractLabel =>
      val las = x.lbl.tp.attrTps
      val newBoundVars = las.filterNot { case (n2, t2) =>
        ctx.contains(VarDef(n2, t2))
      }
      eval(x.lbl, ctx) match {
        case ROutLabel(fs) =>
          newBoundVars.foreach { case (n2, t2) =>
            val d = VarDef(n2, t2)
            ctx.add(d, fs(d))
          }
        case _ =>
      }
      val v = eval(x.e, ctx)
      newBoundVars.foreach { case (n2, tp2) =>
        ctx.remove(VarDef(n2, tp2))
      }
      v
    case NewLabel(as) =>
      ROutLabel(as.map {
        case l: VarRefLabelParameter =>
          l.e.varDef -> ctx(l.e.varDef)
        case l: ProjectLabelParameter =>
          val v1 = VarDef(l.name, l.tp)
          v1 -> eval(l.e.asInstanceOf[Expr], ctx)
      }.toMap)
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
    case d: DictUnion =>
      evalDict(d.dict1, ctx).union(evalDict(d.dict2, ctx))

    case _ => sys.error("Cannot evaluate unknown expression " + e)
  }

  def eval(a: Assignment, ctx: Context): Unit =
    ctx.add(VarDef(a.name, a.rhs.tp), eval(a.rhs, ctx))

  def eval(p: Program, ctx: Context): Unit =
    p.statements.foreach(eval(_, ctx))

  protected def evalBag(e: Expr, ctx: Context): List[_] =
    try {
      eval(e, ctx).asInstanceOf[List[_]]
    }
    catch {
      // TODO: why Vector?
      case _: Exception => eval(e, ctx).asInstanceOf[Vector[_]].toList
    }

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

  protected def evalBool(e: Expr, ctx: Context): Boolean =
    eval(e, ctx).asInstanceOf[Boolean]
}

