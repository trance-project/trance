package shredding.runtime

import shredding.core._
import shredding.nrc.MaterializeNRC

/**
  * Simple Scala evaluator
  */
trait Evaluator extends MaterializeNRC with ScalaRuntime {

  def eval(e: Expr, ctx: Context): Any = e match {
    case c: Const => c.v
    case v: VarRef => ctx(v.varDef)
    case p: Project =>
      evalTuple(p.tuple, ctx)(p.field)
    case ForeachUnion(x, e1, e2) =>
      val v1 = evalBag(e1, ctx)
      val v = v1.flatMap { xv => ctx.add(x, xv); evalBag(e2, ctx) }
      ctx.remove(x)
      v
    case Union(e1, e2) =>
      evalBag(e1, ctx) ++ evalBag(e2, ctx)
    case Singleton(e1) =>
      List(evalTuple(e1, ctx))
    case DeDup(e1) =>
      evalBag(e1, ctx).distinct
    case Tuple(fs) =>
      fs.map(x => x._1 -> eval(x._2, ctx))
    case l: Let =>
      ctx.add(l.x, eval(l.e1, ctx))
      val v = eval(l.e2, ctx)
      ctx.remove(l.x)
      v
    case c: Cmp => (c.e1.tp, c.e2.tp) match {
      case (StringType, StringType) =>
        evalCmp(
          c.op,
          eval(c.e1, ctx).asInstanceOf[String],
          eval(c.e2, ctx).asInstanceOf[String])
      case (IntType, IntType) =>
        evalCmp(
          c.op,
          eval(c.e1, ctx).asInstanceOf[Int],
          eval(c.e2, ctx).asInstanceOf[Int])
      case (LongType, LongType) =>
        evalCmp(
          c.op,
          eval(c.e1, ctx).asInstanceOf[Long],
          eval(c.e2, ctx).asInstanceOf[Long])
      case (DoubleType, DoubleType) =>
        evalCmp(
          c.op,
          eval(c.e1, ctx).asInstanceOf[Double],
          eval(c.e2, ctx).asInstanceOf[Double])
      case (t1, t2) =>
        sys.error("Cannot compare values of type " + t1 + " and " + t2)
    }
    case And(e1, e2) =>
      evalBool(e1, ctx) && evalBool(e2, ctx)
    case Or(e1, e2) =>
      evalBool(e1, ctx) || evalBool(e2, ctx)
    case Not(e1) =>
      !evalBool(e1, ctx)
    case i: IfThenElse =>
      if (evalBool(i.cond, ctx)) eval(i.e1, ctx)
      else i.e2.map(eval(_, ctx)).getOrElse(Nil)
    case ArithmeticExpr(op, a1, a2) => e.tp match {
      case IntType =>
        evalArithmeticIntegral(
          op,
          eval(a1, ctx).asInstanceOf[Int],
          eval(a2, ctx).asInstanceOf[Int])
      case LongType =>
        evalArithmeticIntegral(
          op,
          eval(a1, ctx).asInstanceOf[Long],
          eval(a2, ctx).asInstanceOf[Long])
      case DoubleType =>
        evalArithmeticFractional(
          op,
          eval(a1, ctx).asInstanceOf[Double],
          eval(a2, ctx).asInstanceOf[Double])
      case _ => sys.error("Arithmetic type not supported " + e.tp)
    }
    case Count(e1) =>
      evalBag(e1, ctx).size
    case Sum(e1, fs) =>
      val b = evalBag(e1, ctx)
      fs.map { f => f -> sumAggregate(b, f, e1.tp.tp(f)) }.toMap
    case GroupByKey(e1, ks, vs) =>
      evalBag(e1, ctx).map { t =>
        val tuple = t.asInstanceOf[Map[String, _]]
        val keys = ks.map(k => k -> tuple(k)).toMap
        val values = vs.map(v => v -> tuple(v)).toMap
        (keys, values)
      }.groupBy(_._1).map { case (k, v) =>
        k ++ Map("group" -> v.map(_._2))
      }.toList
    case SumByKey(e1, ks, vs) =>
      evalBag(e1, ctx).map { t =>
        val tuple = t.asInstanceOf[Map[String, _]]
        val keys = ks.map(k => k -> tuple(k)).toMap
        val values = vs.map(v => v -> tuple(v)).toMap
        (keys, values)
      }.groupBy(_._1).map { case (k, v) =>
        val b = v.map(_._2)
        k ++ vs.map(v => v -> sumAggregate(b, v, e1.tp.tp(v))).toMap
      }.toList

    // Label extensions
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
    case l: NewLabel =>
      ROutLabel(l.params.map {
        case l: VarRefLabelParameter =>
          l.e.varDef -> ctx(l.e.varDef)
        case l: ProjectLabelParameter =>
          val v1 = VarDef(l.name, l.tp)
          v1 -> eval(l.e.asInstanceOf[Expr], ctx)
      }.toMap)

    // Dictionary extensions
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

    // Shredding extensions
    case ShredUnion(e1, e2) =>
      sys.error("Not implemented")
    case Lookup(l, BagDict(_, f, _)) =>
      val dictFn = new DictFn(ctx, c => evalBag(f, c))
      ROutBagDict(dictFn, f.tp, null)(evalLabel(l, ctx))
    case Lookup(l, d) =>
      evalBagDict(d, ctx)(evalLabel(l, ctx))

    // Materialization extensions
    case MatDictLookup(l, b) =>
      sys.error("Not implemented")

    case _ => sys.error("Cannot evaluate unknown expression " + e)
  }

  def eval(a: Assignment, ctx: Context): Unit =
    ctx.add(VarDef(a.name, a.rhs.tp), eval(a.rhs, ctx))

  def eval(p: Program, ctx: Context): Unit =
    p.statements.foreach(eval(_, ctx))

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

  protected def evalBool(e: Expr, ctx: Context): Boolean =
    eval(e, ctx).asInstanceOf[Boolean]

  protected def evalCmp[T](op: OpCmp, v1: T, v2: T)
                          (implicit o: Ordering[T]): Boolean = op match {
    case OpEq => o.eq(v1, v2)
    case OpNe => o.ne(v1, v2)
    case OpGt => o.gt(v1, v2)
    case OpGe => o.gteq(v1, v2)
  }

  protected def evalArithmeticIntegral[T](op: OpArithmetic, a1: T, a2: T)
                                         (implicit i: Integral[T]): T = op match {
    case OpPlus => i.plus(a1, a2)
    case OpMinus => i.minus(a1, a2)
    case OpMultiply => i.times(a1, a2)
    case OpDivide => i.quot(a1, a2)
    case OpMod => i.rem(a1, a2)
  }

  protected def evalArithmeticFractional[T](op: OpArithmetic, a1: T, a2: T)
                                           (implicit f: Fractional[T]): T = op match {
    case OpPlus => f.plus(a1, a2)
    case OpMinus => f.minus(a1, a2)
    case OpMultiply => f.times(a1, a2)
    case OpDivide => f.div(a1, a2)
    case OpMod => sys.error("Modulo over fractional values")
  }

  protected def sumAggregate(b: List[_], f: String, tp: Type): AnyVal = tp match {
    case IntType =>
      b.map(_.asInstanceOf[Map[String, Int]](f)).sum
    case LongType =>
      b.map(_.asInstanceOf[Map[String, Long]](f)).sum
    case DoubleType =>
      b.map(_.asInstanceOf[Map[String, Double]](f)).sum
    case _ => sys.error("Aggregation over non-numeric values of type " + tp)
  }
}

