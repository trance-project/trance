package framework.runtime

import framework.common._
import framework.nrc.MaterializeNRC

/**
  * Simple Scala evaluator
  */
trait Evaluator extends MaterializeNRC with ScalaRuntime {

  def eval(e: Expr, ctx: RuntimeContext): Any = e match {
    case c: Const =>
      c.v
    case v: VarRef =>
      ctx(v.varDef)
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
        evalArithmeticIntegral(op, evalInt(a1, ctx), evalInt(a2, ctx))
      case LongType =>
        evalArithmeticIntegral(op, evalLong(a1, ctx), evalLong(a2, ctx))
      case DoubleType =>
        evalArithmeticFractional(op, evalDouble(a1, ctx), evalDouble(a2, ctx))
      case _ =>
        sys.error("Arithmetic type not supported " + e.tp)
    }
    case Count(e1) =>
      evalBag(e1, ctx).size
    case Sum(e1, fs) =>
      val b = evalBag(e1, ctx)
      fs.map { f => f -> reduceAggregate(b, f, e1.tp.tp(f)) }.toMap
    case GroupByKey(e1, ks, vs, an) =>
      evalBag(e1, ctx).map { t =>
        val tuple = t.asInstanceOf[Map[String, _]]
        val keys = ks.map(k => k -> tuple(k)).toMap
        val values = vs.map(v => v -> tuple(v)).toMap
        (keys, values)
      }.groupBy(_._1).map { case (k, v) =>
        k ++ Map(an -> v.map(_._2))
      }.toList
    case ReduceByKey(e1, ks, vs) =>
      evalBag(e1, ctx).map { t =>
        val tuple = t.asInstanceOf[Map[String, _]]
        val keys = ks.map(k => k -> tuple(k)).toMap
        val values = vs.map(v => v -> tuple(v)).toMap
        (keys, values)
      }.groupBy(_._1).map { case (k, v) =>
        val b = v.map(_._2)
        k ++ vs.map(v => v -> reduceAggregate(b, v, e1.tp.tp(v))).toMap
      }.toList

    // Label extensions
    case x: ExtractLabel =>
      val newBoundVars = x.lbl.tp.attrTps.filterNot {
        case (n2, t2) => ctx.contains(VarDef(n2, t2))
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
        case (n, l) => VarDef(n, l.tp) -> eval(l.e, ctx)
      })

    // Dictionary extensions
    case EmptyDict =>
      REmptyDict
    case BagDict(lblTp, flat, dict) =>
      val dictFn = new DictFn(ctx, c => evalBag(flat, c))
      ROutBagDict(dictFn, lblTp, flat.tp, evalTupleDict(dict, ctx))
    case TupleDict(fs) =>
      RTupleDict(fs.map(f => f._1 -> eval(f._2, ctx).asInstanceOf[RTupleDictAttribute]))
    case TupleDictProject(b) =>
      evalBagDict(b, ctx).dict
    case d: DictUnion =>
      evalDict(d.dict1, ctx).union(evalDict(d.dict2, ctx))

    // Shredding extensions
    case _: ShredUnion =>
      sys.error("Not implemented")
    case Lookup(l, BagDict(lblTp, f, _)) =>
      val dictFn = new DictFn(ctx, c => evalBag(f, c))
      ROutBagDict(dictFn, lblTp, f.tp, null)(evalLabel(l, ctx))
    case Lookup(l, d) =>
      evalBagDict(d, ctx)(evalLabel(l, ctx))

    // Materialization extensions
    case MatDictLookup(l, d) =>
      evalMatDict(d, ctx)(evalLabel(l, ctx))
    case BagToMatDict(b) =>
      evalBag(b, ctx).asInstanceOf[List[Map[String, _]]].map(x =>
        x(KEY_ATTR_NAME).asInstanceOf[RLabel] -> (x - KEY_ATTR_NAME)
      ).groupBy(_._1).mapValues(_.map(_._2))
    case MatDictToBag(d) =>
      evalMatDict(d, ctx).toList.flatMap {
        case (k, v) => v.asInstanceOf[List[Map[String, _]]].map(_ + (KEY_ATTR_NAME -> k))
      }

    case _ => sys.error("Cannot evaluate unknown expression " + e)
  }

  def eval(a: Assignment, ctx: RuntimeContext): Unit =
    ctx.add(VarDef(a.name, a.rhs.tp), eval(a.rhs, ctx))

  def eval(p: Program, ctx: RuntimeContext): Unit =
    p.statements.foreach(eval(_, ctx))

  protected def evalBag(e: Expr, ctx: RuntimeContext): List[_] =
    eval(e, ctx).asInstanceOf[List[_]]

  protected def evalTuple(e: Expr, ctx: RuntimeContext): Map[String, _] =
    eval(e, ctx).asInstanceOf[Map[String, _]]

  protected def evalLabel(e: LabelExpr, ctx: RuntimeContext): RLabel =
    eval(e, ctx).asInstanceOf[RLabel]

  protected def evalDict(e: DictExpr, ctx: RuntimeContext): RDict =
    eval(e, ctx).asInstanceOf[RDict]

  protected def evalBagDict(e: BagDictExpr, ctx: RuntimeContext): RBagDict =
    eval(e, ctx).asInstanceOf[RBagDict]

  protected def evalTupleDict(e: TupleDictExpr, ctx: RuntimeContext): RTupleDict =
    eval(e, ctx).asInstanceOf[RTupleDict]

  protected def evalBool(e: Expr, ctx: RuntimeContext): Boolean =
    eval(e, ctx).asInstanceOf[Boolean]

  protected def evalInt(e: Expr, ctx: RuntimeContext): Int = e.tp match {
    case IntType => eval(e, ctx).asInstanceOf[Int]
    case _ => sys.error("Cannot evaluate integer of type " + e.tp)
  }

  protected def evalLong(e: Expr, ctx: RuntimeContext): Long = e.tp match {
    case IntType => eval(e, ctx).asInstanceOf[Int].toLong
    case LongType => eval(e, ctx).asInstanceOf[Long]
    case _ => sys.error("Cannot evaluate long of type " + e.tp)
  }

  protected def evalDouble(e: Expr, ctx: RuntimeContext): Double = e.tp match {
    case IntType => eval(e, ctx).asInstanceOf[Int].toDouble
    case LongType => eval(e, ctx).asInstanceOf[Long].toDouble
    case DoubleType => eval(e, ctx).asInstanceOf[Double]
    case _ => sys.error("Cannot evaluate double of type " + e.tp)
  }

  protected def evalCmp[T](op: OpCmp, v1: T, v2: T)
                          (implicit o: Ordering[T]): Boolean = op match {
    case OpEq => v1 == v2
    case OpNe => v1 != v2
    case OpGt => o.gt(v1, v2)
    case OpGe => o.gteq(v1, v2)
  }

  protected def evalMatDict(e: MatDictExpr, ctx: RuntimeContext): Map[RLabel, List[_]] = {
    eval(e, ctx).asInstanceOf[Map[RLabel, List[_]]]
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

  protected def reduceAggregate(b: List[_], f: String, tp: Type): Any = tp match {
    case IntType =>
      b.map(_.asInstanceOf[Map[String, Int]](f)).sum
    case LongType =>
      b.map(_.asInstanceOf[Map[String, Long]](f)).sum
    case DoubleType =>
      b.map(_.asInstanceOf[Map[String, Double]](f)).sum
    case _: BagType =>
      b.map(_.asInstanceOf[Map[String, List[_]]](f)).reduce(_ ++ _)
    case _ =>
      sys.error("Aggregation over non-reducible values of type " + tp)
  }
}

