package shredding.wmcc

import shredding.core._

/**
  * Unnesting algorithm from Fegaras and Maier
  * WMCC to algebra data operators (select, reduce, etc)
  */

object Unnester {
  type Ctx = (List[Variable], List[Variable], Option[CExpr])
  @inline def u(implicit ctx: Ctx): List[Variable] = ctx._1
  @inline def w(implicit ctx: Ctx): List[Variable] = ctx._2
  @inline def E(implicit ctx: Ctx): Option[CExpr] = ctx._3

  def unnest(e: CExpr)(implicit ctx: Ctx): CExpr = e match {
    case CDeDup(e1) => CDeDup(unnest(e1)((u, w, E)))
    case Comprehension(e1, v, p, e) if u.isEmpty && w.isEmpty && E.isEmpty =>
      unnest(e)((Nil, List(v), Some(Select(e1, v, p)))) // C4
    case Comprehension(e1 @ Comprehension(_, _, _, _), v, p, e) if !w.isEmpty => // C11
      val (nE, v2) = getNest(unnest(e1)((w, w, E)))
      Bind(e1, v2, unnest(e)((u, w:+v2, nE)))
    case ift @ If(cond, Sng(t @ Record(fs)), None) if !w.isEmpty => // C12, C5, and C8
      assert(!E.isEmpty)
      fs.filter(f => f._2.isInstanceOf[Comprehension]).toList match {
        case Nil =>
          if (u.isEmpty) Reduce(E.get, w, t, cond)
          else {
            val et = Tuple(u)
            val v = Variable.fresh(TTupleType(u.map(_.tp) :+ BagCType(t.tp)))
            Nest(E.get, w, et, t, v, cond)
          }
        case (key, value @ Comprehension(e1, v, p, e)) :: tail =>
          val (nE, v2) = getNest(unnest(value)((w, w, E)))
          Bind(value, v2, unnest(If(cond, Sng(Record(fs + (key -> v2))), None))((u, w :+ v2, nE)))
        case _ => ???
      }
    case s @ Sng(t @ Record(fs)) if !w.isEmpty =>
      assert(!E.isEmpty)
      fs.filter(f => f._2.isInstanceOf[Comprehension]).toList match {
        case Nil =>
          if (u.isEmpty) Reduce(E.get, w, t, Constant(true))
          else {
            val et = Tuple(u)
            val v = Variable.fresh(TTupleType(u.map(_.tp) :+ BagCType(t.tp)))
            Nest(E.get, w, et, t, v, Constant(true))
          }
        case (key, value @ Comprehension(e1, v, p, e)) :: tail =>
          val (nE, v2) = getNest(unnest(value)((w, w, E)))
          Bind(value, v2, unnest(Sng(Record(fs + (key -> v2))))((u, w :+ v2, nE)))
        case _ => ???
      }
    case c @ Constant(_) if !w.isEmpty =>
      assert(!E.isEmpty)
      if (u.isEmpty) Reduce(E.get, w, c, Constant(true))
      else Nest(E.get, w, Tuple(u), c, Variable.fresh(TTupleType(u.map(_.tp) :+ IntType)), Constant(true))
    case c @ Comprehension(e1 @ Project(e0, f), v, p, e) if !e0.tp.isInstanceOf[BagDictCType] && u.isEmpty && !w.isEmpty =>
      assert(!E.isEmpty)
      val nE = Some(Unnest(E.get, w, e1, v, p))
      unnest(e)((u, w :+ v, nE)) // C7
    case Comprehension(e1, v, p, e) if u.isEmpty && !w.isEmpty =>
      assert(!E.isEmpty) // TODO extract filters for join condition
      val p2s = p2(p, v, w)
      val nE = p2s._2 match { // ignore joins on top level labels
        //case InputRef(_, l @ LabelType(fs)) => Some(Select(e1, v, And(p1(p,v), Equals(p2s._2, p2s._1))))
        case _ => Some(Join(E.get, Select(e1, v, p1(p,v)), w, p2s._2, v, p2s._1))
      }
      unnest(e)((u, w :+ v, nE)) // C9
    case Comprehension(e1 @ Project(e0, f), v, p, e) if !e0.tp.isInstanceOf[BagDictCType] && !u.isEmpty && !w.isEmpty =>
      assert(!E.isEmpty)
      val nE = Some(OuterUnnest(E.get, w, e1, v, p))
      unnest(e)((u, w :+ v, nE)) // C10
    case Comprehension(e1, v, p, e) if !u.isEmpty && !w.isEmpty =>
      assert(!E.isEmpty)
      val p2s = p2(p, v, w)
      val nE = p2s._2 match { // ignore joins on top level labels
        //case InputRef(_, l @ LabelType(fs)) => Some(Select(e1, v, And(p1(p,v), Equals(p2s._2, p2s._1))))
        case _ => Some(OuterJoin(E.get, Select(e1, v, p1(p,v)), w, p2s._2, v, p2s._1))
      }
      unnest(e)((u, w :+ v, nE)) // C9
    case LinearCSet(exprs) => LinearCSet(exprs.map(unnest(_)((Nil, Nil, None))))
    case CNamed(n, exp) => exp match {
      case Sng(t) => CNamed(n, exp)
      case _ => CNamed(n, unnest(exp)((Nil, Nil, None)))
    }
    case _ => sys.error("not supported "+e)
  }

  def getNest(e: CExpr): (Option[CExpr], Variable) = e match {
    case Bind(nval, nv @ Variable(_,_), e1) => (Some(e), nv)
    case Nest(_,_,_,_,v2 @ Variable(_,_),_) => (Some(e), v2)
    case _ => ???
  }

  // filtering conditions
  def p1(e: CExpr, v: Variable): CExpr = e match {
    case Equals(e1, e2) if (e1 == v && e2 == v) => e
    case And(e1, e2) => And(p1(e1, v), p1(e2, v))
    case Not(e1) => p1(e1, v) match {
      case Constant(true) => Constant(true)
      case n => Not(n)
    }
    case Or(e1, e2) => Or(p1(e1, v), p1(e2, v))
    case Lt(e1, e2 @ Constant(_)) if (e1 == v) => e
    case Lt(e1 @ Constant(_), e2) if (e2 == v) => e
    case Gt(e1, e2 @ Constant(_)) if (e1 == v) => e
    case Gt(e1 @ Constant(_), e2) if (e2 == v) => e
    case Lte(e1, e2 @ Constant(_)) if (e1 == v) => e
    case Lte(e1 @ Constant(_), e2) if (e2 == v) => e
    case Gte(e1, e2 @ Constant(_)) if (e1 == v) => e
    case Gte(e1 @ Constant(_), e2) if (e2 == v) => e
    case _ => Constant(true) 
  }

  // join conditions
  def p2(e: CExpr, v: Variable, vs: List[Variable]): (CExpr, CExpr) = e match {
    case Equals(e1 @ InputRef(_, l @ LabelType(fs)), e2) if (e2 == v || lequals(e2, vs)) => (e2, e1)
    case Equals(e1, e2) if (lequals(e1, vs) && e2 == v) => (e1, e2)
    case Equals(e1, e2) if (lequals(e2, vs) && e1 == v) => (e2, e1)
    case Constant(true) => (Constant(true), Constant(true))
    case _ => sys.error("unsupported join condition "+e)
  }

  def lequals(e: CExpr, vs: List[Variable]): Boolean = vs.map(_.lequals(e)).contains(true)

}
