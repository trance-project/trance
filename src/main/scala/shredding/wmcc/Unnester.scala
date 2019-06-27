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
      unnest(e)((u, w:+v2, nE))
    case ift @ If(cond, Sng(t @ Record(fs)), None) if !w.isEmpty => // C12, C5, and C8
      assert(!E.isEmpty)
      fs.filter(f => f._2.isInstanceOf[Comprehension]).toList match {
        case Nil =>
          if (u.isEmpty) Reduce(E.get, w, t, cond)
          else {
            val et = Tuple(u)
            val v = Variable.fresh(TTupleType(et.tp.attrTps :+ BagCType(t.tp)))
            Nest(E.get, w, et, t, v, cond)
          }
        case (key, value @ Comprehension(e1, v, p, e)) :: tail =>
          val (nE, v2) = getNest(unnest(value)((w, w, E)))
          unnest(If(cond, Sng(Record(fs + (key -> v2))), None))((u, w :+ v2, nE))
        case _ => sys.error("unsupported")
      }
    case s @ Sng(t @ Record(fs)) if !w.isEmpty =>
      assert(!E.isEmpty)
      fs.filter(f => f._2.isInstanceOf[Comprehension]).toList match {
        case Nil =>
          if (u.isEmpty) Reduce(E.get, w, t, Constant(true))
          else {
            val et = Tuple(u)
            val v = Variable.fresh(TTupleType(et.tp.attrTps :+ BagCType(t.tp)))
            Nest(E.get, w, et, t, v, Constant(true))
          }
        case (key, value @ Comprehension(e1, v, p, e)) :: tail =>
          val (nE, v2) = getNest(unnest(value)((w, w, E)))
          unnest(Sng(Record(fs + (key -> v2))))((u, w :+ v2, nE))
        case _ => sys.error("unsupported")
      }
    case c @ Constant(_) if !w.isEmpty =>
      assert(!E.isEmpty)
      if (u.isEmpty) Reduce(E.get, w, c, Constant(true))
      else {
        val et = Tuple(u)
        Nest(E.get, w, et, c, Variable.fresh(TTupleType(et.tp.attrTps :+ IntType)), Constant(true))
      }
    case c @ Comprehension(e1 @ Project(e0, f), v, p, e) if !e0.tp.isInstanceOf[BagDictCType] && !w.isEmpty =>
      assert(!E.isEmpty)
      val nE = u.isEmpty match {
        case true => Some(Unnest(E.get, w, e1, v, p)) //C7
        case _ => Some(OuterUnnest(E.get, w, e1, v, p)) //C10
      }
      unnest(e)((u, w :+ v, nE))
    case Comprehension(e1 @ WeightedSng(_, _), v, p, e) if !w.isEmpty =>
      assert(!E.isEmpty)
      val nE = u.isEmpty match {
        case true => Some(Unnest(E.get, w, e1, v, p)) //C7
        case _ => Some(OuterUnnest(E.get, w, e1, v, p)) //C10
      }
      unnest(e)((u, w :+ v, nE))
    case Comprehension(e1, v, p, e) if !w.isEmpty =>
      assert(!E.isEmpty) 
      val preds = ps(p, v, w)
      val nE = u.isEmpty match {
        case true => Some(Join(E.get, Select(e1, v, preds._1), w, preds._2, v, preds._3)) //C6
        case _ => Some(OuterJoin(E.get, Select(e1, v, preds._1), w, preds._2, v, preds._3)) //C9
      }
      unnest(e)((u, w :+ v, nE))
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
    case _ => sys.error("unsupported")
  }

  // need to support ors
  def andToList(e: CExpr): List[CExpr] = e match {
    case And(e1, e2) => andToList(e1) ++ andToList(e2)
    case _ => List(e)
  }

  def listToAnd(e: List[CExpr]): CExpr = e match {
    case Nil => Constant(true)
    case tail :: Nil => tail
    case head :: tail => And(head, listToAnd(tail))
  }

  def getP1(e: CExpr, v: Variable): Boolean = e match {
    case Equals(e1, e2) if (v.lequals(e1) == v.lequals(e2)) => true
    case Lt(e1, e2 @ Constant(_)) if v.lequals(e1) => true
    case Lt(e1 @ Constant(_), e2) if v.lequals(e2) => true
    case Gt(e1, e2 @ Constant(_)) if v.lequals(e1) => true
    case Gt(e1 @ Constant(_), e2) if v.lequals(e2) => true
    case Lte(e1, e2 @ Constant(_)) if v.lequals(e1) => true
    case Lte(e1 @ Constant(_), e2) if v.lequals(e2) => true
    case Gte(e1, e2 @ Constant(_)) if v.lequals(e1) => true
    case Gte(e1 @ Constant(_), e2) if v.lequals(e2) => true
    case _ => false
  }

  def toList(e: CExpr): List[CExpr] = e match {
    case And(e1, e2) => toList(e1) ++ toList(e2)
    case Equals(e1, e2) => toList(e1) ++ toList(e2)
    case _ => List(e)
  }

  def listToExpr(e: List[CExpr]): CExpr = e match {
    case Nil => Constant(true)
    case tail :: Nil => tail
    case head :: tail => Tuple(e)
  }

  def ps(e: CExpr, v: Variable, vs:List[Variable]): (CExpr, CExpr, CExpr) = {
    val preds = andToList(e)
    val p1s = preds.filter(e2 => getP1(e2, v))
    val preds2 = toList(listToAnd(preds.filterNot(p1s.contains(_))))
    // p1, p2s for left, p2s for right
    (listToAnd(p1s), listToExpr(preds2.filter(e2 => lequals(e2, vs))), listToExpr(preds2.filter(e2 => v.lequals(e2))))
  }
   
  def lequals(e: CExpr, vs: List[Variable]): Boolean = vs.map(_.lequals(e)).contains(true)

}
