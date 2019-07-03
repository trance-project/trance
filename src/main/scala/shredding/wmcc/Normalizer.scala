package shredding.wmcc

import shredding.core._

/**
  * Normalization rules for WMCC 
  */

trait BaseNormalizer extends BaseCompiler {

  // reduce conditionals
  override def equals(e1: Rep, e2: Rep): Rep = (e1, e2) match {
    case (Constant(x1), Constant(x2)) => constant(x1 == x2)
    case _ => super.equals(e1, e2)
  }

  override def not(e1: Rep): Rep = e1 match {
    case Constant(true) => super.constant(false)
    case Constant(false) => super.constant(true)
    case _ => super.not(e1)
  }

  override def and(e1: Rep, e2: Rep): Rep  = (e1, e2) match {
    case (Constant(true), Constant(true)) => super.constant(true)
    case (Constant(false), _) => super.constant(false)
    case (_, Constant(false)) => super.constant(false)
    case (Constant(true), e3) => e3
    case (e3, Constant(true)) => e3
    case _ => super.and(e1, e2)
  }

  override def or(e1: Rep, e2: Rep): Rep  = (e1, e2) match {
    case (Constant(false), Constant(false)) => super.constant(false)
    case (Constant(true), _) => super.constant(true)
    case (_, Constant(true)) => super.constant(true)
    case _ => super.and(e1, e2)
  }

  // N1, N2
  override def bind(e1: Rep, e: Rep => Rep): Rep = e(e1)

  // N3 (a := e1, b: = e2).a = e1
  override def project(e1: Rep, f: String): Rep = e1 match {
    case t:Tuple => t(f.toInt)
    case t:Record => t(f)
    //case t:Label => t(f)
    case t:TupleCDict => t(f)
    case t:BagCDict => t(f)
    case _ => super.project(e1, f)
  }

  override def ifthen(cond: Rep, e1: Rep, e2: Option[Rep]): Rep = cond match {
    case Constant(true) => e1 match {
      case Sng(t) if t.tp.isInstanceOf[PrimitiveType] => t
      case _ => e1
    }
    case Constant(false) => e2 match {
      case Some(a @ Sng(t)) if t.tp.isInstanceOf[PrimitiveType] => t 
      case Some(a) => a 
      case _ => super.emptysng
    }
    case _ => e1 match {
      case Sng(t) if t.tp == IntType => super.ifthen(cond, t, Some(Constant(0)))
      case Sng(t) if t.tp == DoubleType => super.ifthen(cond, t, Some(Constant(0.0))) 
      case _ => super.ifthen(cond, e1, e2)
    }
  }

  override def lookup(lbl: Rep, dict: Rep): Rep = dict match {
    case BagCDict(lbl2, flat2, dict2) if (lbl2.tp == lbl.tp) => flat2
    case _ => super.lookup(lbl, dict)
  }

  // { e(v) | v <- e1, p(v) }
  // where fegaras and maier does: { e | q, v <- e1, s }
  // this has { { { e | s } | v <- e1 } | q }
  // the normalization rules reduce generators, which is why I match on e1
  // N10 is automatically handled in this representation
  override def comprehension(e1: Rep, p: Rep => Rep, e: Rep => Rep): Rep = {
    e1 match {
      case If(cond, e3, e4 @ Some(a)) => //N4
        If(cond, comprehension(e3, p, e), Some(comprehension(a, p, e)))
      case EmptySng => EmptySng // N5
      case Sng(t) => ifthen(p(t), Sng(e(t))) // N6
      case Merge(e1, e2) => Merge(comprehension(e1, p, e), comprehension(e2, p, e))  //N7
      case Variable(_,_) => ifthen(p(e1), Sng(e(e1))) // input relation
      case Comprehension(e2, v2, p2, e3) => e3 match {
        // weighted singleton used for count
        // { 1 | v <- { WeightedSng(t,q) | v2 <- e2, p2}, p(v) }
        // { if (p(v)) { q } else { 0 } | v2 <- e2, p2, v := t, ... }
        // need to sum if more than one weighted sng is in a comprehension
        case WeightedSng(t, q) if (e(e3) == Constant(1)) =>
          val c2 = comprehension(Sng(t), p, (i: CExpr) => q) match {
            case Comprehension(a,b,c,Constant(1)) => Comprehension(a, b, c, q)
            case c3 => c3
          } 
          Comprehension(e2, v2, p2, c2)
        //N8
        // { e(v) | v <- { e3 | v2 <- e2, p2 }, p(v) }
        // { { e(v) | v <- e3 } | v2 <- e2, p2 }
        case _ => Comprehension(e2, v2, p2, comprehension(e3, p, e))
      }
      case c @ CLookup(flat, dict) =>
        val v1 = Variable.fresh(dict.tp.asInstanceOf[BagDictCType].flatTp.tp)
        val v2 = Variable.fresh(c.tp.tp)
        //Comprehension(Project(dict, "flat"), v1, and(equals(flat, Project(dict, "lbl")), p(v1)), e(v1))
        Comprehension(Project(dict, "_1"), v1, equals(flat, Project(v1, "_1")), 
          Comprehension(Project(v1, "_2"), v2, p(v2), e(v2)))
      case _ => // standard case (return self)
        val v = Variable.fresh(e1.tp.asInstanceOf[BagCType].tp)
        Comprehension(e1, v, p(v), e(v))
      }
    }

}
