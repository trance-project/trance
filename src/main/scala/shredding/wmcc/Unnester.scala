package shredding.wmcc

import shredding.core._

/**
  * Unnesting algorithm from Fegaras and Maier
  * with extensions for dictionary lookups
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
    case Comprehension(e1 @ Comprehension(_, _, _, _), v, p, e) if !w.isEmpty => // C11 (relaxed)
      val (nE, v2) = getNest(unnest(e1)((w, w, E)))
      unnest(e)((u, w:+v2, nE))
    case ift @ If(cond, Sng(t @ Record(fs)), None) if !w.isEmpty => // C12, C5, and C8
      assert(!E.isEmpty)
      fs.filter(f => f._2.isInstanceOf[Comprehension]).toList match {
        case Nil =>
          if (u.isEmpty) Reduce(E.get, w, t, cond)
          else {
            val et = Tuple(u)
            val gt = Tuple((w.toSet -- u).toList)
            val v = Variable.fresh(TTupleType(et.tp.attrTps :+ BagCType(t.tp)))
            Nest(E.get, w, et, t, v, cond, gt)
          }
        case (key, value @ Comprehension(e1, v, p, e)) :: tail =>
          val (nE, v2) = getNest(unnest(value)((w, w, E)))
          unnest(If(cond, Sng(Record(fs + (key -> v2))), None))((u, w :+ v2, nE))
        case _ => sys.error("not supported")
      }
    case s @ Sng(v @ Variable(_,_)) if !w.isEmpty =>
      assert(!E.isEmpty)
      if (u.isEmpty) Reduce(E.get, w, v, Constant(true))
      else {
        val et = Tuple(u)
        val gt = Tuple((w.toSet -- u).toList)
        val v2 = Variable.fresh(TTupleType(et.tp.attrTps :+ BagCType(v.tp)))
        Nest(E.get, w, et, v, v2, Constant(true), gt)
      }
    case s @ Sng(t @ Record(fs)) if !w.isEmpty =>
      assert(!E.isEmpty)
      fs.filter(f => f._2.isInstanceOf[Comprehension]).toList match {
        case Nil =>
          if (u.isEmpty) Reduce(E.get, w, t, Constant(true))
          else {
            val et = Tuple(u)
            val gt = Tuple((w.toSet -- u).toList)
            val v = Variable.fresh(TTupleType(et.tp.attrTps :+ BagCType(t.tp)))
            Nest(E.get, w, et, t, v, Constant(true), gt)
          }
        case (key, value @ Comprehension(e1, v, p, e)) :: tail =>
          val (nE, v2) = getNest(unnest(value)((w, w, E)))
          unnest(Sng(Record(fs + (key -> v2))))((u, w :+ v2, nE))
        case _ => sys.error("not supported")
     }
    case c @ Comprehension(e1 @ Project(e0, f), v, p, e) if !e0.tp.isInstanceOf[BagDictCType] && !w.isEmpty =>
      assert(!E.isEmpty)
      getPM(p) match {
        case (Constant(false), _) => u.isEmpty match {
          case true => unnest(e)((u, w :+ v, Some(Unnest(E.get, w, e1, v, p))))
          case _ => unnest(e)((u, w :+ v, Some(OuterUnnest(E.get, w, e1, v, p))))
        }
        case (e2, be2) => 
          val nE = Some(OuterUnnest(E.get, w, e1, v, Constant(true))) // C11
          val (nE2, nv) = getNest(unnest(e2)((w :+ v, w :+ v, nE))) 
          unnest(e)((u, w :+ nv, nE2)) match {
            case Nest(e3, w3, f3, t3, v3, p3, g3) => Nest(e3, w3, f3, t3, nv, be2(nv), g3) 
            case res => res
          }
      }
    case Comprehension(e1 @ WeightedSng(_, _), v, p, e) if !w.isEmpty =>
      assert(!E.isEmpty)
      val nE = u.isEmpty match {
        case true => Some(Unnest(E.get, w, e1, v, p)) //C7
        case _ => Some(OuterUnnest(E.get, w, e1, v, p)) //C10
      }
      unnest(e)((u, w :+ v, nE))
   case Comprehension(e1 @ Project(e0, f), v, p @ Equals(lbl1, lbl2), Comprehension(e2, v2, p2, e3)) if e0.tp.isInstanceOf[BagDictCType] && !w.isEmpty =>
      assert(!E.isEmpty)
      getPM(p2) match {
        case (Constant(false), _) => 
          // p1s are from the context
          val (sp2s, p1s, p2s) = ps(p2, v2, w)
          val nE = e1 match {
            // top level case
             case Project(InputRef(name, BagDictCType(_,_)), "_1") if name.endsWith("__D") => 
               Some(Join(E.get, Select(e1, v2, sp2s), w, p1s, v2, p2s))  
             case _ => if (u.isEmpty) {
               Some(Lookup(E.get, Select(e1, v, sp2s), w, lbl1, v2, p2s, p1s))
             }else{
              Some(OuterLookup(E.get, Select(e1, v, sp2s), w, lbl1, v2, p2s, p1s))
             }
          }
          unnest(e3)((u, w :+ v2, nE)) 
        case (e4, be2) => 
          val nE = Some(OuterLookup(E.get, Select(e1, v2, Constant(true)), w, lbl1, v2, Constant(true), Constant(true)))
          val (nE2, nv) = getNest(unnest(e4)((w :+ v2, w :+ v2, nE)))
          unnest(e3)((u, w :+ nv, nE2)) match {
            case Nest(e4, w4, f4, t4, v4, p4, g3) => Nest(e4, w4, f4, t4, nv, be2(nv), g3)
            case res => res
          }
      }
    case Comprehension(e1, v, p, e) if !w.isEmpty =>
      val preds = ps(p, v, w)
      assert(!E.isEmpty)
      getPM(preds._1) match {
        case (Constant(false), _) => 
          val preds = ps(p, v, w)
          u.isEmpty match {
            case true => unnest(e)((u, w :+ v, Some(Join(E.get, Select(e1, v, preds._1), w, preds._2, v, preds._3))))
            case _ => unnest(e)((u, w :+ v, Some(OuterJoin(E.get, Select(e1, v, preds._1), w, preds._2, v, preds._3))))
          }
        case (e2, be2) => 
          val nE = Some(OuterJoin(E.get, Select(e1, v, Constant(true)), w, preds._2, v, preds._3)) // C11
          val (nE2, nv) = getNest(unnest(e2)((w :+ v, w :+ v, nE))) 
          unnest(e)((u, w :+ nv, nE2)) match {
            case Nest(e3, w3, f3, t3, v3, p3, g3) => Nest(e3, w3, f3, t3, nv, be2(nv), g3) 
            case res => res
          }
      }
    case c if (!w.isEmpty && c.tp.isInstanceOf[PrimitiveType]) =>
      assert(!E.isEmpty)
      if (u.isEmpty) Reduce(E.get, w, c, Constant(true))
      else {
        val et = Tuple(u)
        val gt = Tuple((w.toSet -- u).toList)
        Nest(E.get, w, et, c, Variable.fresh(TTupleType(et.tp.attrTps :+ IntType)), Constant(true), gt)
      }
    case LinearCSet(exprs) => LinearCSet(exprs.map(unnest(_)((Nil, Nil, None))))
    case CNamed(n, exp) => exp match {
      case Sng(Record(lbl)) => CNamed(n, exp)
      case _ => CNamed(n, unnest(exp)((Nil, Nil, None)))
    }
    case Bind(e1, e2, e3) => Bind(e1, e2, unnest(e3)((u, w, E)))
    case _ => sys.error(s"not supported $e")
  }

  def getNest(e: CExpr): (Option[CExpr], Variable) = e match {
    case Bind(nval, nv @ Variable(_,_), e1) => (Some(e), nv)
    case Nest(_,_,_,_,v2 @ Variable(_,_),_,_) => (Some(e), v2)
    case _ => sys.error(s"not supported $e")
  }


  // p1, p2, p3 extraction 

  def andToList(e: CExpr): List[CExpr] = e match {
    case And(e1, e2) => andToList(e1) ++ andToList(e2)
    case Or(e1, e2) => ???
    case _ => List(e)
  }

  def listToAnd(e: List[CExpr]): CExpr = e match {
    case Nil => Constant(true)
    case tail :: Nil => tail
    case head :: tail => And(head, listToAnd(tail))
  }

  def getP1(e: CExpr, v: Variable): Boolean = e match {
    case Equals(e1, e2) if (v.lequals(e1) == v.lequals(e2)) => true
    case Equals(e1, e2 @ Constant(_)) if v.lequals(e1) => true
    case Equals(e1 @ Constant(_), e2) if v.lequals(e2) => true
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
    val preds21 = listToExpr(preds2.filter(e2 => lequals(e2, vs)))
    val preds22 = listToExpr(preds2.filter(e2 => v.lequals(e2)))
    (listToAnd(p1s), preds21, preds22)
  }

  def lequals(e: CExpr, vs: List[Variable]): Boolean = vs.map(_.lequals(e)).contains(true)
  
  def getPM(e: CExpr): (CExpr, Variable => CExpr) = e match {
    case Equals(e1 @ Comprehension(_,_,_,_), e2) => (e1, (v: Variable) => Equals(v, e2))
    case Equals(e1, e2 @ Comprehension(_,_,_,_)) => (e2, (v: Variable) => Equals(e1, v))
    case Gt(e1 @ Comprehension(_,_,_,_), e2) => (e1, (v: Variable) => Gt(v, e2))
    case Gt(e1, e2 @ Comprehension(_,_,_,_)) => (e2, (v: Variable) => Gt(e1, v))
    case Gte(e1 @ Comprehension(_,_,_,_), e2) => (e1, (v: Variable) => Gte(v, e2))
    case Gte(e1, e2 @ Comprehension(_,_,_,_)) => (e2, (v: Variable) => Gte(e1, v))
    case Lt(e1 @ Comprehension(_,_,_,_), e2) => (e1, (v: Variable) => Lt(v, e2))
    case Lt(e1, e2 @ Comprehension(_,_,_,_)) => (e2, (v: Variable) => Lt(e1, v))
    case Lte(e1 @ Comprehension(_,_,_,_), e2) => (e1, (v: Variable) => Lte(v, e2))
    case Lte(e1, e2 @ Comprehension(_,_,_,_)) => (e2, (v: Variable) => Lte(e1, v))
    case And(e1, e2) => getPM(e1) match { // ???
      case (Constant(false), _) => getPM(e2) match {
        case (pm, be) => (pm, (v: Variable) => And(e1, be(v)))
        case _ => (Constant(false), (v: Variable) => Constant(true))
      }
      case (pm, be) => (pm, (v: Variable) => And(be(v), e2))
      case _ => (Constant(false), (v: Variable) => Constant(true))
    }
    case Or(e1, e2) => ???
    case Not(e1) => ???
    case If(c, e1, _) => getPM(c) match {
      case (Constant(false), _) => (Constant(false), (v: Variable) => Constant(true))
      case (pm, bp) => (pm, bp)
    } 
    case _ => (Constant(false), (v: Variable) => Constant(true))
  }




}
