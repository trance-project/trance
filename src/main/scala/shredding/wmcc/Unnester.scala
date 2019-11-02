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

  def isNestedComprehension(e: CExpr): Boolean = e match {
    case c:Comprehension => true
    case If(Equals(c:Comprehension, _), _, None) => true
    case d:CDeDup => true
    case g:CGroupBy => true
    case _ => false
  }

  def isDictType(e: Type): Boolean = e match { 
    case btp:BagDictCType => true
    case BagCType(TTupleType(fs)) => fs match {
      case (head @ IntType) :: (value @ BagCType(_)) :: Nil => true
      case _ => false 
    }
    case _ => false
  }

  def unnest(e: CExpr)(implicit ctx: Ctx): CExpr = e match {
    case CDeDup(e1) => CDeDup(unnest(e1)((u, w, E)))
    case CGroupBy(e1, v1, grp, value) => unnest(e1)(u, w, E) match {
      case Reduce(e2, v2, e3 @ Record(fs), p2 @ Constant(true)) => 
        val key = fs.dropRight(1).get("key") match {
          case Some(a) => a
          case _ => Record(fs.dropRight(1))
        }
        val value = fs.last._2 match { case Sng(t) => t; case t => t }
        val v = Variable.fresh(TTupleType(List(key.tp, value.tp)))
		    val g = Tuple(u ++ fs.dropRight(1).map(v => v._2 match { case Project(t, f) => t; case v3 => v3}).toList)
        Nest(e2, v2, key, value, v, p2, g)
      case n @ Nest(e2, v2, f2, e3 @ Record(fs), v3, p2 @ Constant(true), g) => 
        val key = Tuple(u :+ Record(fs.dropRight(1)))
        val value = fs.last._2 match { case Sng(t) => t; case t => t }
        val v = Variable.fresh(TTupleType(List(Record(fs.dropRight(1)).tp, value.tp)))
		    val g = Tuple(u ++ fs.dropRight(1).map(v => v._2 match { case Project(t, f) => t; case v3 => v3}).toList)
        Nest(e2, v2, key, value, v, p2, g)    
    }
    case Comprehension(e1, v, p, e2) if u.isEmpty && w.isEmpty && E.isEmpty =>
      val filt = p match {
        case Equals(InputRef(_, EmptyCType), _) => Constant(true)
        case _ => p
      }
      e match {
        // hack need to appropriately handle dictionary types
        case Comprehension(ed @ Project(InputRef(dict, dictTp), f), v, ps1, Comprehension(e2, v2, p2, e3)) if isDictType(ed.tp) =>
          val newTp = dictTp match { 
            case BagDictCType(BagCType(TTupleType(List(IntType, bagt))), _) => bagt
            case _ => sys.error("should not be transforming this")
          }
          unnest(e3)((u, w :+ v2, Some(Select(InputRef(dict+f, newTp), v2, p2, v2))))
        case _ =>
          unnest(e2)((Nil, List(v), Some(Select(e1, v, filt, v)))) // C4
      }
    case Comprehension(e1 @ Comprehension(_, _, _, _), v, p, e) if !w.isEmpty => // C11 (relaxed)
      val (nE, v2) = getNest(unnest(e1)((w, w, E)))
      unnest(e)((u, w:+v2, nE))
    case ift @ If(cond, Sng(t @ Record(fs)), None) if !w.isEmpty => // C12, C5, and C8
      assert(!E.isEmpty)
      fs.filter(f => isNestedComprehension(f._2)).toList match {
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
        case (key, CDeDup(value @ Comprehension(e1, v, p, e))) :: tail =>
          val (nE, v2) = getNest(unnest(value)((w, w, E)))
          unnest(If(cond, Sng(Record(fs + (key -> v2))), None))((u, w :+ v2, Some(CDeDup(nE.get))))
        case (key, value @ CGroupBy(e1, v1, grp, value2)) :: tail =>
	        val (nE, v2) = getNest(unnest(value)((w, w, E)))
          unnest(Sng(Record(fs + (key -> v2))))((u, w :+ v2, nE))
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
    case s @ Sng(t @ Record(fs)) if fs.keySet == Set("k", "v") && !w.isEmpty =>
      val (nE, v2) = fs.get("v").get match {    
        case value @ Comprehension(e1, v, p, e) => getNest(unnest(value)((w, w, E))) 
        case CDeDup(value @ Comprehension(e1, v, p, e)) => 
          val (nE1, v21) = getNest(unnest(value)((w, w, E)))
          (Some(CDeDup(nE1.get)), v21)
        case value @ CGroupBy(e1, v1, grp, value2) => getNest(unnest(value)((w, w, E)))
      }
      val vars = w :+ v2
      Reduce(nE.get, vars, Record(Map("k" -> vars.head, "v" -> Tuple(vars.tail))), Constant(true))
    case s @ Sng(t @ Record(fs)) if !w.isEmpty =>
      assert(!E.isEmpty)
      fs.filter(f => isNestedComprehension(f._2)).toList match {
        case Nil =>
          if (u.isEmpty) { 
            t match {
              case Record(ms) if ms.keySet == Set("k", "v") =>
                Reduce(E.get, w, Record(Map("k" -> w.head, "v" -> Tuple(w.tail))), Constant(true))
              case _ => Reduce(E.get, w, t, Constant(true)) 
            }
          }else {
            val et = Tuple(u)
            val gt = Tuple((w.toSet -- u).toList)
            val v = Variable.fresh(TTupleType(et.tp.attrTps :+ BagCType(t.tp))) 
            Nest(E.get, w, et, t, v, Constant(true), gt)
          }
        // address special case
        case (key, value @ If(Equals(c1:Comprehension, c2), x1, None)) :: tail => 
          val (nE, v2) = getNest(unnest(c2)((w, w, E)))
          unnest(Sng(Record(fs + (key -> If(Equals(v2, c2), x1, None)))))((u, w :+ v2, nE))
        case (key, value @ Comprehension(e1, v, p, e)) :: tail =>
	        val (nE, v2) = getNest(unnest(value)((w, w, E)))
          unnest(Sng(Record(fs + (key -> v2))))((u, w :+ v2, nE))
        case (key, CDeDup(value @ Comprehension(e1, v, p, e))) :: tail =>
	        val (nE, v2) = getNest(unnest(value)((w, w, E)))
          unnest(Sng(Record(fs + (key -> v2))))((u, w :+ v2, Some(CDeDup(nE.get))))
        case (key, value @ CGroupBy(e1, v1, grp, value2)) :: tail =>
	        val (nE, v2) = getNest(unnest(value)((w, w, E)))
          unnest(Sng(Record(fs + (key -> v2))))((u, w :+ v2, nE))
        case _ => sys.error("not supported")
     }
    case c @ Comprehension(e1 @ Project(e0, f), v, p, e) if !isDictType(e0.tp) && !w.isEmpty =>
      assert(!E.isEmpty)
      val (p1, p2) = ps1(p,v)
      getPM(p) match {
        case (Constant(false), _) => u.isEmpty match {
          case true => unnest(pushPredicate(e, p2))((u, w :+ v, Some(Unnest(E.get, w, e1, v, p1))))
          case _ => unnest(pushPredicate(e, p2))((u, w :+ v, Some(OuterUnnest(E.get, w, e1, v, p1))))
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
   case Comprehension(e1 @ Project(e0, f), v, ps1, Comprehension(e2, v2, p2, e3)) if isDictType(e0.tp) && !w.isEmpty =>
      assert(!E.isEmpty)
      // filters have been pushed from the previous comprehension
      val lbl1 = ps1 match {
        case Equals(l1, l2) => l1
        case And(Equals(l1, l2), l3) => l1
        case And(l3, Equals(l1, l2)) => l1
      }
      getPM(p2) match {
        case (Constant(false), _) => 
          // p1s are from the context
          val (sp2s, p1s, p2s) = ps(p2, v2, w)
          e1 match {
            // top level case
             case Project(InputRef(name, btp @ BagDictCType(_,_)), "_1") if name.endsWith("__D") => e2 match {
              //case Project(v1, "_2") if v1 == v => unnest(e3)((u, w :+ v2, Some(Select(e1, v2, p2, v2))))
              case _ =>
                val nE = Some(Join(E.get, Select(e1, v2, sp2s, v2), w, p1s, v2, p2s))  
                unnest(e3)((u, w :+ v2, nE))
             }
             case _ => if (u.isEmpty) {
               println("some lookup")
               println(e3)
               println(getPM(p2))
               val nE = Some(Lookup(E.get, Select(e1, v, sp2s, v2), w, lbl1, v2, Constant(true), Constant(true)))
	             unnest(pushPredicate(e3, p2))((u, w :+ v2, nE)) 
             }else{
               val nE = Some(OuterLookup(E.get, Select(e1, v, sp2s, v2), w, lbl1, v2, Constant(true), Constant(true)))
				 unnest(pushPredicate(e3, p2))((u, w :+ v2, nE))      
	           }
          }
	      case (e4, be2) => 
          val nE = Some(OuterLookup(E.get, Select(e1, v2, Constant(true), v2), w, lbl1, v2, Constant(true), Constant(true)))
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
            case true => unnest(e)((u, w :+ v, Some(Join(E.get, Select(e1, v, preds._1, v), w, preds._2, v, preds._3))))
            case _ => unnest(e)((u, w :+ v, Some(OuterJoin(E.get, Select(e1, v, preds._1, v), w, preds._2, v, preds._3))))
          }
        case (e2, be2) => 
          val nE = Some(OuterJoin(E.get, Select(e1, v, Constant(true), v), w, preds._2, v, preds._3)) // C11
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
      case Record(lbl) => CNamed(n, exp)
      case Sng(Record(lbl)) => CNamed(n, exp)
      case _ => CNamed(n, unnest(exp)((Nil, Nil, None)))
    }
    case Bind(e1, e2, e3) => Bind(e1, e2, unnest(e3)((u, w, E)))
    case _ => sys.error(s"not supported $e \n $w")
  }

  def pushPredicate(e: CExpr, p: CExpr): CExpr = (e, p) match {
    case (_, Constant(true)) => e
    case (Comprehension(e1, v1, p1, e2), _) => Comprehension(e1, v1, And(p, p1), e2)
    case _ => If(p, e, None)
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

  def ps1(e: CExpr, v: Variable): (CExpr, CExpr) = {
    val preds = andToList(e)
    val p1s = preds.filter(e2 => getP1(e2, v))
    val ps2 = preds.filterNot(p1s.contains(_))
    (listToAnd(p1s), listToAnd(ps2))
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
    case Or(e1 @ Comprehension(_,_,_,_), e2) => (e1, (v: Variable) => Or(v, e2))
    case Or(e1, e2 @ Comprehension(_,_,_,_)) => (e1, (v: Variable) => Or(e1, v))
    case Not(e1 @ Comprehension(_,_,_,_)) => (e1, (v: Variable) => Not(v))
    case If(c, e1, _) => getPM(c) match {
      case (Constant(false), _) => (Constant(false), (v: Variable) => Constant(true))
      case (pm, bp) => (pm, bp)
    } 
    case _ => (Constant(false), (v: Variable) => Constant(true))
  }




}
