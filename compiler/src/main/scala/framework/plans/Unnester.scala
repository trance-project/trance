package framework.plans

import framework.common._

/** Object that houses the variant of the unnesting algorithm from Fegaras and Maier,
  * includes extensions for intermediate NRC expressions from the shredding transformation 
  *
  * This uses a tuple-at-a-time streaming process.
  */
object Unnester {
  
  type Ctx = (List[Variable], List[Variable], Option[CExpr])
  @inline def u(implicit ctx: Ctx): List[Variable] = ctx._1
  @inline def w(implicit ctx: Ctx): List[Variable] = ctx._2
  @inline def E(implicit ctx: Ctx): Option[CExpr] = ctx._3

  /**
    * Variant of the unnesting algorithm that supports our NRC and a subset of the
    * intermediate NRC for shredding (ie. dictionaries and lookups)
    * 
    * @param e normalized comprehension 
    * @returns plan or sequence of plans 
    */
  def unnest(e: CExpr)(implicit ctx: Ctx): CExpr = e match {

    /** Base case for unnesting algorithm: Rule C4 **/
    case Comprehension(e1, v, p, e2) if u.isEmpty && w.isEmpty && E.isEmpty =>
      unnest(e2)((Nil, List(v), Some(Select(e1, v, p, v))))

    /** Comprehension in the body of another comprehension: Rule C11, relaxed **/
    case Comprehension(e1 @ Comprehension(_, _, _, _), v, p, e) if !w.isEmpty => 
      val (nE, v2) = getNest(unnest(e1)((w, w, E)))
      unnest(e)((u, w:+v2, nE))

    /** UNNEST / OUTERUNNEST: Rule C7 and C10 **/
    case c @ Comprehension(e1 @ Project(e0, f), v, p, e) if !w.isEmpty =>
      assert(!E.isEmpty)

      // push predicates that do not reference v into downstream operations
      // remaining comprehension {e | r, p[\overline{v}]}
      def pushPredicate(e: CExpr, p: CExpr): CExpr = (e, p) match {
        case (_, Constant(true)) => e
        case (Comprehension(e1, v1, p1, e2), _) => Comprehension(e1, v1, And(p, p1), e2)
        case _ => If(p, e, None)
      }

      val (p1, p2) = ps1(p,v)

      if (u.isEmpty) unnest(pushPredicate(e, p2))((u, w :+ v, Some(Unnest(E.get, w, e1, v, p1, v))))
      else unnest(pushPredicate(e, p2))((u, w :+ v, Some(OuterUnnest(E.get, w, e1, v, p1, v))))

    /** LOOKUP: Unnesting rule for shredding extensions of intermediate NRC **/
    case Comprehension(lu @ CLookup(lbl, dict), v, p, e2) if w.isEmpty =>
      unnest(e2)((u, w :+ v, Some(Select(Project(dict, "_1"), v, p, v))))

    case Comprehension(lu @ CLookup(lbl, dict), v, p, e2) =>
      val (sp2s, p1s, p2s) = ps(p, v, w)
      unnest(e2)((u, w :+ v, Some(Lookup(E.get, Project(dict, "_1"), w, lbl, v, p2s, p1s))))

    /** JOIN / OUTERJOIN: Rule C6 and C9 **/
    case Comprehension(e1, v, p, e) if !w.isEmpty =>
      assert(!E.isEmpty)

      val preds = ps(p, v, w)
      val right = Select(e1, v, preds._1, v)

      if (u.isEmpty) unnest(e)((u, w :+ v, Some(Join(E.get, right, w, preds._2, v, preds._3, Tuple(w), v))))
      else unnest(e)((u, w :+ v, Some(OuterJoin(E.get, right, w, preds._2, v, preds._3, Tuple(w), v))))

    case CGroupBy(e1, v1, keys, values) => CGroupBy(unnest(e1)((u, w, E)), v1, keys, values)

    case CReduceBy(e1, v1, keys, values) => unnest(e1)((u, w, E)) match {
      // BUG FIX: this should produce two nest operations
      case n @ Nest(e2, v2, fs, Record(e3), v3, p2, gs, _) =>
        val adjustNest = Reduce(e2, v2, Record(Map("_1" -> fs) ++ e3), Constant("null"))
        val nv = Variable.freshFromBag(adjustNest.tp)
        CReduceBy(adjustNest, nv, "_1" +: keys, values)
      case ne1 => CReduceBy(ne1, v1, keys, values)
    }

    /** 
      * The next several cases handle the base cases.
      * Terminate once all new nesting levels have been handled: RULE C5, C8, C12 
      **/
    case ift @ If(cond, Sng(t @ Record(fs)), None) if !w.isEmpty => 
      assert(!E.isEmpty)

      fs.find(f => isNestedComprehension(f._2)) match {
        case None =>
          if (u.isEmpty) Reduce(E.get, w, t, cond)
          else {
            val et = Tuple(u)
            val gt = Tuple((w.toSet -- u).toList)
            val v = Variable.fresh(TTupleType(et.tp.attrTps :+ BagCType(t.tp)))
            Nest(E.get, w, et, t, v, cond, gt, false)
          }
        case Some((key, value)) =>
          val (nested, nE, v2) = finalizeNest(value, w, E)
          unnest(If(cond, Sng(Record(fs + (key -> nested))), None))((u, w :+ v2, nE))
      }

    case s @ Sng(t @ Record(fs)) if !w.isEmpty =>
      assert(!E.isEmpty)
      fs.find(f => isNestedComprehension(f._2)) match {
        case None =>
          if (u.isEmpty) Reduce(E.get, w, t, Constant(true)) 
          else {
            val et = Tuple(u)
            val v = Variable.fresh(TTupleType(et.tp.attrTps :+ BagCType(t.tp))) 
            val (dk, nulls) = if (u.head.tp.isDict) (true, CUnit)
            else (false, Tuple((w.toSet -- u).toList))
            Nest(E.get, w, et, t, v, Constant(true), nulls, dk)
          }
        case Some((key, value)) =>
          val (nested, nE, v2) = finalizeNest(value, w, E)
          unnest(Sng(Record(fs + (key -> nested))))((u, w :+ v2, nE))
     }

    case CDeDup(e1) => CDeDup(unnest(e1)((u, w, E)))
    case FlatDict(e1) => FlatDict(unnest(e1)((u, w, E)))
    case GroupDict(e1) => GroupDict(unnest(e1)((u, w, E)))
    case LinearCSet(exprs) => LinearCSet(exprs.map(unnest(_)((Nil, Nil, None))))
    case CNamed(n, exp) => exp match {
      case Record(lbl) => CNamed(n, exp)
      case Sng(Record(lbl)) => CNamed(n, exp)
      case _ => CNamed(n, unnest(exp)((Nil, Nil, None)))
    }
    case InputRef(_,_) => e
    case Record(fs) => unnest(Sng(Record(fs)))((u, w, E))
    case Bind(e1, e2, e3) => Bind(e1, e2, unnest(e3)((u, w, E)))
    case _ => sys.error(s"not supported ${Printer.quote(e)} \n $w")
  }


  /** Non-root calls to unnest from within a tuple expression 
    *
    * @param value nested bag expression
    * @param set of variables collected prior to this nested expression
    * @param e subplan produced prior to this nested expression
    *
    * @return a triple containing i) the updated nested expression referencing the variable corresponding 
    * to the subplan, ii) the subplan, iii) the varible referencing the subplan.
    */
  private def finalizeNest(value: CExpr, uw: List[Variable], e: Option[CExpr]): (CExpr, Option[CExpr], Variable) = value match {
    case If(Equals(c1:Comprehension, c2), x1, None) => 
      val (nE, v2) = getNest(unnest(c1)((uw, uw, e)))
      (If(Equals(v2, c2), x1, None), nE, v2)
    case _:Comprehension =>
      val (nE, v2) = getNest(unnest(value)((uw, uw, e)))
      (v2, nE, v2)
    case CDeDup(value:Comprehension) =>
      val (nE, v2) = getNest(unnest(value)((uw, uw, e)))
      (v2, Some(CDeDup(nE.get)), v2)
    case _:CombineOp =>
      val (nE, v2) = getNest(unnest(value)((uw, uw, e)))
      (v2, nE, v2)
    case CLookup(lbl, dict) => 
      val v2 = Variable.freshFromBag(dict.tp)
      val nE = Some(Lookup(e.get, dict, uw, lbl, v2, Constant(true), v2))
      (v2, nE, v2)
    case _ => sys.error(s"not supported ${Printer.quote(value)}")
  }

  
  /** Utility for extracting information from nest that has come from the 
    * previous level.
    *
    * @param e the subplan that has come out of a recursive call to unnest
    * @return the subplan and variable from the top level operator
    */
  private def getNest(e: CExpr): (Option[CExpr], Variable) = e match {
    case Bind(nval, nv @ Variable(_,_), e1) => (Some(e), nv)
    case Nest(_,_,_,_,v2 @ Variable(_,_),_,_,_) => (Some(e), v2)
    case Reduce(lu @ Lookup(e1,e2,v1,p1, v2 @ Variable(_,_),p2,_),v3,p3,p4) => 
      val v3 = Variable.fresh(e.tp.asInstanceOf[BagCType].tp)
      (Some(e), v3)
    case CReduceBy(e1, v1, keys, values) => (Some(e), v1)
    case Reduce(e1, v1, e2, p2) => 
      val v3 = Variable.fresh(e.tp.asInstanceOf[BagCType].tp)
      (Some(e), v3)
    case Lookup(_,_,_,_, v2 @ Variable(n,tp),_,_) => (Some(e), Variable(n, BagCType(tp)))
    case _ => sys.error(s"not supported $e")
  }


  /** Utility for determining if a tuple contains a nested comprehension. 
    * 
    * @param e tuple attribute expression
    * @return true if there is a nested comprehension
    */
  private def isNestedComprehension(e: CExpr): Boolean = e match {
    case Record(fs) => fs.filter(c => isNestedComprehension(c._2)).nonEmpty
    case Variable(_, BagCType(_)) => false
    case c:Comprehension if c.tp.isInstanceOf[PrimitiveType] => true
    case Project(_,_) => false
    case _ => e.tp.isInstanceOf[BagCType] || e.tp.isInstanceOf[BagDictCType]
  }


  /** Utility for splitting predicates: e1 && e2 => List(e1, e2)
    * 
    * @param predicate to split
    * @return list representation of a predicate expression
    */
  private def andToList(e: CExpr): List[CExpr] = e match {
    case And(e1, e2) => andToList(e1) ++ andToList(e2)
    case Or(e1, e2) => ???
    case _ => List(e)
  }

  /** Utility for reconstructing predicates from a list: List(e1, e2) => e1 && e2
    *
    * @param e list of previously split predicates
    * @return And expression corresponding to a list of predicates
    */
  private def listToAnd(e: List[CExpr]): CExpr = e match {
    case Nil => Constant(true)
    case tail :: Nil => tail
    case head :: tail => And(head, listToAnd(tail))
  }

  /** Utility for extracting predicates referencing the current variable, ie. p[v] 
    * 
    * @param e predicate expression
    * @param v: current variable
    * 
    * @return true if predicate (e) references input variable (v)
    */
  private def getP1(e: CExpr, v: Variable): Boolean = e match {
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

  private def toList(e: CExpr): List[CExpr] = e match {
    case And(e1, e2) => toList(e1) ++ toList(e2)
    case Equals(e1, e2) => toList(e1) ++ toList(e2)
    case _ => List(e)
  }

  private def listToExpr(e: List[CExpr]): CExpr = e match {
    case Nil => Constant(true)
    case tail :: Nil => tail
    case head :: tail => Tuple(e)
  }

  private def ps1(e: CExpr, v: Variable): (CExpr, CExpr) = {
    val preds = andToList(e)
    val p1s = preds.filter(e2 => getP1(e2, v))
    val ps2 = preds.filterNot(p1s.contains(_))
    (listToAnd(p1s), listToAnd(ps2))
  }

  /** Determines predicates referencing current variables, predicates referencing both current 
    * variable and variables in w, and predicates in neither 
    *
    * @param e predicate expressoin
    * @param v current variable
    * @param vs set of variables up to this point in the plan (w)
    * 
    * @return the predicate split into three parts (p[v], p[(w,v)], p[\overline{(w,v)}]))
    */
  private def ps(e: CExpr, v: Variable, vs:List[Variable]): (CExpr, CExpr, CExpr) = {
    val preds = andToList(e)
    val p1s = preds.filter(e2 => getP1(e2, v))
    val preds2 = toList(listToAnd(preds.filterNot(p1s.contains(_))))
    val preds21 = listToExpr(preds2.filter(e2 => lequals(e2, vs)))
    val preds22 = listToExpr(preds2.filter(e2 => v.lequals(e2)))
    (listToAnd(p1s), preds21, preds22)
  }

  private def lequals(e: CExpr, vs: List[Variable]): Boolean = vs.map(_.lequals(e)).contains(true)

  /** Utility for extracting primitvie monoids (Rule C11)
    * ie. when a comprehension is used within a condition
    *
    * @deprecated Support removed when primitive monoids were reflecting 
    *   local operations for distributed plans.
    */
  private def getPM(e: CExpr): (CExpr, Variable => CExpr) = e match {
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
