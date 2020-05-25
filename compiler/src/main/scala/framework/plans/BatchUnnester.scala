package framework.plans

import framework.common._


/**
  * This is a batch version of the unnesting procedure introduced in the paper. 
  * Rather than the tuple stream version of Fegaras and Maier, 
  * every operator returns an intermediate result.
  */
object BatchUnnester {
  
  type Ctx = (Map[String, Type], Map[String, Type], Option[CExpr])
  @inline def u(implicit ctx: Ctx): Map[String, Type] = ctx._1
  @inline def w(implicit ctx: Ctx): Map[String, Type] = ctx._2
  @inline def E(implicit ctx: Ctx): Option[CExpr] = ctx._3

  def isNestedComp(e: CExpr): Boolean = e match {
    case Variable(_, BagCType(_)) => false
    case c:Comprehension => true
    case c:CReduceBy => true
    case c:CGroupBy => true
    case _ => false
  }

  def isBaseNest(fs: Map[String, CExpr]): Boolean = 
    fs.find(f => f._2 match { case Project(_, "_2") => true; case _ => false}).nonEmpty

  val extensions = new Extensions{}
  import extensions._

  def flat(tp1: Map[String, Type], tp2: Type): Map[String, Type] =
    RecordCType(tp1).merge(tp2).attrs

  def wvar(mtp: Map[String, Type]): Variable = Variable.fresh(RecordCType(mtp))

  def getName(e: CExpr): String = e match {
    case InputRef(name, _) => name
    case FlatDict(e1) => getName(e1)
    case _ => ???
  }

  def unnest(e: CExpr)(implicit ctx: Ctx): CExpr = e match {

    /** Base case for unnesting algorithm: Rule C4 **/
    case Comprehension(e1, v, p, e2) if u.isEmpty && w.isEmpty && E.isEmpty =>
      val ne1 = AddIndex(e1, getName(e1)+"_index")
      val nv = Variable(v.name, ne1.tp.tp)
      unnest(e2)((u, nv.tp.attrs, Some(Select(ne1, nv, replace(p, nv), nv))))

    case c @ Comprehension(e1 @ Project(e0, f), v, p, e2) if !w.isEmpty =>
      assert(!E.isEmpty)

      if (u.isEmpty)
        unnest(e2)((u, flat((w - f), v.tp), Some(DFUnnest(E.get, wvar(w), f, v, p, Nil))))
      else{
        val ne1 = AddIndex(E.get, f+"_index")
        val nv = Variable(v.name, ne1.tp.tp)
        val nw = flat(w - f, nv.tp)
        val nE = DFOuterUnnest(ne1, wvar(nw), f, v, p, Nil)
        unnest(e2)((u - f), flat(nw - f, v.tp.outer), Some(nE))
      }

    case Comprehension(e1 @ CLookup(Project(_, p1), dict), v1, Constant(true), e2) => 
      assert(!E.isEmpty)
      val nE = DFOuterJoin(E.get, wvar(w), p1, dict, v1, "_1", Nil)
      unnest(e2)((u, flat(w, v1.tp), Some(nE)))

    case Comprehension(e1 @ InputRef(name, _), v, Equals(Project(_, p1), Project(_, p2)), e2) if !w.isEmpty =>
      assert(!E.isEmpty)
      val right = AddIndex(e1, name+"_index")
      val nv = Variable(v.name, right.tp.tp)
      val (nw, nE) = 
        if (u.isEmpty) (flat(w, nv.tp), DFJoin(E.get, wvar(w), p1, Select(right, nv, Constant(true), nv), nv, p2, Nil))
        else (flat(w, nv.tp.outer), DFOuterJoin(E.get, wvar(w), p1, Select(right, nv, Constant(true), nv), nv, p2, Nil))
      unnest(e2)((u, nw, Some(nE)))

    case s @ If(cnd, Sng(t @ Record(fs)), nextIf) =>
      handleLevel(fs, x => If(cnd, Sng(x), nextIf))((u, w, E))

    case s @ Sng(t @ Record(fs)) if !w.isEmpty =>
      handleLevel(fs, identity)((u, w, E))

    case CReduceBy(e1, v1, keys, values) => unnest(e1)((u, w, E)) match {
      case DFNest(e2, v2, keys2, values2, filter, nulls) =>
        // adjust input for reduce operation
        val pattern = replace(extendIf(keys2, values2, v2), v2)
        val reduceInput = DFProject(e2, v2, pattern, Nil)
        val nv1 = Variable(v2.name, reduceInput.tp)

        val crd = DFReduceBy(reduceInput, nv1, keys2 ++ keys, values)
        val nv2 = wvar(v2.tp.attrs ++ crd.tp.attrs)
        val nr = Record((keys ++ values).map(k => k -> Project(nv2, k)).toMap)
        DFNest(crd, nv2, keys2, nr, filter, nulls)

      case e2 => 
        DFReduceBy(e2, v1, keys, values)
    }

    case FlatDict(e1) => FlatDict(unnest(e1)((u, w, E)))
    case GroupDict(e1) => GroupDict(unnest(e1)((u, w, E)))
    case LinearCSet(exprs) => LinearCSet(exprs.map(unnest(_)((u, w, E))))
    case CNamed(n, exp) => CNamed(n, unnest(exp)((u, w, E)))
    case _ => e

  }

  def extendIf(ls: List[String], e: CExpr, v: Variable): CExpr = e match {
    case If(cond, Sng(Record(fs)), nextIf) => 
      val extendedAttrs = ls.map(l => l -> Project(v, l)).toMap
      val next = nextIf match { case Some(ni) => Some(extendIf(ls, ni, v)); case _ => None}
      If(cond, Sng(extendIf(ls, Record(fs), v)), next)
    case Record(ms) => 
      val extendedAttrs = ls.map(l => l -> Project(v, l)).toMap
      Record(extendedAttrs ++ ms)
    case _ => e
  }

  def handleLevel(fs: Map[String, CExpr], exp: CExpr => CExpr)(implicit ctx: Ctx): CExpr = {
   fs.find(c => isNestedComp(c._2)) match {
    case Some((key, value)) =>
      val nE = unnest(value)((w, w, E))
      val nv = Variable.freshFromBag(nE.tp)
      val nr = Record(fs.map(f => f._1 -> replace(f._2, nv)) + (key -> Project(nv, "_2")))
      val nw = w + (key -> nv.tp)
      unnest(exp(Sng(nr)))((u, nw, Some(nE)))
    case y if u.isEmpty => 
      DFProject(E.get, wvar(w), exp(Record(fs)), Nil)
    case _ => 
      DFNest(E.get, wvar(w), u.keys.toList, exp(Record(fs)), Constant(true), (w.keySet -- u.keySet).toList)
    }   
  }

}
