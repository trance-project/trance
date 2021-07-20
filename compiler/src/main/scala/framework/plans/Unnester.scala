package framework.plans

import framework.common._


/**
  * This is a batch version of the unnesting procedure introduced in the paper. 
  * Rather than the tuple stream version of Fegaras and Maier, 
  * every operator returns an intermediate result.
  * 
  */
object Unnester {
  
  // The general structure of the algorithm is [ { e | r } ]^{u}_{w} E
  // Here we define the follow as the full context (Ctx): u, w, E 
  // where u is the set of parent attributes when inside of an nest, 
  // and w is the set of attributes coming from the context E.
  type Ctx = (Map[String, Type], Map[String, Type], Option[CExpr], String)
  @inline def u(implicit ctx: Ctx): Map[String, Type] = ctx._1
  @inline def w(implicit ctx: Ctx): Map[String, Type] = ctx._2
  @inline def E(implicit ctx: Ctx): Option[CExpr] = ctx._3
  @inline def tag(implicit ctx: Ctx): String = ctx._4

  def isNestedComp(e: CExpr): Boolean = e match {
    case Variable(_, BagCType(_)) => false
    case c:Comprehension => true
    case c:CReduceBy => true
    case c:CGroupBy => true
    case c:CLookup => true
    case _ => false
  }

  val extensions = new Extensions{}
  val normalizer = new BaseNormalizer{}
  val normalize = new Finalizer(normalizer)
  import extensions._

  var varset = Set.empty[String]

  // since we use a set of attributes, and not a list of tupled variables 
  // like in fegaras and maier - this just flattens out all the attributes 
  // in the context into one tuple
  def flat(tp1: Map[String, Type], tp2: Type): Map[String, Type] =
    RecordCType(tp1).merge(tp2).attrs

  def wvar(mtp: Map[String, Type]): Variable = Variable.fresh(RecordCType(mtp))

  def getName(e: CExpr): String = e match {
    case InputRef(name, _) => name
    case FlatDict(e1) => getName(e1)
    case Variable(n, tp) => n
    case _ => Variable.fresh(StringType).name 
  }


  // This is the start of the unnest algorithm, you can see that 
  // Ctx is provided at each call.
  def unnest(e: CExpr)(implicit ctx: Ctx): CExpr = e match {

    // Base case for unnesting algorithm: Rule C4 
	  // But unlike in C4, in the topmost generator v <-- e1, e1 may not be a simple term, since we
	  // do not have as strong a normal form. So we need to unnest e1
    case Comprehension(e1, v, p, e2) if u.isEmpty && w.isEmpty && E.isEmpty =>
      // do not index a domain
      val nE = unnest(e1)((u, w, E, tag))
      val ne1 = getName(nE) match {
        case en if en.contains("Dom") => nE
        case en => AddIndex(nE, en+"_index")
      }
      val nv = Variable(v.name, ne1.tp.asInstanceOf[BagCType].tp)
      unnest(e2)((u, nv.tp.attrs, Some(Select(ne1, nv, replace(p, nv))), tag))

    // Case for Unnest operator (C7 and C10)
    // outer operator is generated when u is non empty 
    case c @ Comprehension(e1 @ Project(e0, f), v, p, e2) if !w.isEmpty =>
      assert(!E.isEmpty)

      // u is empty C7
      if (u.isEmpty){
        val nE = Unnest(E.get, wvar(w), f, v, p, Nil)
        unnest(e2)((u, flat((w - f), v.tp), Some(nE), tag))
      // u is nonEmpty C10
      }else{
        val ne1 = AddIndex(E.get, f+"_index")
        val nv = Variable(v.name, ne1.tp.tp)
        val nw = flat(w - f, nv.tp)
        val nE = OuterUnnest(ne1, wvar(nw), f, v, p, Nil)
        unnest(e2)((u - f), flat(nw - f, v.tp.outer), Some(nE), tag)
      }

    // Case of a lookup in the head
    case e1 @ CLookup(lbl, dict) => 
      assert(!E.isEmpty)
      val nv = Variable.fresh(e1.tp.tp)
      unnest(Comprehension(e1, nv, Constant(true), Sng(nv)))((u, w, E, tag))

    // case of a lookup as a generator
    // translates to outerjoin, and drops label attributes
    case Comprehension(CLookup(lbl, e1), v1, filt, e2) => 
      assert(!E.isEmpty)

      val dict = unnest(e1)((u, w, E, tag))
      val nv = wvar(w)
      val nv1 = Variable.freshFromBag(dict.tp)
      val pdict = Projection(dict, nv1, Record(nv1.tp.attrs.map(x => x._1 match {
        case "_1" =>  "LABEL_1" -> Project(nv1, x._1); case _ => x._1 -> Project(nv1, x._1)})), Nil)
      val nv2 = Variable.freshFromBag(pdict.tp)

      // capture join conditions and remove dropped attributes
      val joinCond = lbl match {
        case Project(e3, p1) => 
          val rev = normalize.finalize(replace(e3, nv, useType = false)).asInstanceOf[CExpr]
          Equals(Project(rev, p1), Project(nv2, "LABEL_1"))
        case _ => 
          Equals(lbl, Project(nv2, "LABEL_1"))
      }

      // generate the outer join and make recursive call
      val rfs = collect(joinCond)
      // val fdict = Select(dict, nv1, filt)
      val nE = OuterJoin(E.get, nv, pdict, nv2, joinCond, (w.keySet ++ v1.tp.attrs.keySet).toList)
      unnest(e2)((u, flat(w, nv2.tp), Some(nE), tag))

    // Deprecated: primitive monoid case, this was only relevant when we were using count 
    // and weighted singleton - all functionality is replaced by SumBy
    case Comprehension(e1:Comprehension, v, p, Constant(c)) => unnest(e1)((u, w, E, tag)) match {
      case Nest(e2, v2, keys2, values2, filt, nulls, ntag) => 
        Nest(e2, v2, keys2, Constant(c), values2, nulls, ntag)
      case _ => ???
    }

    // all sumBy handling
    // you first unnest the input (e1), then match the response - 
    case CReduceBy(e1, v1, keys, values) => unnest(e1)((u, w, E, tag)) match {
      // if Gamma is returned then this means that this SumBy was 
      // generated by some parent context: {(sumBy(e1) | r }
      case Nest(e2, v2, keys2, values2, filter, nulls, ctag) =>
        // adjust input for reduce operation
        val pattern = replace(extendIf(keys2, values2, v2), v2)
        val reduceInput = Projection(e2, v2, pattern, Nil)
        val nv1 = Variable(v2.name, reduceInput.tp)

        val crd = Reduce(reduceInput, nv1, keys2 ++ keys, values)
        val nv2 = wvar(v2.tp.attrs ++ crd.tp.attrs)
        val nr = Record((keys ++ values).map(k => k -> Project(nv2, k)).toMap)
        Nest(crd, nv2, keys2, nr, filter, nulls, ctag)
      
      // else the sumby was applied at the top-level 
      // of a comprehension: sumBy({ e | r })
      case e2 => E match {
        case Some(e3) => 
          val nkeys = (w.keySet ++ keys.toSet).toList
          Reduce(e2, v1, nkeys :+ "_1", values)
        case _ => Reduce(e2, v1, keys, values)
      }
    }

    // sumBy generator 
    // handles the same as the above case
    case Comprehension(e1:CReduceBy, v, cond, e2) if !w.isEmpty => 
      val nE = unnest(e1)((w, w, E, tag)) match {
        case Nest(e2:Reduce, _, _, _, _, _, _) => RemoveNulls(e2)
        case r:Reduce =>  RemoveNulls(r)
        case _ => ???
      }
      val nw = flat(w, nE.tp.asInstanceOf[BagCType].tp)
      unnest(e2)((u, nw, Some(nE), tag))

    // same as the sumBy handling, except catching dedup
    case CDeDup(e1) => unnest(e1)((u, w, E, tag)) match {
      // if it came from above then 
      case Nest(e2, v2, keys, values, filter, nulls, ctag) =>
        val indices = u.keySet.filter(x => x.contains("_index"))
        val keys2 = (indices ++ keys).toList
        CDeDup(Projection(e2, v2, replace(extendIf(keys2, values, v2), v2), Nil))
      case s =>  CDeDup(s)
    }

    // Dedup generator
    // handles the same as the above case
    case Comprehension(e1:CDeDup, v, cond, e2) if !w.isEmpty =>
      val nE = RemoveNulls(unnest(e1)((w, w, E, tag)))
      val nw = flat(w, nE.tp.asInstanceOf[BagCType].tp)
      unnest(liftFilter(cond, e2))((u, nw, Some(nE), tag))

    // Rule C6/C9: all other generator types translate to joins
    // inner or outer depending on if u is empty
    case Comprehension(e1, v, cond, e2) if !w.isEmpty =>
      assert(!E.isEmpty)
      val name = getName(e1)
      val right = AddIndex(unnest(e1)((u, w, E, tag)), name+"_index")
      val nv = Variable(v.name, right.tp.tp)

      // if u is empty generate inner join, if not then outer
	    val (nw, nE) = 
        if (u.isEmpty) (flat(w, nv.tp), Join(E.get, wvar(w), Select(right, nv, Constant(true)), nv, cond, Nil))
        else (flat(w, nv.tp.outer), OuterJoin(E.get, wvar(w), Select(right, nv, Constant(true)), nv, cond, Nil))
      unnest(e2)((u, nw, Some(nE), tag))

    // translate a groupBy directly into a Gamma
    case CGroupBy(e1, v1, keys, values, gname) => 
      val nE = unnest(e1)((u, w, E, gname)) 
      val nv = wvar(w)
      val tup = Record(values.map(v => v -> Project(nv, v)).toMap)
      Nest(nE, nv, keys, tup, Constant(true), (w.keySet -- u.keySet).toList, gname)

    // the next three cases are going into a new 
    // level - this will trigger rule C12
    // see handleLevel function below
    case s @ If(cnd, Sng(t @ Record(fs)), nextIf) =>
      handleLevel(fs, x => If(cnd, Sng(x), nextIf))((u, w, E, tag))

    case s @ Sng(t @ Record(fs)) if !w.isEmpty =>
      handleLevel(fs, identity)((u, w, E, tag))

    case Sng(v @ Variable(_, tp)) if !w.isEmpty => 
      val fs = tp.attrs.map(k => k._1 -> Project(v, k._1))
      handleLevel(fs, identity)((u, w, E, tag))

    // all these cases just pass through
    case Bind(x, e1, e2) => 
      Bind(x, unnest(e1)((u, w, E, tag)), unnest(e2)((u, w, E, tag)))
    case FlatDict(e1) => FlatDict(unnest(e1)((u, w, E, tag)))
    case GroupDict(e1) => GroupDict(unnest(e1)((u, w, E, tag)))
    case LinearCSet(exprs) => LinearCSet(exprs.map(unnest(_)((u, w, E, tag))))
    case CNamed(n, exp) => 
      CNamed(n, unnest(exp)((u, w, E, tag)))
    case _ => e

  }

  // organization of key and value attributes when there is a primitive if condition
  // return a record type or a record type with an if condition when necessary
	//in the intended application, e will be the head of a simple comprehension, ls a list of attributes
	//the goal is just to append v.l for l in ls to the head
  private def extendIf(ls: List[String], e: CExpr, v: Variable): CExpr = e match {
    case If(cond, Sng(Record(fs)), nextIf) => 
      val extendedAttrs = ls.map(l => l -> Project(v, l)).toMap
      val next = nextIf match { case Some(ni) => Some(extendIf(ls, ni, v)); case _ => None}
      If(cond, Sng(extendIf(ls, Record(fs), v)), next)
    case Record(ms) => 
      val extendedAttrs = ls.map(l => l -> Project(v, l)).toMap
      Record(extendedAttrs ++ ms)
    case _ => e
  }

  // the function that controls the actual unnesting, rule c12
  // when there is a comprehension in the head, this will capture 
  // it and apply the proper unnesting rule
  private def handleLevel(fs: Map[String, CExpr], exp: CExpr => CExpr)(implicit ctx: Ctx): CExpr = {
    // if there is a nested comprehension in the head then
   fs.find(c => isNestedComp(c._2)) match {
    // unnest it and then bind the value to variable v in the body
    case Some((key, value)) =>
      val nE = unnest(value)((w, w, E, key))
      val nv = Variable.freshFromBag(nE.tp)
      val nr = Record(fs.map(f => f._1 -> replace(f._2, nv)) + (key -> Project(nv, key)))
      val nw = w + (key -> nv.tp)
      unnest(exp(Sng(nr)))((u, nw, Some(nE), tag))
    // when u is empty this is just a projection C5
    case y if u.isEmpty => 
      Projection(E.get, wvar(w), exp(Record(fs)), w.keySet.toList)
    // when u is not empty and there are no other comprehensions in the 
    // head then rule C8
    case _ => 
      val nv = wvar(w)
      val tup = exp(Record(fs))
      val rtup = replace(tup, nv)
      Nest(E.get, nv, u.keys.toList, rtup, Constant(true), (w.keySet -- u.keySet).toList, tag)
    }  
  } 

  /**
   *  Extends the condition to the next step:
   *  ex: 
   * 
   *  input { (x.a, y.c) | y <- DeDup(Lookup(x.b, Y)), y.d < 2 )}^{()}_{w} E
   *  output { if (y.d < 2) (x.a, y.c) | }^{()}_{w, y} E  
   *
   **/
  private def liftFilter(f: CExpr, e: CExpr): CExpr = f match {
    case Constant(true) => e
    case _ => e match {
      case Comprehension(e1, v, p, e2) => Comprehension(e1, v, normalizer.and(f, p), e2)
      case If(cnd, e1, e2) => If(normalizer.and(f, cnd), e1, e2)
      case Sng(e1) => If(f, e1, None)
      case _ => sys.error(s"unsupported expression $e")
    } 
  }
}
