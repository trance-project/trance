package shredding.nrc

import shredding.core._

/**
  * Extension methods for NRC expressions
  */
trait Extensions {
  this: ShredNRC =>

  def collect[A](e: Expr, f: PartialFunction[Expr, List[A]]): List[A] =
    f.applyOrElse(e, (ex: Expr) => ex match {
      case p: Project => collect(p.tuple, f)
      case ForeachUnion(_, e1, e2) => collect(e1, f) ++ collect(e2, f)
      case Union(e1, e2) => collect(e1, f) ++ collect(e2, f)
      case Singleton(e1) => collect(e1, f)
      case Tuple(fs) => fs.flatMap(x => collect(x._2, f)).toList
      case l: Let => collect(l.e1, f) ++ collect(l.e2, f)
      case Total(e1) => collect(e1, f)
      case DeDup(e1) => collect(e1, f)
      case c: Cmp => collect(c.e1, f) ++ collect(c.e2, f)
      case And(e1, e2) => collect(e1, f) ++ collect(e2, f)
      case Or(e1, e2) => collect(e1, f) ++ collect(e2, f)
      case Not(e1) => collect(e1, f)
      case i: IfThenElse =>
        collect(i.cond, f) ++ collect(i.e1, f) ++ i.e2.map(collect(_, f)).getOrElse(Nil)
      case ArithmeticExpr(_, e1, e2) => collect(e1, f) ++ collect(e2, f)

      // Label extensions
      case el: ExtractLabel => collect(el.lbl, f) ++ collect(el.e, f)
      case NewLabel(pp) => pp.toList.flatMap(collect(_, f))
      case p: LabelParameter => collect(p.e, f)

      // Dictionary extensions
      case BagDict(l, b, d) => collect(l, f) ++ collect(b, f) ++ collect(d, f)
      case TupleDict(fs) => fs.flatMap(x => collect(x._2, f)).toList
      case BagDictProject(v, _) => collect(v, f)
      case TupleDictProject(d) => collect(d, f)
      case d: DictUnion => collect(d.dict1, f) ++ collect(d.dict2, f)

      // Shredding extensions
      case ShredUnion(e1, e2) => collect(e1, f) ++ collect(e2, f)
      case Lookup(l, d) => collect(l, f) ++ collect(d, f)

      /////////////////
      //
      //
      // UNSTABLE BELOW
      //
      //
      /////////////////

      case WeightedSingleton(e1, w1) => collect(e1, f) ++ collect(w1, f)
      case BagGroupBy(bag, _, _, _) => collect(bag, f)
      case PlusGroupBy(bag, _, _, _) => collect(bag, f)
      case _ => List()
    })

  def collectAssignment[A](a: Assignment, f: PartialFunction[Expr, List[A]]): List[A] =
    collect(a.rhs, f)

  def collectProgram[A](p: Program, f: PartialFunction[Expr, List[A]]): List[A] =
    p.statements.flatMap(collectAssignment(_, f))

  def replace(e: Expr, f: PartialFunction[Expr, Expr]): Expr =
    f.applyOrElse(e, (ex: Expr) => ex match {
      case p: Project =>
        replace(p.tuple, f).asInstanceOf[TupleExpr].apply(p.field)
      case ForeachUnion(x, e1, e2) =>
        val r1 = replace(e1, f).asInstanceOf[BagExpr]
        val xd = VarDef(x.name, r1.tp.tp)
        val r2 = replace(e2, f).asInstanceOf[BagExpr]
        ForeachUnion(xd, r1, r2)
      case Union(e1, e2) =>
        val r1 = replace(e1, f).asInstanceOf[BagExpr]
        val r2 = replace(e2, f).asInstanceOf[BagExpr]
        Union(r1, r2)
      case Singleton(e1) =>
        Singleton(replace(e1, f).asInstanceOf[TupleExpr])
      case Tuple(fs) =>
        Tuple(fs.map(x => x._1 -> replace(x._2, f).asInstanceOf[TupleAttributeExpr]))
      case l: Let =>
        val r1 = replace(l.e1, f)
        val xd = VarDef(l.x.name, r1.tp)
        val r2 = replace(l.e2, f)
        Let(xd, r1, r2)
      case Total(e1) =>
        Total(replace(e1, f).asInstanceOf[BagExpr])
      case DeDup(e1) =>
        DeDup(replace(e1, f).asInstanceOf[BagExpr])
      case c: Cmp =>
        val c1 = replace(c.e1, f).asInstanceOf[PrimitiveExpr]
        val c2 = replace(c.e2, f).asInstanceOf[PrimitiveExpr]
        Cmp(c.op, c1, c2)
      case And(e1, e2) =>
        val c1 = replace(e1, f).asInstanceOf[CondExpr]
        val c2 = replace(e2, f).asInstanceOf[CondExpr]
        And(c1, c2)
      case Or(e1, e2) =>
        val c1 = replace(e1, f).asInstanceOf[CondExpr]
        val c2 = replace(e2, f).asInstanceOf[CondExpr]
        Or(c1, c2)
      case Not(e1) =>
        Not(replace(e1, f).asInstanceOf[CondExpr])
      case i: IfThenElse =>
        val c = replace(i.cond, f).asInstanceOf[CondExpr]
        val r1 = replace(i.e1, f)
        if (i.e2.isDefined)
          IfThenElse(c, r1, replace(i.e2.get, f))
        else
          IfThenElse(c, r1)
      case ArithmeticExpr(op, e1, e2) =>
        val n1 = replace(e1, f).asInstanceOf[NumericExpr]
        val n2 = replace(e2, f).asInstanceOf[NumericExpr]
        ArithmeticExpr(op, n1, n2)

      // Label extensions
      case x: ExtractLabel =>
        val rl = replace(x.lbl, f).asInstanceOf[LabelExpr]
        val re = replace(x.e, f)
        ExtractLabel(rl, re)
      case NewLabel(pp) =>
        NewLabel(pp.map(replace(_, f).asInstanceOf[LabelParameter]))
      case VarRefLabelParameter(v) =>
        VarRefLabelParameter(replace(v, f).asInstanceOf[Expr with VarRef])
      case ProjectLabelParameter(p) =>
        ProjectLabelParameter(replace(p, f).asInstanceOf[Expr with Project])

      // Dictionary extensions
      case BagDict(l, b, d) =>
        val rl = replace(l, f).asInstanceOf[NewLabel]
        val rb = replace(b, f).asInstanceOf[BagExpr]
        val rd = replace(d, f).asInstanceOf[TupleDictExpr]
        BagDict(rl, rb, rd)
      case TupleDict(fs) =>
        TupleDict(fs.map(x => x._1 -> replace(x._2, f).asInstanceOf[TupleDictAttributeExpr]))
      case BagDictProject(v, n) =>
        replace(v, f).asInstanceOf[TupleDictExpr].apply(n)
      case TupleDictProject(d) =>
        replace(d, f).asInstanceOf[BagDictExpr].tupleDict
      case d: DictUnion =>
        val r1 = replace(d.dict1, f).asInstanceOf[DictExpr]
        val r2 = replace(d.dict2, f).asInstanceOf[DictExpr]
        DictUnion(r1, r2)

      // Shredding extensions
      case ShredUnion(e1, e2) =>
        val r1 = replace(e1, f).asInstanceOf[BagExpr]
        val r2 = replace(e2, f).asInstanceOf[BagExpr]
        ShredUnion(r1, r2)
      case Lookup(l, d) =>
        val rl = replace(l, f).asInstanceOf[LabelExpr]
        val rd = replace(d, f).asInstanceOf[BagDictExpr]
        rd.lookup(rl)

      /////////////////
      //
      //
      // UNSTABLE BELOW
      //
      //
      /////////////////

      case WeightedSingleton(e1, w1) =>
        WeightedSingleton(
          replace(e1, f).asInstanceOf[TupleExpr],
          replace(w1, f).asInstanceOf[NumericExpr])
      case BagGroupBy(bag, v, grp, value) =>
        BagGroupBy(replace(bag, f).asInstanceOf[BagExpr], v, grp, value)
      case PlusGroupBy(bag, v, grp, value) => 
        PlusGroupBy(replace(bag, f).asInstanceOf[BagExpr], v, grp, value)
      case _ => ex
    })

  def replaceAssignment(a: Assignment, f: PartialFunction[Expr, Expr]): Assignment =
    Assignment(a.name, replace(a.rhs, f))

  def replaceProgram(p: Program, f: PartialFunction[Expr, Expr]): Program =
    Program(p.statements.map(replaceAssignment(_, f)))

  // Replace label parameter projections with variable references
  def replaceLabelParams(e: Expr): Expr = replace(e, {
    case BagDict(lbl, flat, dict) =>
      val flat2 = lbl.params.foldRight(flat) {
        case (p: ProjectLabelParameter, acc) =>
          substitute(acc, VarDef(p.name, p.tp)).asInstanceOf[BagExpr]
        case (_, acc) => acc
      }
      val dict2 = replaceLabelParams(dict).asInstanceOf[TupleDictExpr]
      BagDict(lbl, flat2, dict2)
  })

  // Substitute label projections with their variable counterpart
  protected def substitute(e: Expr, v: VarDef): Expr = replace(e, {
    case p: Project if v.name == p.tuple.name + "." + p.field =>
      // Sanity check
      assert(v.tp == p.tp, "Types differ: " + v.tp + " and " + p.tp)
      VarRef(v)
    case p: ProjectLabelParameter if v.name == p.name =>
      // Sanity check
      assert(v.tp == p.tp, "Types differ: " + v.tp + " and " + p.tp)
      VarRefLabelParameter(VarRef(v).asInstanceOf[Expr with VarRef])
  })

  def labelParameters(e: Expr): Set[LabelParameter] = 
    labelParameters(e, Map.empty).filterNot(invalidLabelElement).toSet

  protected def labelParameters(e: Expr, scope: Map[String, VarDef]): List[LabelParameter] = collect(e, {
    case v: VarRef =>
      filterByScope(v, scope).map(_ => VarRefLabelParameter(v)).toList
    case p: Project =>
      filterByScope(p.tuple, scope).map(_ => ProjectLabelParameter(p)).toList
    case ForeachUnion(x, e1, e2) =>
      labelParameters(e1, scope) ++ labelParameters(e2, scope + (x.name -> x))
    case l: Let =>
      labelParameters(l.e1, scope) ++ labelParameters(l.e2, scope + (l.x.name -> l.x))
    case p: VarRefLabelParameter =>
      filterByScope(p.e, scope).map(_ => p).toList
    case p: ProjectLabelParameter =>
      filterByScope(p.e.tuple, scope).map(_ => p).toList
    case BagDict(l, f, d) =>
      val lblVars = inputVars(l, Map.empty)
      val lblScope = scope ++ lblVars.map(v => v.name -> v.varDef).toMap
      labelParameters(f, lblScope) ++ labelParameters(d, scope)
  })

  // Input labels and dictionaries are invalid in labels
  protected def invalidLabelElement(e: LabelParameter): Boolean = e match {
    case VarRefLabelParameter(v: LabelVarRef) => v.tp.attrTps.isEmpty
    case _ => e.tp.isInstanceOf[DictType]
  }

  def inputVars(e: Expr): Set[VarRef] = inputVars(e, Map.empty).toSet

  protected def inputVars(e: Expr, scope: Map[String, VarDef]): List[VarRef] = collect(e, {
    case v: VarRef => filterByScope(v, scope).toList
    case ForeachUnion(x, e1, e2) => 
      inputVars(e1, scope) ++ inputVars(e2, scope + (x.name -> x))
    case l: Let =>
      inputVars(l.e1, scope) ++ inputVars(l.e2, scope + (l.x.name -> l.x))
    case BagDict(l, f, d) =>
      val lblVars = inputVars(l, Map.empty)
      val lblScope = scope ++ lblVars.map(v => v.name -> v.varDef).toMap
      inputVars(f, lblScope) ++ inputVars(d, scope)
  })

  protected def filterByScope(v: VarRef, scope: Map[String, VarDef]): Option[VarRef] =
    scope.get(v.name) match {
      case Some(v2) =>
        // Sanity check
        assert(v.tp == v2.tp, "Types differ: " + v.tp + " and " + v2.tp)
        None
      case None => Some(v)
    }

}
