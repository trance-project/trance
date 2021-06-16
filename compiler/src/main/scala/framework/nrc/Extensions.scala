package framework.nrc

import framework.common._
import framework.utils.Utils.Symbol

/**
  * Extension methods for NRC expressions
  */
trait Extensions {
  this: MaterializeNRC with Factory with Implicits =>

  def collect[A](e: Expr, f: PartialFunction[Expr, List[A]]): List[A] =
    f.applyOrElse(e, (ex: Expr) => ex match {
      case p: Project =>
        collect(p.tuple, f)
      case ForeachUnion(_, e1, e2) =>
        collect(e1, f) ++ collect(e2, f)
      case Union(e1, e2) =>
        collect(e1, f) ++ collect(e2, f)
      case Singleton(e1) =>
        collect(e1, f)
      case DeDup(e1) =>
        collect(e1, f)
      case Tuple(fs) =>
        fs.flatMap(x => collect(x._2, f)).toList
      case l: Let =>
        collect(l.e1, f) ++ collect(l.e2, f)
      case c: Cmp =>
        collect(c.e1, f) ++ collect(c.e2, f)
      case And(e1, e2) =>
        collect(e1, f) ++ collect(e2, f)
      case Or(e1, e2) =>
        collect(e1, f) ++ collect(e2, f)
      case Not(e1) =>
        collect(e1, f)
      case i: IfThenElse =>
        collect(i.cond, f) ++ collect(i.e1, f) ++ i.e2.map(collect(_, f)).getOrElse(Nil)
      case ArithmeticExpr(_, e1, e2) =>
        collect(e1, f) ++ collect(e2, f)
      case Count(e1) =>
        collect(e1, f)
      case Sum(e1, _) =>
        collect(e1, f)
      case g: GroupByKey =>
        collect(g.e, f)
      case ReduceByKey(e, _, _) =>
        collect(e, f)

      // Label extensions
      case l: ExtractLabel =>
        collect(l.lbl, f) ++ collect(l.e, f)
      case l: NewLabel =>
        l.params.toList.flatMap(x => collect(x._2, f))
      case p: LabelParameter =>
        collect(p.e, f)

      // Dictionary extensions
      case BagDict(_, b, d) =>
        collect(b, f) ++ collect(d, f)
      case TupleDict(fs) =>
        fs.flatMap(x => collect(x._2, f)).toList
      case TupleDictProject(d) =>
        collect(d, f)
      case d: DictUnion =>
        collect(d.dict1, f) ++ collect(d.dict2, f)

      // Shredding extensions
      case ShredUnion(e1, e2) =>
        collect(e1, f) ++ collect(e2, f)
      case Lookup(l, d) =>
        collect(l, f) ++ collect(d, f)

      // Materialization extensions
      case KeyValueMapLookup(l, d) =>
        collect(l, f) ++ collect(d, f)
      case BagToKeyValueMap(b) =>
        collect(b, f)
      case KeyValueMapToBag(d) =>
        collect(d, f)

      case _ => List()
    })

  def replace(e: Expr, f: PartialFunction[Expr, Expr]): Expr =
    f.applyOrElse(e, (ex: Expr) => ex match {
      case p: Project =>
        Project(replace(p.tuple, f).asInstanceOf[AbstractTuple], p.field)
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
      case DeDup(e1) =>
        DeDup(replace(e1, f).asInstanceOf[BagExpr])
      case Tuple(fs) =>
        Tuple(fs.map(x => x._1 -> replace(x._2, f).asInstanceOf[TupleAttributeExpr]))
      case l: Let =>
        val r1 = replace(l.e1, f)
        val r2 = replace(l.e2, f)
        Let(l.x.name, r1, r2)
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
      case Count(e1) =>
        Count(replace(e1, f).asInstanceOf[BagExpr])
      case Sum(e1, fs) =>
        Sum(replace(e1, f).asInstanceOf[BagExpr], fs)
      case GroupByKey(e1, ks, vs, n) =>
        GroupByKey(replace(e1, f).asInstanceOf[BagExpr], ks, vs, n)
      case ReduceByKey(e1, ks, vs) =>
        ReduceByKey(replace(e1, f).asInstanceOf[BagExpr], ks, vs)

      // Label extensions
      case x: ExtractLabel =>
        val rl = replace(x.lbl, f).asInstanceOf[LabelExpr]
        val re = replace(x.e, f)
        ExtractLabel(rl, re)
      case l: NewLabel =>
        val ps = l.params.map(x => x._1 -> replace(x._2, f).asInstanceOf[LabelParameter])
        NewLabel(ps, l.id)
      case VarRefLabelParameter(v) =>
        VarRefLabelParameter(replace(v, f).asInstanceOf[Expr with VarRef])
      case ProjectLabelParameter(p) =>
        ProjectLabelParameter(replace(p, f).asInstanceOf[Expr with Project])

      // Dictionary extensions
      case BagDict(tp, b, d) =>
        val rb = replace(b, f).asInstanceOf[BagExpr]
        val rd = replace(d, f).asInstanceOf[TupleDictExpr]
        BagDict(tp, rb, rd)
      case TupleDict(fs) =>
        TupleDict(fs.map(x => x._1 -> replace(x._2, f).asInstanceOf[TupleDictAttributeExpr]))
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

      // Materialization extensions
      case KeyValueMapLookup(l, d) =>
        val rl = replace(l, f).asInstanceOf[LabelExpr]
        val rd = replace(d, f).asInstanceOf[KeyValueMapExpr]
        KeyValueMapLookup(rl, rd)
      case BagToKeyValueMap(b) =>
        val r = replace(b, f).asInstanceOf[BagExpr]
        BagToKeyValueMap(r)
      case KeyValueMapToBag(d) =>
        val r = replace(d, f).asInstanceOf[KeyValueMapExpr]
        KeyValueMapToBag(r)

      case _ => ex
    })

  // Replace label parameter projections with variable references
  def createLambda(lbl: NewLabel, e: BagExpr): BagExpr =
    lbl.params.foldRight(e) {
      case ((n, l: ProjectLabelParameter), acc) =>
        // Replace projection in l with a variable reference
        replace(acc, {
          case p: Project if p.tuple.name == l.e.tuple.name && p.field == l.e.field =>
            // Sanity check
            assert(p.tp == l.tp, "[createLambda] Types differ: " + p.tp + " and " + l.tp)
            VarRef(n, l.tp)
          case p: ProjectLabelParameter if p.e.tuple.name == l.e.tuple.name && p.e.field == l.e.field =>
            // Sanity check
            assert(p.tp == l.tp, "[createLambda] Types differ: " + p.tp + " and " + l.tp)
            VarRefLabelParameter(VarRef(n, l.tp).asInstanceOf[Expr with VarRef])
        }).asInstanceOf[BagExpr]

      case ((n, l: VarRefLabelParameter), acc) if n != l.e.name =>
        // Replace variable reference in l with a new variable reference
        replace(acc, {
          case v: VarRef if v.name == l.e.name =>
            assert(v.tp == l.tp, "[createLambda] Types differ: " + v.tp + " and " + l.tp)
            VarRef(n, l.tp)
        }).asInstanceOf[BagExpr]

      case (_, acc) => acc
    }

  // Replace variable references with projections
  def applyLambda(lbl: NewLabel, e: BagExpr): BagExpr =
    lbl.params.foldRight(e) {
      case ((n, l: ProjectLabelParameter), acc) =>
        // Replace variable reference with a projection
        replace(acc, {
          case v: VarRef if v.name == n =>
            // Sanity check
            assert(v.tp == l.tp, "[applyLambda] Types differ: " + v.tp + " and " + l.tp)
            l.e
          case p: VarRefLabelParameter if p.e.name == n =>
            // Sanity check
            assert(p.tp == l.tp, "[applyLambda] Types differ: " + p.tp + " and " + l.tp)
            l
        }).asInstanceOf[BagExpr]

      case ((n, l: VarRefLabelParameter), acc) if n != l.e.name =>
        replace(acc, {
          case v: VarRef if v.name == n =>
            assert(v.tp == l.tp, "[applyLambda] Types differ: " + v.tp + " and " + l.tp)
            l.e
        }).asInstanceOf[BagExpr]

      case (_, acc) => acc
    }

  def labelParameters(e: Expr, scope: Map[String, VarDef]): List[LabelParameter] = collect(e, {
    case v: VarRef =>
      filterByScope(v, scope).map(_ => VarRefLabelParameter(v)).toList
    case p: Project =>
      filterByScope(p.tuple, scope).map(_ => ProjectLabelParameter(p)).toList
    case ForeachUnion(x, e1, e2) =>
      labelParameters(e1, scope) ++ labelParameters(e2, scope + (x.name -> x))
    case l: Let =>
      labelParameters(l.e1, scope) ++ labelParameters(l.e2, scope + (l.x.name -> l.x))
    case x: ExtractLabel =>
      val xscope = x.lbl.tp.attrTps.collect {
        case (n, t) if !coveredByScope(n, t, scope) => n -> VarDef(n, t)
      }
      labelParameters(x.lbl, scope) ++ labelParameters(x.e, scope ++ xscope)
    case p: VarRefLabelParameter =>
      filterByScope(p.e, scope).map(_ => p).toList
    case p: ProjectLabelParameter =>
      filterByScope(p.e.tuple, scope).map(_ => p).toList
    case BagDict(ltp, f, d) =>
      val params = ltp.attrTps.map(v => v._1 -> VarDef(v._1, v._2))
      labelParameters(f, scope ++ params) ++ labelParameters(d, scope)
  })

  protected def filterByScope(v: VarRef, scope: Map[String, VarDef]): Option[VarRef] =
    if (coveredByScope(v.name, v.tp, scope)) None else Some(v)

  protected def coveredByScope(name: String, tp: Type, scope: Map[String, VarDef]): Boolean =
    scope.get(name) match {
      case Some(v) =>
        // Sanity check
         assert(tp == v.tp, s"[coveredByScope] Types differ for $name: " + tp + " and " + v.tp)
        true
      case None => false
    }

  def inputVars(e: Expr): Set[VarRef] =
    inputVars(e, Map.empty[String, VarDef]).toSet

  def inputVars(a: Assignment): Set[VarRef] =
    inputVars(a.rhs)

  def inputVars(p: Program): Set[VarRef] =
    p.statements.foldLeft (Map.empty[String, VarDef], Set.empty[VarRef]) {
      case ((scope, vv), s) =>
        ( scope + (s.name -> VarDef(s.name, s.rhs.tp)),
          vv ++ inputVars(s.rhs, scope).toSet )
    }._2

  def inputVars(e: ShredExpr): Set[VarRef] =
    inputVars(e, Map.empty[String, VarDef]).toSet

  def inputVars(a: ShredAssignment): Set[VarRef] =
    inputVars(a, Map.empty[String, VarDef])

  def inputVars(p: ShredProgram): Set[VarRef] =
    p.statements.foldLeft (Map.empty[String, VarDef], Set.empty[VarRef]) {
      case ((scope, vv), s) =>
        ( scope +
          (flatName(s.name) -> VarDef(flatName(s.name), s.rhs.flat.tp)) +
            (dictName(s.name) -> VarDef(dictName(s.name), s.rhs.dict.tp)),
          vv ++ inputVars(s.rhs, scope).toSet )
    }._2

  protected def inputVars(e: Expr, scope: Map[String, VarDef]): List[VarRef] = collect(e, {
    case v: VarRef =>
      filterByScope(v, scope).toList
    case ForeachUnion(x, e1, e2) =>
      inputVars(e1, scope) ++ inputVars(e2, scope + (x.name -> x))
    case l: Let =>
      inputVars(l.e1, scope) ++ inputVars(l.e2, scope + (l.x.name -> l.x))
    case x: ExtractLabel =>
      val xscope = x.lbl.tp.attrTps.collect {
        case (n, t) if !coveredByScope(n, t, scope) => n -> VarDef(n, t)
      }
      inputVars(x.lbl, scope) ++ inputVars(x.e, scope ++ xscope)
    case BagDict(ltp, f, d) =>
      val params = ltp.attrTps.map(v => v._1 -> VarDef(v._1, v._2))
      inputVars(f, scope ++ params) ++ inputVars(d, scope)
  })

  protected def inputVars(a: Assignment, scope: Map[String, VarDef]): List[VarRef] =
    inputVars(a.rhs, scope)

  protected def inputVars(e: ShredExpr, scope: Map[String, VarDef]): List[VarRef] =
    inputVars(e.flat, scope) ++ inputVars(e.dict, scope)

  protected def inputVars(a: ShredAssignment, scope: Map[String, VarDef]): Set[VarRef] =
    inputVars(a.rhs.flat, scope).toSet ++ inputVars(a.rhs.dict, scope).toSet


  def addOutputField(f: (String, TupleAttributeExpr), t: TupleExpr): TupleExpr = t match {
    case Tuple(fs) if fs.contains(f._1) => (fs(f._1), f._2) match {
      case (NewLabel(ps1, _), NewLabel(ps2, _)) =>
        Tuple(fs + (f._1 -> NewLabel(ps1 ++ ps2)))
      case _ =>
        Tuple(fs + f)
    }
    case Tuple(fs) =>
      Tuple(fs + f)
    case TupleLet(x, e1, e2) =>
      TupleLet(x, e1, addOutputField(f, e2))
    case TupleIfThenElse(c, e1, e2) =>
      TupleIfThenElse(c, addOutputField(f, e1), addOutputField(f, e2))
    case TupleExtractLabel(l, e) =>
      TupleExtractLabel(l, addOutputField(f, e))
    case _ =>
      Tuple(t.tp.attrTps.keys.map(k => k -> t(k)).toMap + f)
  }

  def addOutputField(f: (String, TupleAttributeExpr), b: BagExpr): BagExpr = b match {
      case ForeachUnion(x, b1, b2) =>
        ForeachUnion(x, b1, addOutputField(f, b2))
      case Union(e1, e2) =>
        Union(addOutputField(f, e1), addOutputField(f, e2))
      case DeDup(e) =>
        DeDup(addOutputField(f, e))
      case Singleton(t) =>
        Singleton(addOutputField(f, t))
      case BagLet(x, e1, e2) =>
        BagLet(x, e1, addOutputField(f, e2))
      case BagIfThenElse(c, e1, Some(e2)) =>
        BagIfThenElse(c, addOutputField(f, e1), Some(addOutputField(f, e2)))
      case BagIfThenElse(c, e1, None) =>
        BagIfThenElse(c, addOutputField(f, e1), None)
      case GroupByKey(e, ks, vs, n) =>
        GroupByKey(addOutputField(f, e), (f._1 :: ks).distinct, vs, n)
      case ReduceByKey(e, ks, vs) =>
        ReduceByKey(addOutputField(f, e), (f._1 :: ks).distinct, vs)
      case BagExtractLabel(l, e) =>
        BagExtractLabel(l, addOutputField(f, e))
      case _ =>
        val x = TupleVarRef(Symbol.fresh(), b.tp.tp)
        val xProjects = x.tp.attrTps.keys.map(k => k -> x(k)).toMap
        ForeachUnion(x, b, Singleton(Tuple(xProjects + f)))
    }
}
