package shredding.nrc

import shredding.core.VarDef

/**
  * Extension methods for NRC expressions
  */
trait Extensions extends LinearizedNRC {

  def collect[A](e: Expr, f: PartialFunction[Expr, List[A]]): List[A] =
    f.applyOrElse(e, (ex: Expr) => ex match {
      case p: Project => collect(p.tuple, f)
      case ForeachUnion(_, e1, e2) => collect(e1, f) ++ collect(e2, f)
      case Union(e1, e2) => collect(e1, f) ++ collect(e2, f)
      case Singleton(e1) => collect(e1, f)
      case Tuple(fs) => fs.flatMap(x => collect(x._2, f)).toList
      case l: Let => collect(l.e1, f) ++ collect(l.e2, f)
      case Total(e1) => collect(e1, f)
      case i: IfThenElse =>
        collect(i.cond.e1, f) ++ collect(i.cond.e2, f) ++
          collect(i.e1, f) ++ i.e2.map(collect(_, f)).getOrElse(Nil)
      case Lookup(l, d) => collect(l, f) ++ collect(d, f)
      case BagDict(l, b, d) => collect(l, f) ++ collect(b, f) ++ collect(d, f)
      case TupleDict(fs) => fs.flatMap(x => collect(x._2, f)).toList
      case BagDictProject(v, _) => collect(v, f)
      case TupleDictProject(d) => collect(d, f)
      case DictUnion(d1, d2) => collect(d1, f) ++ collect(d2, f)
      case Named(_, e1) => collect(e1, f)
      case Sequence(ee) => ee.flatMap(collect(_, f))
      case _ => List()
    })

  def replace(e: Expr, f: PartialFunction[Expr, Expr]): Expr =
    f.applyOrElse(e, (ex: Expr) => ex match {
      case p: Project =>
        val r = replace(p.tuple, f).asInstanceOf[TupleExpr]
        ShredProject(r, p.field)
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
        ShredLet(xd, r1, r2)
      case Total(e1) =>
        Total(replace(e1, f).asInstanceOf[BagExpr])
      case i: IfThenElse =>
        val c1 = replace(i.cond.e1, f).asInstanceOf[TupleAttributeExpr]
        val c2 = replace(i.cond.e2, f).asInstanceOf[TupleAttributeExpr]
        val r1 = replace(i.e1, f)
        if (i.e2.isDefined)
          ShredIfThenElse(Cond(i.cond.op, c1, c2), r1, replace(i.e2.get, f))
        else
          ShredIfThenElse(Cond(i.cond.op, c1, c2), r1)

      case Lookup(l, d) =>
        val rl = replace(l, f).asInstanceOf[LabelExpr]
        val rd = replace(d, f).asInstanceOf[BagDictExpr]
        Lookup(rl, rd)
      case BagDict(l, b, d) =>
        val rl = replace(l, f).asInstanceOf[LabelExpr]
        val rb = replace(b, f).asInstanceOf[BagExpr]
        val rd = replace(d, f).asInstanceOf[TupleDictExpr]
        BagDict(rl, rb, rd)
      case TupleDict(fs) =>
        TupleDict(fs.map(x => x._1 -> replace(x._2, f).asInstanceOf[TupleDictAttributeExpr]))
      case BagDictProject(v, n) =>
        BagDictProject(replace(v, f).asInstanceOf[TupleDictExpr], n)
      case TupleDictProject(d) =>
        TupleDictProject(replace(d, f).asInstanceOf[BagDictExpr])
      case DictUnion(d1, d2) =>
        val r1 = replace(d1, f).asInstanceOf[DictExpr]
        val r2 = replace(d2, f).asInstanceOf[DictExpr]
        DictUnion(r1, r2)

      case Named(v, e1) => Named(v, replace(e1, f))
      case Sequence(ee) => Sequence(ee.map(replace(_, f)))

      case _ => ex
    })

  def inputVars(e: Expr): Set[VarRef] = inputVars(e, Map.empty).toSet

  protected def inputVars(e: Expr, scope: Map[String, VarDef]): List[VarRef] = collect(e, {
    case v: VarRef => inputVarRef(v, scope)
    case ForeachUnion(x, e1, e2) => inputVars(e1, scope) ++ inputVars(e2, scope + (x.name -> x))
    case l: Let => inputVars(l.e1, scope) ++ inputVars(l.e2, scope + (l.x.name -> l.x))
    case NewLabel(vs) => vs.flatMap(inputVarRef(_, scope)).toList
    case _: BagDict => Nil    // bag dictionary has no free variables
    case Named(v, e1) => inputVars(e1, scope + (v.name -> v))
  })

  protected def inputVarRef(v: VarRef, scope: Map[String, VarDef]): List[VarRef] =
    scope.get(v.name).map { v2 =>
      // Sanity check
      assert(v.tp == v2.tp, "Types differ: " + v.tp + " and " + v2.tp)
      Nil
    }.getOrElse(List(v))
}
