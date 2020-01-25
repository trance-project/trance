package shredding.nrc

import shredding.core._

/**
  * Common shredding methods
  */
trait BaseShredding {

  def flatName(s: String): String = s + "__F"

  def flatTp(tp: Type): Type = tp match {
    case _: PrimitiveType => tp
    case _: BagType => LabelType()
    case TupleType(as) =>
      TupleType(as.map { case (n, t) => n -> flatTp(t).asInstanceOf[TupleAttributeType] })
    case _ => sys.error("Unknown flat type of " + tp)
  }

  def dictName(s: String): String = s + "__D"

  def dictTp(tp: Type): DictType = tp match {
    case _: PrimitiveType => EmptyDictType
    case BagType(t) =>
      BagDictType(BagType(flatTp(t).asInstanceOf[TupleType]), dictTp(t).asInstanceOf[TupleDictType])
    case TupleType(as) =>
      TupleDictType(as.map { case (n, t) => n -> dictTp(t).asInstanceOf[TupleDictAttributeType] })
    case _ => sys.error("Unknown dict type of " + tp)
  }

}

/**
  * Shredding transformation
  */
trait Shredding extends BaseShredding with Extensions {
  this: ShredNRC =>

  def shred(e: Expr): ShredExpr = substituteLabelParams(shred(e, Map.empty))

  private def substituteLabelParams(e: ShredExpr): ShredExpr =
    ShredExpr(e.flat, replaceLabelParams(e.dict).asInstanceOf[DictExpr])

  private def shred(e: Expr, ctx: Map[String, ShredExpr]): ShredExpr = e match {
    case _: Const => ShredExpr(e, EmptyDict)

    /* ctx ensures correct type when dealing with labels
     * because flatTp returns empty LabelType()
     */
    case v: VarRef if ctx.contains(v.name) =>
      val sv = ctx(v.name)
      ShredExpr(
        VarRef(flatName(v.name), sv.flat.tp),
        DictVarRef(dictName(v.name), sv.dict.tp))

    case v: VarRef =>
      ShredExpr(
        VarRef(flatName(v.name), flatTp(v.tp)),
        DictVarRef(dictName(v.name), dictTp(v.tp)))

    case p: Project =>
      val ShredExpr(flat: TupleExpr, dict: TupleDictExpr) = shred(p.tuple, ctx)
      ShredExpr(Project(flat, p.field), dict(p.field))

    case ForeachUnion(x, e1, e2) =>
      // Shredding e1
      val ShredExpr(l1: LabelExpr, dict1: BagDictExpr) = shred(e1, ctx)
      val resolved1 = dict1.lookup(l1)
      // Create x^F
      val xFlat = VarDef(flatName(x.name), resolved1.tp.tp)
      val xDict = VarDef(dictName(x.name), dict1.tp.dictTp)
      // Shredding e2
      val ctx2 = ctx + (x.name -> ShredExpr(VarRef(xFlat), dict1.tupleDict))
      val ShredExpr(l2: LabelExpr, dict2: BagDictExpr) = shred(e2, ctx2)
      val resolved2 = dict2.lookup(l2)
      // Output flat bag
      val flat =
        BagLet(xDict, dict1.tupleDict,
          ForeachUnion(xFlat, resolved1, resolved2))
      val lbl = NewLabel(labelParameters(flat))
      val tupleDict = TupleDictLet(xDict, dict1.tupleDict, dict2.tupleDict)
      ShredExpr(lbl, BagDict(lbl, flat, tupleDict))

    case Union(e1, e2) =>
      val ShredExpr(l1: LabelExpr, dict1: BagDictExpr) = shred(e1, ctx)
      val ShredExpr(l2: LabelExpr, dict2: BagDictExpr) = shred(e2, ctx)
      val flat = ShredUnion(dict1.lookup(l1), dict2.lookup(l2))
      val lbl = NewLabel(labelParameters(flat))
      ShredExpr(lbl, BagDict(lbl, flat, TupleDictUnion(dict1.tupleDict, dict2.tupleDict)))

    case Singleton(e1) =>
      val ShredExpr(flat: TupleExpr, dict: TupleDictExpr) = shred(e1, ctx)
      val lbl = NewLabel(labelParameters(flat))
      ShredExpr(lbl, BagDict(lbl, Singleton(flat), dict))

    case Tuple(fs) =>
      val shredFs = fs.map(f => f._1 -> shred(f._2, ctx))
      val flatFs = shredFs.map(f => f._1 -> f._2.flat.asInstanceOf[TupleAttributeExpr])
      val dictFs = shredFs.map(f => f._1 -> f._2.dict.asInstanceOf[TupleDictAttributeExpr])
      ShredExpr(Tuple(flatFs), TupleDict(dictFs))

    case l: Let =>
      val se1 = shred(l.e1, ctx)
      val xFlat = VarDef(flatName(l.x.name), se1.flat.tp)
      val xDict = VarDef(dictName(l.x.name), se1.dict.tp)
      val se2 = shred(l.e2, ctx + (l.x.name -> ShredExpr(VarRef(xFlat), se1.dict)))
      val flat = Let(xDict, se1.dict, Let(xFlat, se1.flat, se2.flat))
      val dict = DictLet(xDict, se1.dict, se2.dict)
      ShredExpr(flat, dict)

    case Total(e1) =>
      val ShredExpr(lbl: LabelExpr, dict1: BagDictExpr) = shred(e1, ctx)
      ShredExpr(Total(dict1.lookup(lbl)), EmptyDict)

    case DeDup(e1) =>
      val ShredExpr(lbl1: LabelExpr, dict1: BagDictExpr) = shred(e1, ctx)
      val flat = DeDup(dict1.lookup(lbl1))
      val lbl = NewLabel(labelParameters(flat))
      ShredExpr(lbl, BagDict(lbl, flat, dict1.tupleDict))

    case c: Cmp =>
      val ShredExpr(c1: PrimitiveExpr, EmptyDict) = shred(c.e1, ctx)
      val ShredExpr(c2: PrimitiveExpr, EmptyDict) = shred(c.e2, ctx)
      ShredExpr(Cmp(c.op, c1, c2), EmptyDict)

    case And(e1, e2) =>
      val ShredExpr(c1: CondExpr, EmptyDict) = shred(e1, ctx)
      val ShredExpr(c2: CondExpr, EmptyDict) = shred(e2, ctx)
      ShredExpr(And(c1, c2), EmptyDict)

    case Or(e1, e2) =>
      val ShredExpr(c1: CondExpr, EmptyDict) = shred(e1, ctx)
      val ShredExpr(c2: CondExpr, EmptyDict) = shred(e2, ctx)
      ShredExpr(Or(c1, c2), EmptyDict)

    case Not(e1) =>
      val ShredExpr(c1: CondExpr, EmptyDict) = shred(e1, ctx)
      ShredExpr(Not(c1), EmptyDict)

    case i: IfThenElse =>
      val ShredExpr(c: CondExpr, EmptyDict) = shred(i.cond, ctx)
      val se1 = shred(i.e1, ctx)
      if (i.e2.isDefined) {
        val se2 = shred(i.e2.get, ctx)
        ShredExpr(IfThenElse(c, se1.flat, se2.flat), DictIfThenElse(c, se1.dict, se2.dict))
      }
      else
        ShredExpr(IfThenElse(c, se1.flat), se1.dict)

    case a: ArithmeticExpr =>
      val ShredExpr(n1: NumericExpr, EmptyDict) = shred(a.e1, ctx)
      val ShredExpr(n2: NumericExpr, EmptyDict) = shred(a.e2, ctx)
      ShredExpr(ArithmeticExpr(a.op, n1, n2), EmptyDict)


    /////////////////
    //
    //
    // UNSTABLE BELOW
    //
    //
    /////////////////

    case WeightedSingleton(e1, w1) =>
      val ShredExpr(flat1: TupleExpr, dict: TupleDictExpr) = shred(e1, ctx)
      val ShredExpr(flat2: NumericExpr, EmptyDict) = shred(w1, ctx)
      val flat = WeightedSingleton(flat1, flat2)
      val lbl = NewLabel(labelParameters(flat))
      ShredExpr(lbl, BagDict(lbl, flat, dict))

    case BagGroupBy(e1, v1, grp, value) =>
      val ShredExpr(lbl1: LabelExpr, dict1: BagDictExpr) = shred(e1, ctx)
      val flat = BagGroupBy(dict1.lookup(lbl1), v1, grp, value)
      val lbl = NewLabel(labelParameters(flat))
      ShredExpr(lbl, BagDict(lbl, flat, dict1.tupleDict))

    case PlusGroupBy(e1, v1, grp, value) =>
      val ShredExpr(lbl1: LabelExpr, dict1: BagDictExpr) = shred(e1, ctx)
      val flat = PlusGroupBy(dict1.lookup(lbl1), v1, grp, value)
      val lbl = NewLabel(labelParameters(flat))
      ShredExpr(lbl, BagDict(lbl, flat, dict1.tupleDict))

    case _ => sys.error("Cannot shred expr " + e)
  }

  def shred(stmt: Assignment): ShredAssignment = shred(stmt, Map.empty)

  private def shred(stmt: Assignment, ctx: Map[String, ShredExpr]): ShredAssignment =
    ShredAssignment(stmt.name, shred(stmt.rhs, ctx))

  def shred(program: Program): ShredProgram =
    ShredProgram(
      program.statements.foldLeft (List[ShredAssignment](), Map.empty[String, ShredExpr]) {
        case ((acc, ctx), stmt) =>
          val sa = shred(stmt, ctx)
          (acc :+ sa, ctx + (sa.name -> sa.rhs))
      }._1
    )

}
