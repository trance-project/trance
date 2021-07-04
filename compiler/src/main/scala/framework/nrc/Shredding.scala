package framework.nrc

import framework.common._

/**
  * Common shredding methods
  */
trait BaseShredding {

  def flatName(s: String): String = s + "__F"

  def flatTp(tp: Type): Type = tp match {
    case _: PrimitiveType => tp
    case bt: BagType => LabelType()
    case TupleType(as) =>
      TupleType(as.map { case (n, t) => n -> flatTp(t).asInstanceOf[TupleAttributeType] })
    case _ => sys.error("Unknown flat type of " + tp)
  }

  def dictName(s: String): String = s + "__D"

  def dictTp(tp: Type): DictType = tp match {
    case _: PrimitiveType => EmptyDictType
    case BagType(t) =>
      BagDictType(
        LabelType(),
        BagType(flatTp(t).asInstanceOf[TupleType]),
        dictTp(t).asInstanceOf[TupleDictType]
      )
    case TupleType(as) =>
      TupleDictType(as.map { case (n, t) => n -> dictTp(t).asInstanceOf[TupleDictAttributeType] })
    case _ => sys.error("Unknown dict type of " + tp)
  }

}

/**
  * Shredding transformation
  */
trait Shredding extends BaseShredding with Extensions {
  this: MaterializeNRC =>

  def shred(e: Expr): ShredExpr = shred(e, Map.empty)

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
      val tupleDict = dict2.tupleDict

      ShredExpr(lbl, BagDict(lbl.tp, createLambda(lbl, flat), tupleDict))

    case Union(e1, e2) =>
      val ShredExpr(l1: LabelExpr, dict1: BagDictExpr) = shred(e1, ctx)
      val ShredExpr(l2: LabelExpr, dict2: BagDictExpr) = shred(e2, ctx)
      val flat = ShredUnion(dict1.lookup(l1), dict2.lookup(l2))
      val dictUnion = TupleDictUnion(dict1.tupleDict, dict2.tupleDict)
      val lbl = NewLabel(labelParameters(flat))
      ShredExpr(lbl, BagDict(lbl.tp, createLambda(lbl, flat), dictUnion))

    case Singleton(e1) =>
      val ShredExpr(t: TupleExpr, dict: TupleDictExpr) = shred(e1, ctx)
      val flat = Singleton(t)
      val lbl = NewLabel(labelParameters(flat))
      ShredExpr(lbl, BagDict(lbl.tp, createLambda(lbl, flat), dict))

    case DeDup(e1) =>
      val ShredExpr(lbl1: LabelExpr, dict1: BagDictExpr) = shred(e1, ctx)
      val flat = DeDup(dict1.lookup(lbl1))
      val lbl = NewLabel(labelParameters(flat))
      ShredExpr(lbl, BagDict(lbl.tp, createLambda(lbl, flat), dict1.tupleDict))

    case Get(e1) =>
      val ShredExpr(flat: TupleExpr, dict: TupleDictExpr) = shred(e1, ctx)
      ShredExpr(Get(Singleton(flat)), dict)

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

    case Count(e1) =>
      val ShredExpr(lbl: LabelExpr, dict1: BagDictExpr) = shred(e1, ctx)
      ShredExpr(Count(dict1.lookup(lbl)), EmptyDict)

    case Sum(e1, fs) =>
      val ShredExpr(lbl: LabelExpr, dict1: BagDictExpr) = shred(e1, ctx)
      ShredExpr(Sum(dict1.lookup(lbl), fs), EmptyDict)

    case GroupByKey(e1, ks, vs, n) =>
      val ShredExpr(lbl1: LabelExpr, dict1: BagDictExpr) = shred(e1, ctx)
      val flat = GroupByKey(dict1.lookup(lbl1), ks, vs, n)
      val lbl = NewLabel(labelParameters(flat))
      ShredExpr(lbl, BagDict(lbl.tp, createLambda(lbl, flat), dict1.tupleDict))

    case ReduceByKey(e1, ks, vs) =>
      val ShredExpr(lbl1: LabelExpr, dict1: BagDictExpr) = shred(e1, ctx)
      val flat = ReduceByKey(dict1.lookup(lbl1), ks, vs)
      val lbl = NewLabel(labelParameters(flat))
      ShredExpr(lbl, BagDict(lbl.tp, createLambda(lbl, flat), dict1.tupleDict))

    case PrimitiveUdf(name, in : PrimitiveExpr, otp) =>
      val ShredExpr(flat: PrimitiveExpr, dict: TupleDictExpr) = shred(in, ctx)
      ShredExpr(PrimitiveUdf(name,flat,otp), dict)

    case _ => sys.error("Cannot shred expr " + e)
  }

  def shred(stmt: Assignment): ShredAssignment = shred(stmt, Map.empty)

  private def shred(stmt: Assignment, ctx: Map[String, ShredExpr]): ShredAssignment =
    ShredAssignment(stmt.name, shred(stmt.rhs, ctx))

  def shred(program: Program): ShredProgram =
    shredCtx(program, Map.empty[String, ShredExpr])._1

  def shredCtx(program: Program, ctx: Map[String, ShredExpr] = Map.empty): (ShredProgram, Map[String, ShredExpr]) =
    program.statements.foldLeft (ShredProgram(), ctx) {
      case ((acc, ctx), stmt) =>
        val sa = shred(stmt, ctx)
        (ShredProgram(acc.statements :+ sa), ctx + (sa.name -> sa.rhs))
    }

}
