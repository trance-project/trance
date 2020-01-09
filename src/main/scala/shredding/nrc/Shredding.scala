package shredding.nrc

import shredding.core._

/**
  * Common shredding methods
  */
trait BaseShredding {

  def flatName(s: String): String = s + "^F"

  def flatTp(tp: Type): Type = tp match {
    case _: PrimitiveType => tp
    case _: BagType => LabelType()
    case TupleType(as) =>
      TupleType(as.map { case (n, t) => n -> flatTp(t).asInstanceOf[TupleAttributeType] })
    case _ => sys.error("Unknown flat type of " + tp)
  }

  def dictName(s: String): String = s + "^D"

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

  def labelVars(e: Expr): Set[VarRef] = inputVars(e).filterNot(_.isInstanceOf[DictExpr])

  def shred(e: Expr): ShredExpr = shred(e, Map.empty)

  def shred(e: Expr, ctx: Map[String, ShredExpr]): ShredExpr = e match {
    case Const(_, _) => ShredExpr(e, EmptyDict)

    /* ctx ensures correct type when dealing with labels
     * because flatTp returns empty LabelType()
     */
    case v: VarRef if ctx.contains(v.name) =>
      val sv = ctx(v.name)
      ShredExpr(
        ShredVarRef(flatName(v.name), sv.flat.tp),
        DictVarRef(dictName(v.name), sv.dict.tp))

    case v: VarRef =>
      ShredExpr(
        ShredVarRef(flatName(v.name), flatTp(v.tp)),
        DictVarRef(dictName(v.name), dictTp(v.tp)))

    case p: Project =>
      val ShredExpr(flat: TupleExpr, dict: TupleDictExpr) = shred(p.tuple, ctx)
      ShredExpr(ShredProject(flat, p.field), dict(p.field))

    case ForeachUnion(x, e1, e2) =>
      // Shredding e1
      val ShredExpr(l1: LabelExpr, dict1: BagDictExpr) = shred(e1, ctx)
      val resolved1 = dict1.lookup(l1)
      // Create x^F
      val xFlat = VarDef(flatName(x.name), resolved1.tp.tp)
      val xDict = VarDef(dictName(x.name), dict1.tp.dictTp)
      // Shredding e2
      val ctx2 = ctx + (x.name -> ShredExpr(ShredVarRef(xFlat), dict1.tupleDict))
      val ShredExpr(l2: LabelExpr, dict2: BagDictExpr) = shred(e2, ctx2)
      val resolved2 = dict2.lookup(l2)
      // Output flat bag
      val flat =
        BagLet(xDict, dict1.tupleDict,
          ForeachUnion(xFlat, resolved1, resolved2))
      val lbl = NewLabel(labelVars(flat))
      val outputDict = TupleDictLet(xDict, dict1.tupleDict, dict2.tupleDict)
      ShredExpr(lbl, BagDict(lbl, flat, outputDict))

    case Union(e1, e2) =>
      val ShredExpr(l1: LabelExpr, dict1: BagDictExpr) = shred(e1, ctx)
      val ShredExpr(l2: LabelExpr, dict2: BagDictExpr) = shred(e2, ctx)
      val flat = Union(dict1.lookup(l1), dict2.lookup(l2))  // could be a heterogeneous union where labels in e1 and e2
                                                            // for one attribute encapsulate different input variables
      val lbl = NewLabel(labelVars(flat))
      ShredExpr(lbl, BagDict(lbl, flat, TupleDictUnion(dict1.tupleDict, dict2.tupleDict)))

    case Singleton(e1) =>
      val ShredExpr(flat: TupleExpr, dict: TupleDictExpr) = shred(e1, ctx)
      val lbl = NewLabel(labelVars(flat))
      ShredExpr(lbl, BagDict(lbl, Singleton(flat), dict))

    case WeightedSingleton(e1, w1) =>
      val ShredExpr(flat1: TupleExpr, dict: TupleDictExpr) = shred(e1, ctx)
      val ShredExpr(flat2: PrimitiveExpr, EmptyDict) = shred(w1, ctx)
      val flat = WeightedSingleton(flat1, flat2)
      val lbl = NewLabel(labelVars(flat))
      ShredExpr(lbl, BagDict(lbl, flat, dict))

    case Tuple(fs) =>
      val shredFs = fs.map(f => f._1 -> shred(f._2, ctx))
      val flatFs = shredFs.map(f => f._1 -> f._2.flat.asInstanceOf[TupleAttributeExpr])
      val dictFs = shredFs.map(f => f._1 -> f._2.dict.asInstanceOf[TupleDictAttributeExpr])
      ShredExpr(Tuple(flatFs), TupleDict(dictFs))

    case l: Let =>
      val se1 = shred(l.e1, ctx)
      val xFlat = VarDef(flatName(l.x.name), se1.flat.tp)
      val xDict = VarDef(dictName(l.x.name), se1.dict.tp)
      val se2 = shred(l.e2, ctx + (l.x.name -> ShredExpr(ShredVarRef(xFlat), se1.dict)))
      val flat = ShredLet(xDict, se1.dict, ShredLet(xFlat, se1.flat, se2.flat))
      val dict = DictLet(xDict, se1.dict, se2.dict)
      ShredExpr(flat, dict)

    case Total(e1) =>
      val ShredExpr(lbl: LabelExpr, dict1: BagDictExpr) = shred(e1, ctx)
      ShredExpr(Total(dict1.lookup(lbl)), EmptyDict)

    case DeDup(e1) =>
      val ShredExpr(lbl: LabelExpr, dict1: BagDictExpr) = shred(e1, ctx)
      ShredExpr(lbl, BagDict(lbl, DeDup(dict1.lookup(lbl)), dict1.tupleDict))

    case c: Cond => c match {
      case Cmp(op, e1, e2) =>
        val ShredExpr(c1: TupleAttributeExpr, _: DictExpr) = shred(e1, ctx)
        val ShredExpr(c2: TupleAttributeExpr, _: DictExpr) = shred(e2, ctx)
        ShredExpr(Cmp(op, c1, c2), EmptyDict)
      case And(e1, e2) =>
        val ShredExpr(c1: Cond, EmptyDict) = shred(e1, ctx)
        val ShredExpr(c2: Cond, EmptyDict) = shred(e2, ctx)
        ShredExpr(And(c1, c2), EmptyDict)
      case Or(e1, e2) =>
        val ShredExpr(c1: Cond, EmptyDict) = shred(e1, ctx)
        val ShredExpr(c2: Cond, EmptyDict) = shred(e2, ctx)
        ShredExpr(Or(c1, c2), EmptyDict)
      case Not(e1) =>
        val ShredExpr(c1: Cond, EmptyDict) = shred(e1, ctx)
        ShredExpr(Not(c1), EmptyDict)
    }

    case i: IfThenElse =>
      val ShredExpr(c: Cond, EmptyDict) = shred(i.cond, ctx)
      val se1 = shred(i.e1, ctx)
      if (i.e2.isDefined) {
        val se2 = shred(i.e2.get, ctx)
        ShredExpr(ShredIfThenElse(c, se1.flat, se2.flat), DictIfThenElse(c, se1.dict, se2.dict))
      }
      else
        ShredExpr(ShredIfThenElse(c, se1.flat), se1.dict)

    case _ => sys.error("Cannot shred expr " + e)
  }

}
