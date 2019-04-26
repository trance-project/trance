package shredding.nrc

import shredding.core._

/**
  * Shredding transformation
  */
trait Shredding extends ShreddedNRCImplicits {
  this: ShreddedNRC =>

  def shredTp(tp: Type): Type = tp match {
    case _: PrimitiveType => tp
    case _: BagType => LabelType()
    case TupleType(as) =>
      TupleType(as.map { case (n, t) => n -> shredTp(t).asInstanceOf[TupleAttributeType] })
    case _ => sys.error("Unknown flat type of " + tp)
  }

  def shredDictTp(tp: Type): DictType = tp match {
    case _: PrimitiveType => EmptyDictType
    case BagType(t) =>
      BagDictType(
        BagType(shredTp(t).asInstanceOf[TupleType]),
        shredDictTp(t).asInstanceOf[TupleDictType])
    case TupleType(fs) =>
      TupleDictType(fs.map { case (f, t) => f -> shredDictTp(t).asInstanceOf[TupleDictAttributeType] })
    case _ => sys.error("Unknown dict type of " + tp)
  }

  def shred(e: Expr): ShredExpr = shred(e, Map.empty)

  def shred(e: Expr, ctx: Map[String, ShredExpr]): ShredExpr = e match {
      case Const(_, _) => ShredExpr(e, EmptyDict)

      case v: VarRef if ctx.contains(v.name) => ctx(v.name)

      case v: VarRef => v.tp match {
        case _: PrimitiveType =>
          ShredExpr(v, EmptyDict)
        case t: TupleType =>
          ShredExpr(
            TupleVarRef(VarDef(v.name + "^F", shredTp(t))),
            TupleDictVarRef(VarDef(v.name + "^D", shredDictTp(t)))
          )
        case t: BagType =>
          ShredExpr(
            LabelVarRef(VarDef(v.name + "^F", shredTp(t))),
            BagDictVarRef(VarDef(v.name + "^D", shredDictTp(t)))
          )
        case t => sys.error("Cannot shred variable ref of type " + t)
      }

      case p: Project =>
        val ShredExpr(flat: TupleExpr, dict: TupleDictExpr) = shred(p.tuple, ctx)
        ShredExpr(ShredProject(flat, p.field), dict(p.field))

      case ForeachUnion(x, e1, e2) =>
        // Shredding e1
        val ShredExpr(l1: LabelExpr, dict1: BagDictExpr) = shred(e1, ctx)
        val resolved1 = dict1.lookup(l1)
        // Create x^F and x^D
        val xFlat = VarDef(x.name, resolved1.tp.tp)
        // Shredding e2
        val ctx2 = ctx + (x.name -> ShredExpr(ShredVarRef(xFlat), dict1.tupleDict))
        val ShredExpr(l2: LabelExpr, dict2: BagDictExpr) = shred(e2, ctx2)
        val resolved2 = dict2.lookup(l2)
        // Output flat bag
        val flat = ForeachUnion(xFlat, resolved1, resolved2)
        ShredExpr(Label(inputVars(flat)), BagDict(flat, dict2.tupleDict))

      case Union(e1, e2) =>
        val ShredExpr(l1: LabelExpr, dict1: BagDictExpr) = shred(e1, ctx)
        val ShredExpr(l2: LabelExpr, dict2: BagDictExpr) = shred(e2, ctx)
        val dict = dict1.union(dict2).asInstanceOf[BagDictExpr]
        val flat = Union(dict1.lookup(l1), dict2.lookup(l2))
        ShredExpr(Label(inputVars(flat)), BagDict(flat, dict.tupleDict))

      case Singleton(e1) =>
        val ShredExpr(flat: TupleExpr, dict: TupleDictExpr) = shred(e1, ctx)
        val lbl = Label(inputVars(flat))
        ShredExpr(lbl, BagDict(Singleton(flat), dict))

      case Tuple(fs) =>
        val shredFs = fs.map(f => f._1 -> shred(f._2, ctx))
        val flatFs = shredFs.map(f => f._1 -> f._2.flat.asInstanceOf[TupleAttributeExpr])
        val dictFs = shredFs.map(f => f._1 -> f._2.dict.asInstanceOf[TupleDictAttributeExpr])
        ShredExpr(Tuple(flatFs), TupleDict(dictFs))

      case l: Let =>
        val se1 = shred(l.e1, ctx)
        val xFlat = VarDef(l.x.name, se1.flat.tp)
        val se2 = shred(l.e2, ctx + (l.x.name -> ShredExpr(ShredVarRef(xFlat), se1.dict)))
        ShredExpr(ShredLet(xFlat, se1.flat, se2.flat), se2.dict)

      case Total(e1) =>
        val ShredExpr(lbl: LabelExpr, dict2: BagDictExpr) = shred(e1, ctx)
        ShredExpr(Total(dict2.lookup(lbl)), EmptyDict)

      case i: IfThenElse =>
        if (i.e2.isDefined) {
          val se1 = shred(i.e1, ctx)
          val se2 = shred(i.e2.get, ctx)
          ShredExpr(
            ShredIfThenElse(i.cond, se1.flat, se2.flat),
            DictIfThenElse(i.cond, se1.dict, se2.dict))
        }
        else {
          val se1 = shred(i.e1, ctx)
          ShredExpr(ShredIfThenElse(i.cond, se1.flat), se1.dict)
        }

      case _ => sys.error("Cannot shred expr " + e)
    }

}
