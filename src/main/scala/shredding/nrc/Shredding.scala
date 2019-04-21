package shredding.nrc

import shredding.core._

/**
  * Shredding transformation
  */
trait Shredding extends Dictionary with ShreddedPrinter with ShreddedNRCImplicits {
  this: ShreddedNRC =>

  case class ShredValue(flat: Any, flatTp: Type, dict: Dict) {
    def quote: String =
      s"""|Flat: $flat
          |Dict: ${Shredding.super[ShreddedPrinter].quote(dict)}""".stripMargin
  }

  case class ShredExpr(flat: Expr, dict: Dict) {
    def quote: String =
      s"""|Flat: ${Shredding.super[ShreddedPrinter].quote(flat)}
          |Dict: ${Shredding.super[ShreddedPrinter].quote(dict)}""".stripMargin
  }

  def shred(v: Any, tp: Type): ShredValue = ValueShredder.shred(v, tp)

  def unshred(s: ShredValue): Any = ValueShredder.unshred(s.flat, s.flatTp, s.dict)

  def shred(e: Expr): ShredExpr = ExprShredder.shred(e, Map.empty)

  def unshred(s: ShredExpr): Expr = ExprShredder.unshred(s.flat, s.dict, Map.empty)

  object ValueShredder extends Serializable{

    def flatTp(tp: Type): Type = tp match {
      case _: PrimitiveType => tp
      case _: BagType => LabelType()
      case TupleType(as) =>
        TupleType(as.map { case (n, t) => n -> flatTp(t).asInstanceOf[TupleAttributeType] })
      case _ => sys.error("Unknown flat type of " + tp)
    }

    def nestedTp(tp: Type, dict: Dict): Type = tp match {
      case _: PrimitiveType => tp
      case BagType(t) =>
        val tupleDict = dict.asInstanceOf[TupleDict]
        BagType(nestedTp(t, tupleDict).asInstanceOf[TupleType])
      case TupleType(as) =>
        val d = dict.asInstanceOf[TupleDict]
        TupleType(as.map { case (n, t) => n -> nestedTp(t, d.fields(n)).asInstanceOf[TupleAttributeType] })
      case _: LabelType =>
        val d = dict.asInstanceOf[BagDict]
        BagType(nestedTp(d.flatBagTp.tp, d.tupleDict).asInstanceOf[TupleType])
      case _ => sys.error("Unknown nested type of " + tp)
    }

    def shred(v: Any, tp: Type): ShredValue = tp match {
      case _: PrimitiveType =>
        ShredValue(v, flatTp(tp), EmptyDict)

      case BagType(t) =>
        val l = v.asInstanceOf[List[_]]
        val sl = l.map(shred(_, t))
        val flatBag = sl.map(_.flat)
        val flatBagTp = BagType(flatTp(t).asInstanceOf[TupleType])
        val tupleDict = sl.map(_.dict).reduce(_.union(_)).asInstanceOf[TupleDict]
        val lbl = Label()
        val matDict = InputBagDict(Map(lbl -> flatBag), flatBagTp, tupleDict)
        ShredValue(lbl, flatTp(tp), matDict)

      case TupleType(as) =>
        val m = v.asInstanceOf[Map[String, Any]]
        val sm = m.map { case (n, a) => n -> shred(a, as(n)) }
        val flat = sm.map { case (n, s) => n -> s.flat }
        val dict = TupleDict(sm.map { case (n, s) => n -> s.dict.asInstanceOf[AttributeDict] })
        ShredValue(flat, flatTp(tp), dict)

      case _ => sys.error("shredValue with unknown type of " + tp)
    }

    def unshred(flat: Any, flatTp: Type, dict: Dict): Any = flatTp match {
      case _: PrimitiveType => flat

      case _: BagType =>
        val l = flat.asInstanceOf[List[Any]]
        val flatBagTp = flatTp.asInstanceOf[BagType]
        val tupleDict = dict.asInstanceOf[TupleDict]
        l.map(t => unshred(t, flatBagTp.tp, tupleDict))

      case TupleType(as) =>
        val tuple = flat.asInstanceOf[Map[String, Any]]
        val tupleDict = dict.asInstanceOf[TupleDict]
        as.map { case (n, tp) => n -> unshred(tuple(n), tp, tupleDict.fields(n)) }

      case _: LabelType =>
        val lbl = flat.asInstanceOf[Label]
        val matDict = dict.asInstanceOf[InputBagDict]
        matDict.f(lbl).map(t => unshred(t, matDict.flatBagTp.tp, matDict.tupleDict))

      case _ => sys.error("unshredValue with unknown type of " + flatTp)
    }

  }

  object ExprShredder extends Serializable{

    def shred(e: Expr, ctx: Map[String, ShredExpr]): ShredExpr = e match {
      case Const(_, _) => ShredExpr(e, EmptyDict)

      case v: VarRef => ctx(v.name)

      case p: Project =>
        val ShredExpr(flat: TupleExpr, dict: TupleDict) = shred(p.tuple, ctx)
        ShredExpr(Project(flat, p.field), dict.fields(p.field))

      case ForeachUnion(x, e1, e2) =>
//        System.err.println("MILOS e1 = " + e1)
//        System.err.println("MILOS shred(e1, ctx) = " + shred(e1, ctx))
//        System.err.println("MILOS ctx = " + ctx)

        val ShredExpr(l1: LabelExpr, dict1: BagDict) = shred(e1, ctx)
        val xdef = VarDef(x.name, dict1.flatBagTp.tp)
        val ShredExpr(l2: LabelExpr, dict2: BagDict) =
          shred(e2, ctx + (x.name -> ShredExpr(VarRef(xdef), dict1.tupleDict)))
        val flat = ForeachUnion(xdef, Lookup(l1, dict1), Lookup(l2, dict2))
        val lbl = Label(inputVars(flat))
        ShredExpr(lbl, OutputBagDict(lbl, flat, dict2.tupleDict))

      case Union(e1, e2) =>
        val ShredExpr(l1: LabelExpr, dict1: BagDict) = shred(e1, ctx)
        val ShredExpr(l2: LabelExpr, dict2: BagDict) = shred(e2, ctx)
        val dict = dict1.union(dict2).asInstanceOf[BagDict]
        val flat = Union(Lookup(l1, dict1), Lookup(l2, dict2))
        val lbl = Label(inputVars(flat))
        ShredExpr(lbl, OutputBagDict(lbl, flat, dict.tupleDict))

      case Singleton(e1) =>
        val ShredExpr(flat: TupleExpr, dict: TupleDict) = shred(e1, ctx)
        val lbl = Label(inputVars(flat))
        ShredExpr(lbl, OutputBagDict(lbl, Singleton(flat), dict))

      case Tuple(fs) =>
        val sfs = fs.map(f => f._1 -> shred(f._2, ctx))
        val attrs = sfs.map(f => f._1 -> f._2.flat.asInstanceOf[TupleAttributeExpr])
        val attrTps = sfs.map(f => f._1 -> f._2.dict.asInstanceOf[AttributeDict])
        ShredExpr(Tuple(attrs), TupleDict(attrTps))

//      case Let(x, e1: BagExpr, e2) =>
//        val ShredExpr(l1: LabelExpr, dict1: BagDict) = shred(e1, ctx)
////        val x1 = VarDef(x.name, dict1.flatBagTp)
//        val x1 = VarDef(x.name, l1.tp)
//        val se2 = shred(e2, ctx + (x.name -> ShredExpr(VarRef(x1), dict1)))
//        ShredExpr(Let(x1, Lookup(l1, dict1), se2.flat), se2.dict)

      case l: Let =>
        val se1 = shred(l.e1, ctx)
//        assert(se1.dict.isPrimitiveDict, "Cannot shred Let with non-primitive dictionaries")
        val x1 = VarDef(l.x.name, se1.flat.tp)
        val se2 = shred(l.e2, ctx + (l.x.name -> ShredExpr(VarRef(x1), se1.dict)))
        ShredExpr(Let(x1, se1.flat, se2.flat), se2.dict)

      case Total(e1) =>
        val ShredExpr(lbl: LabelExpr, dict2: BagDict) = shred(e1, ctx)
        ShredExpr(Total(Lookup(lbl, dict2)), EmptyDict)

      case IfThenElse(c, e1, None) =>
        // TODO: fix this
        val ShredExpr(l1: LabelExpr, dict1: BagDict) = shred(e1, ctx)
        val flat = IfThenElse(c, Lookup(l1, dict1), None)
        val lbl = Label(inputVars(flat))
        ShredExpr(lbl, OutputBagDict(lbl, flat, dict1.tupleDict))

      case IfThenElse(c, e1, Some(e2)) =>
        // TODO: Propagate if-then-else through dictionaries
        sys.error("TODO: Propagate if-then-else through dictionaries")

      case InputBag(n, ts, tp) =>
        val ShredValue(lbl: Label, _: LabelType, dict: InputBagDict) = ValueShredder.shred(ts, tp)
        ShredExpr(lbl, OutputBagDict(lbl, InputBag(n, dict.f(lbl), dict.flatBagTp), dict.tupleDict))

      case _ => sys.error("Cannot shred expr " + e)
    }

    def unshred(flat: Expr, dict: Dict, ctx: Map[String, Expr]): Expr = flat match {
      case Const(_, _) => flat

      case v: VarRef => ctx(v.name)

      case p: Project =>
        Project(unshred(p.tuple, dict, ctx).asInstanceOf[TupleExpr], p.field)

      case ForeachUnion(x, l1: Lookup, l2: Lookup) =>
        val ue1 = unshred(l1.lbl, l1.dict, ctx).asInstanceOf[BagExpr]
        val xdef = VarDef(x.name, ue1.tp.tp)
        val ue2 = unshred(l2.lbl, l2.dict, ctx + (x.name -> VarRef(xdef))).asInstanceOf[BagExpr]
        ForeachUnion(xdef, ue1, ue2)

      case Union(l1: Lookup, l2: Lookup) =>
        val ue1 = unshred(l1.lbl, l1.dict, ctx).asInstanceOf[BagExpr]
        val ue2 = unshred(l2.lbl, l2.dict, ctx).asInstanceOf[BagExpr]
        Union(ue1, ue2)

      case Singleton(e1) =>
        val ue1 = unshred(e1, dict, ctx).asInstanceOf[TupleExpr]
        Singleton(ue1)

      case Tuple(fs) =>
        val d = dict.asInstanceOf[TupleDict]
        val ufs = fs.map { case (n, v) => n -> unshred(v, d.fields(n), ctx).asInstanceOf[TupleAttributeExpr] }
        Tuple(ufs)

//      case Let(x, l1: Lookup, e2) =>
//        val ue1 = unshred(l1.lbl, l1.dict, ctx).asInstanceOf[BagExpr]
//        val xdef = VarDef(x.name, ue1.tp)
//        val ue2 = unshred(e2, dict, ctx + (x.name -> VarRef(xdef)))
//        Let(xdef, ue1, ue2)

      case l: Let =>
        // TODO: e1 could be a label with a non-null dictionary
        val ue1 = unshred(l.e1, null, ctx)    // Dictionary must be EmptyDict or a tuple of EmptyDict
        val xdef = VarDef(l.x.name, ue1.tp)
        val ue2 = unshred(l.e2, dict, ctx + (l.x.name -> VarRef(xdef)))
        Let(xdef, ue1, ue2)

      case Total(e1: Lookup) =>
        val ue1 = unshred(e1.lbl, e1.dict, ctx).asInstanceOf[BagExpr]
        Total(ue1)

      case IfThenElse(c, e1, None) =>
        IfThenElse(c, unshred(e1, dict, ctx).asInstanceOf[BagExpr], None)

      case IfThenElse(c, e1, Some(e2)) =>
        // TODO: Propagate if-then-else through dictionaries
        sys.error("TODO: Propagate if-then-else through dictionaries")

      case InputBag(n, v, tp) =>
        val uv = ValueShredder.unshred(v, tp, dict).asInstanceOf[List[Any]]
        val tupleDict = dict.asInstanceOf[TupleDict]
        val tupleTp = ValueShredder.nestedTp(tp.tp, tupleDict).asInstanceOf[TupleType]
        InputBag(n, uv, BagType(tupleTp))

      case Lookup(lbl, bagDict) => unshred(lbl, bagDict, ctx)

      case _: Label => dict match {
        case d: OutputBagDict => unshred(d.flatBag, d.tupleDict, ctx)
        case _ => sys.error("Unknown dictionary type: " + dict)
      }

      case _ => sys.error("Cannot unshred flat expr : " + flat)
    }

  }

}
