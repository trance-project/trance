package shredding.nrc2

import shredding.core._

trait ShreddingTransform {
  this: NRC with NRCTransforms =>

  case class ShredValue(flat: Any, flatTp: Type, dict: Dict) {
    def quote: String =
      s"""|Flat: $flat
          |Dict: ${Printer.quote(dict)}""".stripMargin
  }

  case class ShredExpr(flat: Expr, dict: Dict) {
    def quote: String =
      s"""|Flat: ${Printer.quote(flat)}
          |Dict: ${Printer.quote(dict)}""".stripMargin
  }

  /**
    * Shredding transformation
    */
  object Shredder {

    def flatTp(tp: Type): Type = tp match {
      case _: PrimitiveType => tp
      case _: BagType => LabelType("id" -> IntType)
      case TupleType(as) =>
        TupleType(as.map { case (n, t) => n -> flatTp(t).asInstanceOf[TupleAttributeType] })
      case _ => sys.error("unknown flat type of " + tp)
    }

    def nestedTp(tp: Type, dict: Dict): Type = tp match {
      case _: PrimitiveType => tp
      case _: LabelType =>
        val d = dict.asInstanceOf[BagDict]
        BagType(nestedTp(d.flatBagTp.tp, d.tupleDict).asInstanceOf[TupleType])
      case bagTp: BagType =>
        val tupleDict = dict.asInstanceOf[TupleDict]
        BagType(nestedTp(bagTp.tp, tupleDict).asInstanceOf[TupleType])
      case tupleTp: TupleType =>
        val d = dict.asInstanceOf[TupleDict]
        TupleType(tupleTp.attrs.map { case (n, t) =>
          n -> nestedTp(t, d.fields(n)).asInstanceOf[TupleAttributeType]})
      case _ => sys.error("unknown nested type of " + tp)
    }

    def shredValue(v: Any, tp: Type): ShredValue = tp match {
      case _: PrimitiveType =>
        ShredValue(v, flatTp(tp), EmptyDict)

      case BagType(tp2) =>
        val l = v.asInstanceOf[List[_]]
        val sl = l.map(shredValue(_, tp2))
        val flatBag = sl.map(_.flat)
        val flatBagTp = BagType(flatTp(tp2).asInstanceOf[TupleType])
        val tupleDict = sl.map(_.dict).reduce(_.union(_)).asInstanceOf[TupleDict]
        val lbl = LabelId()
        val matDict = InputBagDict(Map(lbl -> flatBag), flatBagTp, tupleDict)
        ShredValue(lbl, flatTp(tp), matDict)

      case TupleType(as) =>
        val m = v.asInstanceOf[Map[String, Any]]
        val sm = m.map { case (n, a) => n -> shredValue(a, as(n)) }
        val flat = sm.map { case (n, s) => n -> s.flat }
        val dict = TupleDict(sm.map { case (n, s) => n -> s.dict.asInstanceOf[AttributeDict] })
        ShredValue(flat, flatTp(tp), dict)

      case _ => sys.error("shredValue with unknown type of " + tp)
    }

    def unshredValue(s: ShredValue): Any = unshredValue(s.flat, s.flatTp, s.dict)

    def unshredValue(flat: Any, flatTp: Type, dict: Dict): Any = flatTp match {
      case _: PrimitiveType => flat

      case _: LabelType =>
        val lbl = flat.asInstanceOf[LabelId]
        val matDict = dict.asInstanceOf[InputBagDict]
        matDict.f(lbl).map(t => unshredValue(t, matDict.flatBagTp.tp, matDict.tupleDict))

      case _: BagType =>
        val l = flat.asInstanceOf[List[Any]]
        val flatBagTp = flatTp.asInstanceOf[BagType]
        val tupleDict = dict.asInstanceOf[TupleDict]
        l.map(t => unshredValue(t, flatBagTp.tp, tupleDict))

      case TupleType(as) =>
        val tuple = flat.asInstanceOf[Map[String, Any]]
        val tupleDict = dict.asInstanceOf[TupleDict]
        as.map { case (n, tp) => n -> unshredValue(tuple(n), tp, tupleDict.fields(n)) }

      case _ => sys.error("unshredValue with unknown type of " + flatTp)
    }

    def apply(e: Expr): ShredExpr = shred(e, Map.empty)

    def ivars(e: Expr): Map[String, VarRef] = Map.empty     // TODO: compute ivars

    private def shred(e: Expr, ctx: Map[String, ShredExpr]): ShredExpr = e match {
      case Const(_, _) => ShredExpr(e, EmptyDict)

      case BagConst(v, tp) =>
        val ShredValue(lbl: LabelId, _: LabelType, dict: InputBagDict) = shredValue(v, tp)
        ShredExpr(lbl, OutputBagDict(lbl, BagConst(dict.f(lbl), dict.flatBagTp), dict.tupleDict))

      case p: Project =>
        val ShredExpr(flat: TupleExpr, dict: TupleDict) = shred(p.tuple, ctx)
        ShredExpr(Project(flat, p.field), dict.fields(p.field))

      case v: VarRef => ctx(v.name)

      case ForeachUnion(x, e1, e2) =>
        val ShredExpr(l1: LabelExpr, dict1: BagDict) = shred(e1, ctx)
        val xdef = VarDef(x.name, dict1.flatBagTp.tp)
        val ShredExpr(l2: LabelExpr, dict2: BagDict) =
          shred(e2, ctx + (x.name -> ShredExpr(VarRef(xdef), dict1.tupleDict)))
        val lbl = LabelId()
        ShredExpr(lbl, OutputBagDict(lbl, ForeachUnion(xdef, Lookup(l1, dict1), Lookup(l2, dict2)), dict2.tupleDict))

      case Union(e1, e2) =>
        val ShredExpr(l1: LabelExpr, dict1: BagDict) = shred(e1, ctx)
        val ShredExpr(l2: LabelExpr, dict2: BagDict) = shred(e2, ctx)
        val dict = dict1.union(dict2).asInstanceOf[BagDict]
        val lbl = LabelId()
        ShredExpr(lbl, OutputBagDict(lbl, Union(Lookup(l1, dict1), Lookup(l2, dict2)), dict.tupleDict))

      case Singleton(e1) =>
        val ShredExpr(flat: TupleExpr, dict: TupleDict) = shred(e1, ctx)
        val lbl = LabelId()
        ShredExpr(lbl, OutputBagDict(lbl, Singleton(flat), dict))

      case Tuple(fs) =>
        val sfs = fs.map(f => f._1 -> shred(f._2, ctx))
        val attrs = sfs.map(f => f._1 -> f._2.flat.asInstanceOf[TupleAttributeExpr])
        val attrTps = sfs.map(f => f._1 -> f._2.dict.asInstanceOf[AttributeDict])
        ShredExpr(Tuple(attrs), TupleDict(attrTps))

      case Let(x, e1, e2) =>
        val se1 = shred(e1, ctx)
        val x1 = VarDef(x.name, se1.flat.tp)
        val se2 = shred(e2, ctx + (x.name -> ShredExpr(VarRef(x1), se1.dict)))
        ShredExpr(Let(x1, se1.flat, se2.flat), se2.dict)

      case Mult(e1, e2) =>
        val ShredExpr(flat1: TupleExpr, dict1: Dict) = shred(e1, ctx)
        val ShredExpr(l2: LabelExpr, dict2: BagDict) = shred(e2, ctx)
        ShredExpr(Mult(flat1, Lookup(l2, dict2)), EmptyDict)

      case IfThenElse(c, e1, None) =>
        val ShredExpr(l1: LabelExpr, dict1: BagDict) = shred(e1, ctx)
        val lbl = LabelId()
        ShredExpr(lbl, OutputBagDict(lbl, IfThenElse(c, Lookup(l1, dict1), None), dict1.tupleDict))

      case IfThenElse(c, e1, Some(e2)) =>
        sys.error("TODO")

      case Relation(n, ts, tp) =>
        val ShredValue(lbl: LabelId, _: LabelType, dict: InputBagDict) = shredValue(ts, tp)
        ShredExpr(lbl, OutputBagDict(lbl, Relation(n, dict.f(lbl), dict.flatBagTp), dict.tupleDict))

      case _ => sys.error("not implemented")
    }

    def unshred(s: ShredExpr): Expr = unshred(s.flat, s.dict, Map.empty)

    def unshred(flat: Expr, dict: Dict, ctx: Map[String, Expr]): Expr = flat match {
      case Const(_, _) => flat

      //      case BagConst(l, tp) =>
      //        val d = dict.asInstanceOf[InputBagDict]
      //        val uv = unshredValue(d.f(l), tp, d.tupleDict).asInstanceOf[List[Any]]
      //        BagConst(uv, ???)

      case p: Project =>
        Project(unshred(p.tuple, dict, ctx).asInstanceOf[TupleExpr], p.field)

      case v: VarRef => ctx(v.name)

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

      case Let(x, l1: Lookup, e2) =>
        val ue1 = unshred(l1.lbl, l1.dict, ctx)
        val xdef = VarDef(x.name, ue1.tp)
        val ue2 = unshred(e2, dict, ctx + (x.name -> VarRef(xdef)))
        Let(xdef, ue1, ue2)

      case Mult(e1, e2: Lookup) =>
        // TODO: need to keep dict of e1 when shredding Mult
        val d = TupleDict(e1.tp.attrs.map(x => x._1 -> EmptyDict))
        val ue1 = unshred(e1, d, ctx).asInstanceOf[TupleExpr]
        val ue2 = unshred(e2.lbl, e2.dict, ctx).asInstanceOf[BagExpr]
        Mult(ue1, ue2)

      case IfThenElse(c, e1, None) =>
        IfThenElse(c, unshred(e1, dict, ctx).asInstanceOf[BagExpr], None)

      case IfThenElse(c, e1, Some(e2)) =>
        sys.error("TODO")

      case _: LabelId => dict match {
//        case d: InputBagDict => BagConst(d.f(l), d.flatBagTp)
        case d: OutputBagDict => unshred(d.flat, d.tupleDict, ctx)
        case _ => sys.error("Unknown dictionary type: " + dict)
      }

      case Lookup(lbl, bagDict) => unshred(lbl, bagDict, ctx)

      case Relation(n, v, tp) =>
        val uv = unshredValue(v, tp, dict).asInstanceOf[List[Any]]
        val tupleDict = dict.asInstanceOf[TupleDict]
        val tupleTp = nestedTp(tp.tp, tupleDict).asInstanceOf[TupleType]
        Relation(n, uv, BagType(tupleTp))

      case _ => sys.error("Unknown expr in unshred: " + flat)
    }

  }
}
