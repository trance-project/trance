package shredding.nrc

import shredding.core.{BagType, LabelType, MatDictType, OpEq, TupleType, VarDef}
import shredding.utils.Utils.Symbol

trait MaterializationDomain extends Printer {
  this: MaterializeNRC with MaterializationContext =>

  def eliminateDomain(lblTp: LabelType, b: BagExpr, ctx: Context): Option[(BagExpr, Context)] = {
    val newCtx = lblTp.attrTps.foldLeft (ctx) {
      case (acc, (n, t)) => acc.addVarDef(VarDef(n, t))
    }
    val (flatBag: BagExpr, flatCtx) = rewriteUsingContext(b, newCtx)
    eliminateDomain(flatBag).map(_ -> flatCtx)
  }

  private def eliminateDomain(b: BagExpr): Option[BagExpr] = {
    val iv = inputVars(b).filterNot(x =>
      x.tp.isInstanceOf[MatDictType] || x.tp.isInstanceOf[BagType])
    eliminateDomainIfHoisting(b, iv) orElse
      eliminateDomainDictIteration(b, iv) orElse
        eliminateDomainAggregation(b, iv)
  }

  private def eliminateDomainIfHoisting(b: BagExpr, iv: Set[VarRef]): Option[BagExpr] = b match {
    case ForeachUnion(x, b1, BagIfThenElse(c, Singleton(Tuple(fs)), None)) => c match {
      case PrimitiveCmp(OpEq, p1: Project, p2: VarRef)
        if !iv.contains(p1.tuple) && iv.contains(p2) =>
        val lbl = NewLabel(Map(p2.name -> ProjectLabelParameter(p1)))
        val fs1 = fs + (KEY_ATTR_NAME -> lbl)
        Some(
          GroupByKey(
            ForeachUnion(x, b1, Singleton(Tuple(fs1))),
            List(KEY_ATTR_NAME),
            fs.keys.toList,
            VALUE_ATTR_NAME))

      case PrimitiveCmp(OpEq, p1: VarRef, p2: Project)
        if !iv.contains(p2.tuple) && iv.contains(p1) =>
        val lbl = NewLabel(Map(p1.name -> ProjectLabelParameter(p2)))
        val fs1 = fs + (KEY_ATTR_NAME -> lbl)
        Some(
          GroupByKey(
            ForeachUnion(x, b1, Singleton(Tuple(fs1))),
            List(KEY_ATTR_NAME),
            fs.keys.toList,
            VALUE_ATTR_NAME))
      case _ => None
    }

    case ForeachUnion(x, b1, BagIfThenElse(c, b2, None)) => c match {
      case PrimitiveCmp(OpEq, p1: Project, p2: VarRef)
        if !iv.contains(p1.tuple) && iv.contains(p2) =>
        val lbl = NewLabel(Map(p2.name -> ProjectLabelParameter(p1)))
        Some(
          ReduceByKey(
            ForeachUnion(x, b1,
              Singleton(Tuple(
                KEY_ATTR_NAME -> lbl,
                VALUE_ATTR_NAME -> b2))),
            List(KEY_ATTR_NAME),
            List(VALUE_ATTR_NAME)))

      case PrimitiveCmp(OpEq, p1: VarRef, p2: Project)
        if !iv.contains(p2.tuple) && iv.contains(p1) =>
        val lbl = NewLabel(Map(p1.name -> ProjectLabelParameter(p2)))
        Some(
          ReduceByKey(
            ForeachUnion(x, b1,
              Singleton(Tuple(
                KEY_ATTR_NAME -> lbl,
                VALUE_ATTR_NAME -> b2))),
            List(KEY_ATTR_NAME),
            List(VALUE_ATTR_NAME)))

      case _ => None
    }

    case _ => None
  }

  private def eliminateDomainDictIteration(b: BagExpr, iv: Set[VarRef]): Option[BagExpr] = b match {
    case ForeachUnion(x, MatDictLookup(l: VarRef, d), b2) if iv == Set(l) =>
      val dictBag = MatDictToBag(d)
      val kv = TupleVarRef(Symbol.fresh(name = "kv"), dictBag.tp.tp)
      val lbl = LabelProject(kv, KEY_ATTR_NAME)
      Some(
        ForeachUnion(kv, dictBag,
          Singleton(Tuple(
            KEY_ATTR_NAME -> NewLabel(Map(l.name -> ProjectLabelParameter(lbl))),
            VALUE_ATTR_NAME ->
              ForeachUnion(x, kv(VALUE_ATTR_NAME).asInstanceOf[BagExpr], b2)))))
    case  _ => None
  }

  private def eliminateDomainAggregation(b: Expr, iv: Set[VarRef]): Option[BagExpr] = b match {
    case ReduceByKey(e, ks, vs) =>
      eliminateDomain(e) match {
        case Some(ForeachUnion(kv, b1, Singleton(Tuple(fs)))) =>
          Some(
            ForeachUnion(kv, b1,
              Singleton(Tuple(
                KEY_ATTR_NAME -> fs(KEY_ATTR_NAME),
                VALUE_ATTR_NAME -> ReduceByKey(fs(VALUE_ATTR_NAME).asInstanceOf[BagExpr], ks, vs)
              )))
          )
        case Some(_) =>
          sys.error("Missed opportunity for domain elimination")

        case _ => None
      }
    case _ => None
  }

  def createLabelDomain(varRef: VarRef, topLevel: Boolean, field: String): Assignment = {
    val domain = if (topLevel) {
      val bagVarRef = varRef.asInstanceOf[BagVarRef]
      val x = TupleVarRef(Symbol.fresh(), bagVarRef.tp.tp)
      val lbl = LabelProject(x, field)
      DeDup(
        ForeachUnion(x, bagVarRef, Singleton(Tuple(LABEL_ATTR_NAME -> lbl)))
      )
    }
    else {
      val matDictVarRef = varRef.asInstanceOf[MatDictVarRef]
      val kvTp =
        TupleType(
          KEY_ATTR_NAME -> matDictVarRef.tp.keyTp,
          VALUE_ATTR_NAME -> matDictVarRef.tp.valueTp
        )
      val kv = TupleVarRef(Symbol.fresh(name = "kv"), kvTp)
      val values = BagProject(kv, VALUE_ATTR_NAME)
      val x = TupleVarRef(Symbol.fresh(), values.tp.tp)
      val lbl = LabelProject(x, field)
      DeDup(
        ForeachUnion(kv, MatDictToBag(matDictVarRef),
          ForeachUnion(x, values, Singleton(Tuple(LABEL_ATTR_NAME -> lbl))))
      )
    }

    val bagRef = BagVarRef(domainName(Symbol.fresh(field + "_")), domain.tp)
    Assignment(bagRef.name, domain)
  }

}
