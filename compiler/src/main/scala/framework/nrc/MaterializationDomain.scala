package framework.nrc

import framework.common.{OpEq, TupleType}
import framework.utils.Utils.Symbol

trait MaterializationDomain extends Printer {
  this: MaterializeNRC with MaterializationContext =>

  def eliminateDomain(e: BagExpr, ctx: Context): Option[BagExpr] = {
    val iv0 = inputVars(e)
    val flatBag1 = rewriteUsingContext(e, ctx).asInstanceOf[BagExpr]    // eliminate dict vars
    val iv1 = inputVars(flatBag1).intersect(iv0)                   // keep non-dict input vars
    val flatBag2 = eliminateDomain(flatBag1, iv1)
    val iv2 = inputVars(flatBag2).intersect(iv0)
    if (iv2.isEmpty) Some(flatBag2) else None
  }

  private def eliminateDomain(b: BagExpr, iv: Set[VarRef]): BagExpr =
    replace(b, {
      // If hoisting
      case ForeachUnion(x, b1,
        BagIfThenElse(PrimitiveCmp(OpEq, p1: Project, p2: VarRef), b2, None))
          if !iv.contains(p1.tuple) && iv.contains(p2) =>

        val lbl = NewLabel(Map(p2.name -> ProjectLabelParameter(p1)))
        ForeachUnion(x, b1,
          addOutputField(KEY_ATTR_NAME -> lbl, eliminateDomain(b2, iv)))

      // If hoisting
      case ForeachUnion(x, b1,
        BagIfThenElse(PrimitiveCmp(OpEq, p1: VarRef, p2: Project), b2, None))
          if !iv.contains(p2.tuple) && iv.contains(p1) =>
        val lbl = NewLabel(Map(p1.name -> ProjectLabelParameter(p2)))
        ForeachUnion(x, b1,
          addOutputField(KEY_ATTR_NAME -> lbl, eliminateDomain(b2, iv)))

      // Dictionary iteration
      case ForeachUnion(x, KeyValueMapLookup(l: VarRef, d), b2) if iv.contains(l) =>
        val dictBag = KeyValueMapToBag(d)
        val kv = TupleVarRef(Symbol.fresh(name = "kv"), dictBag.tp.tp)
        val lbl = LabelProject(kv, KEY_ATTR_NAME)
        val attrs = kv.tp.attrTps.keys.filter(_ != KEY_ATTR_NAME).map(k => k -> kv(k)).toMap
        val newLabel = NewLabel(Map(l.name -> ProjectLabelParameter(lbl)))
        ForeachUnion(kv, dictBag,
          BagLet(x, Tuple(attrs),
            addOutputField(KEY_ATTR_NAME -> newLabel, eliminateDomain(b2, iv))))

      // Aggregation
      case GroupByKey(e, ks, vs, n) =>
        val e2 = eliminateDomain(e, iv)
        if (e2.tp.tp.attrTps.contains(KEY_ATTR_NAME))
          GroupByKey(e2, (KEY_ATTR_NAME :: ks).distinct, vs, n)
        else
          GroupByKey(e2, ks, vs, n)

      // Aggregation
      case ReduceByKey(e, ks, vs) =>
        val e2 = eliminateDomain(e, iv)
        if (e2.tp.tp.attrTps.contains(KEY_ATTR_NAME))
          ReduceByKey(e2, (KEY_ATTR_NAME :: ks).distinct, vs)
        else
          ReduceByKey(e2, ks, vs)

    }).asInstanceOf[BagExpr]

  def createLabelDomain(dict: MaterializedDict, field: String): MBag = {
    val labels = dict match {
      case d: MBag =>
        val bagVarRef = d.varRef
        val x = TupleVarRef(Symbol.fresh(), bagVarRef.tp.tp)
        DeDup(ForeachUnion(x, bagVarRef,
          Singleton(Tuple(LABEL_ATTR_NAME -> LabelProject(x, field)))))

      case d: MKeyValueMap =>
        val matDictVarRef = d.varRef
        val kvTp =
          TupleType(
            matDictVarRef.tp.valueTp.tp.attrTps +
              (KEY_ATTR_NAME -> matDictVarRef.tp.keyTp))
        val kv = TupleVarRef(Symbol.fresh(name = "kv"), kvTp)
        DeDup(
          ForeachUnion(kv, KeyValueMapToBag(matDictVarRef),
            Singleton(Tuple(LABEL_ATTR_NAME -> LabelProject(kv, field)))))

      case _ => sys.error("Error creating domain for " + dict)
    }
    val name = Symbol.fresh(dict.name + "_" + field)
    MBag(domainName(name), labels)
  }

}
