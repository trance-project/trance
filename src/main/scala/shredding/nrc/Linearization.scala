package shredding.nrc

import shredding.utils.Utils.Symbol
import shredding.core._

/**
  * Linearization of nested output queries
  */
trait Linearization {
  this: LinearizedNRC with Shredding with Optimizer =>

  type DictMapper = Map[BagDictExpr, BagVarRef]

  case class MaterializationInfo(seq: Sequence, dictMapper: DictMapper) {

    def this(e: Expr, d: DictMapper) = this(Sequence(List(e)), d)

    def append(m: MaterializationInfo): MaterializationInfo =
      MaterializationInfo(Sequence(seq.exprs ++ m.seq.exprs), dictMapper ++ m.dictMapper)

    def prepend(e: Expr): MaterializationInfo =
      MaterializationInfo(Sequence(e :: seq.exprs), dictMapper)
  }

  def materialize(e: ShredExpr): MaterializationInfo = e.dict match {
    case d: BagDictExpr =>
      Symbol.freshClear()

      // Construct initial context containing e.flat
      val bagFlat = Singleton(Tuple("lbl" -> e.flat.asInstanceOf[TupleAttributeExpr]))
      val initCtxNamed = Named(VarDef(Symbol.fresh("M_ctx"), bagFlat.tp), bagFlat)
      val initCtxRef = BagVarRef(initCtxNamed.v)

      // Recur
      val matInfo = materializeDomains(d, initCtxRef)

      matInfo.prepend(initCtxNamed)

    case _ => sys.error("Cannot linearize dict type " + e.dict)
  }

  private def materializeDomains(dict: BagDictExpr, ctx: BagVarRef): MaterializationInfo = {
    val ldef = VarDef(Symbol.fresh("l"), ctx.tp.tp)
    val lbl = LabelProject(TupleVarRef(ldef), "lbl")

    // Return expr creating pairs of labels from ctx and flat bags from dict
    def kvPairs(b: BagExpr) =
      ForeachUnion(ldef, ctx,
        BagExtractLabel(
          LabelProject(TupleVarRef(ldef), "lbl"),
          Singleton(Tuple("k" -> lbl, "v" -> b))))

    def materializeDictionary(kvPairs: BagExpr, dict: BagDictExpr): MaterializationInfo = {
      // 1. Create named materialized expression
      val mDictNamed = Named(VarDef(Symbol.fresh("M_dict"), kvPairs.tp), kvPairs)
      val mDictRef = BagVarRef(mDictNamed.v)

      // 2. Associate dict with its materialized expression
      val dictMapper = Map(dict -> mDictRef)

      // 3. Create materialization strategy
      val matStrategy = new MaterializationInfo(mDictNamed, dictMapper)

      // 4. For each label type in dict.flatBagTp.tp,
      //    create the context (bag of labels) and recur
      val labelTps = dict.tp.flatTp.tp.attrTps.filter(_._2.isInstanceOf[LabelType]).toList

      val childMatStrategies: List[MaterializationInfo] =
        labelTps.map { case (n, tp) =>

//          val lblTp = tp.asInstanceOf[LabelType]
//          val parentLblTp = ctx.tp.tp("lbl").asInstanceOf[LabelType]
//
//          if (lblTp.attrTps.toSet.subsetOf(parentLblTp.attrTps.toSet))
//            dict.tupleDict(n) match {
//              case b: BagDictExpr => materializeDomains(b, ctx)
//              case b => sys.error("Unknown dictionary " + b)
//            }
//          else {
            val kvDef = VarDef(Symbol.fresh("kv"), kvPairs.tp.tp)
            val xDef = VarDef(Symbol.fresh("xF"), dict.tp.flatTp.tp)
            val mCtx =
              DeDup(ForeachUnion(kvDef, mDictRef,
                ForeachUnion(xDef, BagProject(TupleVarRef(kvDef), "v"),
                  Singleton(Tuple("lbl" -> LabelProject(TupleVarRef(xDef), n))))))
            val mCtxNamed = Named(VarDef(Symbol.fresh("M_ctx"), mCtx.tp), mCtx)
            val mCtxRef = BagVarRef(mCtxNamed.v)

            dict.tupleDict(n) match {
              case b: BagDictExpr =>
                val matInfo = materializeDomains(b, mCtxRef)
                matInfo.prepend(mCtxNamed)
              case b => sys.error("Unknown dictionary " + b)
            }
//          }
        }

      (matStrategy :: childMatStrategies).reduce(_ append _)
    }

    dict match {
      case BagDict(l, Union(b1, b2), TupleDictUnion(d1, d2)) if lbl.tp == l.tp =>
        val dict1 = BagDict(null, b1, d1)
        val dict2 = BagDict(null, b2, d2)
        materializeDictionary(kvPairs(b1), dict1) append
          materializeDictionary(kvPairs(b2), dict2)
      case _ =>
        val b = optimize(dict.lookup(lbl)).asInstanceOf[BagExpr]
        materializeDictionary(kvPairs(b), dict)
    }
  }

  def unshred(e: ShredExpr, dictMapper: Map[BagDictExpr, BagVarRef]): BagExpr = e.dict match {
    case d: BagDictExpr => unshred(e.flat.asInstanceOf[LabelExpr], d, dictMapper)
    case _ => sys.error("Cannot unshred dict type " + e.dict)
  }

  private def unshred(lbl: LabelExpr, dict: BagDictExpr, dictMapper: Map[BagDictExpr, BagVarRef]): BagExpr = {

    def unshredDictionary(d: BagDictExpr): BagExpr = {
      val matDict = dictMapper(d)
      val kvdef = VarDef(Symbol.fresh("kv"), matDict.tp.tp)
      val kvref = TupleVarRef(kvdef)

      val valueTp = kvref.tp.attrTps("v").asInstanceOf[BagType]
      val labelTypeExist = valueTp.tp.attrTps.exists(_._2.isInstanceOf[LabelType])

      val bagExpr = if (!labelTypeExist) BagProject(kvref, "v")
      else {
        val bag = BagProject(kvref, "v")
        val tdef = VarDef(Symbol.fresh("t"), bag.tp.tp)
        val tref = TupleVarRef(tdef)

        ForeachUnion(tdef, bag,
          Singleton(Tuple(
            tref.tp.attrTps.map {
              case (n, _: LabelType) =>
                n-> unshred(LabelProject(tref, n), d.tupleDict(n).asInstanceOf[BagDictExpr], dictMapper)
              case (n, _) => n -> tref(n)
            }
          )))
      }
      ForeachUnion(kvdef, matDict,
        BagIfThenElse(Cmp(OpEq, lbl, LabelProject(kvref, "k")), bagExpr, None))
    }

    dict match {
      case BagDict(_, Union(b1, b2), TupleDictUnion(d1, d2)) =>
        val dict1 = BagDict(null, b1, d1)
        val dict2 = BagDict(null, b2, d2)
        Union(unshredDictionary(dict1), unshredDictionary(dict2))
      case _ =>
        unshredDictionary(dict)
    }
  }

  /* Old linearization approach w/o unshredding */

  def linearize(e: ShredExpr): Sequence = e.dict match {
    case d: BagDictExpr =>
      Symbol.freshClear()

      // Construct variable reference to the initial context
      // that represents a bag containing e.flat.
      val bagFlat = Singleton(Tuple("lbl" -> e.flat.asInstanceOf[TupleAttributeExpr]))
      val initCtxNamed = Named(VarDef(Symbol.fresh("M_ctx"), bagFlat.tp), bagFlat)
      val initCtxRef = BagVarRef(initCtxNamed.v)

      // Let's roll
      Sequence(initCtxNamed :: linearize(d, initCtxRef))

    case _ => sys.error("Cannot linearize dict type " + e.dict)
  }

  private def linearize(dict: BagDictExpr, ctx: BagVarRef): List[Expr] = {
    // 1. Iterate over ctx (bag of labels) and produce key-value pairs
    //    consisting of labels from ctx and flat bags from dict
    val ldef = VarDef(Symbol.fresh("l"), ctx.tp.tp)
    val lbl = LabelProject(TupleVarRef(ldef), "lbl")
    val kvpair = Tuple("k" -> lbl, "v" -> optimize(dict.lookup(lbl)).asInstanceOf[BagExpr])
    val mFlat = ForeachUnion(ldef, ctx,
      BagExtractLabel(LabelProject(TupleVarRef(ldef), "lbl"), Singleton(kvpair)))
    val mFlatNamed = Named(VarDef(Symbol.fresh("M_flat"), mFlat.tp), mFlat)
    val mFlatRef = BagVarRef(mFlatNamed.v)

    // 2. For each label type in dict.flatBagTp.tp,
    //    create the context (bag of labels) and recur
    val labelTps = dict.tp.flatTp.tp.attrTps.filter(_._2.isInstanceOf[LabelType]).toList

    mFlatNamed ::
      labelTps.flatMap { case (n, _) =>
        val kvDef = VarDef(Symbol.fresh("kv"), mFlat.tp.tp)
        val xDef = VarDef(Symbol.fresh("xF"), dict.tp.flatTp.tp)
        val mCtx =
          DeDup(ForeachUnion(kvDef, mFlatRef,
            ForeachUnion(xDef, BagProject(TupleVarRef(kvDef), "v"),
              Singleton(Tuple("lbl" -> LabelProject(TupleVarRef(xDef), n))))))
        val mCtxNamed = Named(VarDef(Symbol.fresh("M_ctx"), mCtx.tp), mCtx)
        val mCtxRef = BagVarRef(mCtxNamed.v)

        dict.tupleDict(n) match {
          case b: BagDictExpr => mCtxNamed :: linearize(b, mCtxRef)
          case b => sys.error("Unknown dictionary " + b)
        }
      }
  }

  /* Experimental */

  def linearizeNoDomains(e: ShredExpr): Sequence = e.dict match {
    case d: BagDict =>
      Symbol.freshClear()
      Sequence(linearizeNoDomains(d))
    case _ => sys.error("Cannot linearize bag dict type " + e.dict)
  }

  def linearizeNoDomains(dict: BagDict): List[Expr] = {
    val flatBagExpr = dict.flat
    val flatBagExprRewritten = nestingRewriteLossy(flatBagExpr)
    val mFlatNamed = Named(VarDef(Symbol.fresh("M_flat"), flatBagExpr.tp), flatBagExprRewritten)

    val labelTps = dict.tp.flatTp.tp.attrTps.filter(_._2.isInstanceOf[LabelType]).toList
    mFlatNamed ::
      labelTps.flatMap { case (n, _) =>
        dict.tupleDict(n) match {
          case b: BagDict => linearizeNoDomains(b)
          case b => sys.error("Unknown dictionary " + b)
        }
      }
  }

}
