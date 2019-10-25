package shredding.nrc

import shredding.utils.Utils.Symbol
import shredding.core._

/**
  * Linearization of nested output queries
  */
trait Linearization {
  this: LinearizedNRC with Shredding with Optimizer with Printer =>

  val initCtxName: String = "initCtx"

  def linearizeNoDomains(e: ShredExpr): Sequence = e.dict match {
    case d: BagDict =>
      Symbol.freshClear()
      Sequence(linearizeNoDomains(d))
    case _ => sys.error("Cannot linearize bag dict type " + e.dict)
  }

  def linearizeNoDomains(dict: BagDict): List[Expr] = {
    val flatBagExpr = dict.flat
    //println("before")
    //println(flatBagExpr)
    val flatBagExprRewritten = nestingRewriteLossy(flatBagExpr)
    //println("after")
    //println(quote(flatBagExprRewritten))
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

  def linearize(e: ShredExpr): Sequence = e.dict match {
    case d: BagDictExpr =>
      Symbol.freshClear()

      // Construct variable reference to the initial context
      // that represents a bag containing e.flat.
      //val bagFlat = Singleton(Tuple("lbl" -> e.flat.asInstanceOf[TupleAttributeExpr]))
      val bagFlat = Singleton(Tuple("lbl" ->
                      NewLabel(labelParameters(e.flat)).asInstanceOf[TupleAttributeExpr]))
      
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
        val bagDict = dict.tupleDict(n)

        bagDict match {
          case b: BagDictExpr => mCtxNamed :: linearize(b, mCtxRef)
          case b => sys.error("Unknown dictionary " + b)
        }
      }
  }

}
