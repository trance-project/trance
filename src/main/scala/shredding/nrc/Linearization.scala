package shredding.nrc

import shredding.Utils.Symbol
import shredding.core._

/**
  * Linearization of nested output queries
  */
trait Linearization {
  this: LinearizedNRC with Shredding =>

  val initCtxName: String = "initCtx"

  def linearize(e: ShredExpr): Sequence = e.dict match {
    case d: BagDictExpr =>
      Symbol.freshClear()

      // Construct variable reference to the initial context
      // that represents a bag containing a single label.
      // The label encapsulate the input variables.
      val ivars = e.flat.inputVars
      val labelTp = LabelType(ivars.map(v => v.name -> v.tp).toMap)
      val initCtxTp = BagType(TupleType("lbl" -> labelTp))
      val initVarRef = BagVarRef(VarDef(initCtxName, initCtxTp))
      val initCtxNamed = Named(Symbol.fresh("M_ctx"), initVarRef)
      val initCtxRef = BagVarRef(VarDef(initCtxNamed.n, initVarRef.tp))

      // Let's roll
      Sequence(initCtxNamed :: linearize(d, initCtxRef))

    case _ => sys.error("Cannot linearize dict type " + e.dict)
  }

  private def linearize(dict: BagDictExpr, ctx: BagVarRef): List[Expr] = {
    // 1. Iterate over ctx (bag of labels) and produce key-value pairs
    //    consisting of labels from ctx and flat bags from dict
    val ldef = VarDef(Symbol.fresh("l"), ctx.tp.tp)
    val lbl = LabelProject(TupleVarRef(ldef), "lbl")
    val kvpair = Tuple("k" -> lbl, "v" -> dict.lookup(lbl))
    val mFlat = ForeachUnion(ldef, ctx, Singleton(kvpair))
    val mFlatNamed = Named(Symbol.fresh("M_flat"), mFlat)
    val mFlatRef = BagVarRef(VarDef(mFlatNamed.n, mFlat.tp))

    // 2. For each label type in dict.flatBagTp.tp,
    //    create the context (bag of labels) and recur
    val labelTps = dict.tp.flatTp.tp.attrTps.filter(_._2.isInstanceOf[LabelType]).toList

    mFlatNamed ::
      labelTps.flatMap { case (n, _) =>
        val kvDef = VarDef(Symbol.fresh("kv"), mFlat.tp.tp)
        val xDef = VarDef(Symbol.fresh("xF"), dict.tp.flatTp.tp)
        val mCtx =
          ForeachUnion(kvDef, mFlatRef,
            ForeachUnion(xDef, BagProject(TupleVarRef(kvDef), "v"),
              Singleton(Tuple("lbl" -> LabelProject(TupleVarRef(xDef), n)))))
        val mCtxNamed = Named(Symbol.fresh("M_ctx"), mCtx)
        val mCtxRef = BagVarRef(VarDef(mCtxNamed.n, mCtx.tp))
        val bagDict = dict.tupleDict(n)

        bagDict match {
          case b: BagDictExpr => mCtxNamed :: linearize(b, mCtxRef)
          case b => sys.error("Unknown dictionary " + b)
        }
      }
  }
}
