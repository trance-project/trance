package shredding.nrc

import shredding.Utils.Symbol
import shredding.core._

/**
  * Linearization of nested output queries
  */
//trait Linearization extends Optimizer {
//  this: ShreddedNRC with Shredding =>
//
//  object EmptyCtx {
//    val varRef = BagVarRef(VarDef(
//      Symbol.fresh("emptyCtx"),
//      BagType(TupleType("lbl" -> LabelType()))
//    ))
//
//    val value = List(Map("lbl" -> Map.empty[String, Any]))
//  }
//
//  def linearize(e: ShredExpr): Sequence = e.dict match {
//    case d: OutputBagDict =>
//      Symbol.freshClear()
//      val emptyCtxNamed = Named(Symbol.fresh("M_ctx"), EmptyCtx.varRef)
//      val emptyCtxRef = BagVarRef(VarDef(emptyCtxNamed.n, EmptyCtx.varRef.tp))
//      Sequence(emptyCtxNamed :: linearize(d, emptyCtxRef))
//    case _ => sys.error("Cannot linearize dict type " + e.dict)
//  }
//
//  private def linearize(dict: OutputBagDict, ctx: BagVarRef): List[Expr] = {
//    // 1. Iterate over ctx (bag of labels) and produce key-value pairs
//    //    consisting of labels from ctx and flat bags from dict
//    val ldef = VarDef(Symbol.fresh("l"), ctx.tp.tp)
//    val kvpair = Tuple(
//      "k" -> Project(TupleVarRef(ldef), "lbl"),
//      "v" -> betaReduce(dict.flatBag).asInstanceOf[BagExpr])
//    val mFlat = ForeachUnion(ldef, ctx, Singleton(kvpair))
//    val mFlatNamed = Named(Symbol.fresh("M_flat"), mFlat)
//    val mFlatRef = BagVarRef(VarDef(mFlatNamed.n, mFlat.tp))
//
//    // 2. For each label type in dict.flatBagTp.tp,
//    //    create the context (bag of labels) and recur
//    val labelTps = dict.flatBagTp.tp.attrTps.filter(_._2.isInstanceOf[LabelType]).toList
//
//    mFlatNamed ::
//      labelTps.flatMap { case (n, _) =>
//        val kvDef = VarDef(Symbol.fresh("kv"), mFlat.tp.tp)
//        val xDef = VarDef(Symbol.fresh("xF"), dict.flatBagTp.tp)
//        val mCtx =
//          ForeachUnion(kvDef, mFlatRef,
//            ForeachUnion(xDef, BagProject(TupleVarRef(kvDef), "v"),
//              Singleton(Tuple("lbl" -> Project(TupleVarRef(xDef), n)))))
//        val mCtxNamed = Named(Symbol.fresh("M_ctx"), mCtx)
//        val mCtxRef = BagVarRef(VarDef(mCtxNamed.n, mCtx.tp))
//        val bagDict = dict.tupleDict.fields(n)
//
//        bagDict match {
//          case b: OutputBagDict => mCtxNamed :: linearize(b, mCtxRef)
//          case _: InputBagDict => Nil
//          case _ => sys.error("Unknown dictionary")
//        }
//      }
//  }
//}
