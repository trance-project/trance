package shredding.nrc2

import shredding.core._

trait Linearization {
  this: NRC with ShreddingTransform with NRCImplicits with NRCTransforms with Dictionary =>

  object Linearize extends Serializable{

   private var currId = 0

   def getId: Int = { currId += 1; currId }

    def apply(e: ShredExpr): List[Expr] = e.dict match {
      case d: OutputBagDict =>
        val emptyCtx = NamedBag("EmptyCtx"+getId, Singleton(Tuple("lbl" -> e.flat.asInstanceOf[TupleAttributeExpr])))
          //BagConst(Nil, BagType(TupleType("lbl" -> LabelType()))))
        val emptyCtxDef = VarDef("EmptyCtx", emptyCtx.tp, currId)
        val emptyCtxRef = BagVarRef(emptyCtxDef)
        List(emptyCtx) ++ linearize(d, emptyCtxRef)

      case _ => sys.error("Cannot linearize dict type " + e.dict)
    }

    def betaReduce(e: Expr): Expr = e.replace {
      case Lookup(l1, OutputBagDict(l2, flatBag, _)) if l1 == l2 =>
        betaReduce(flatBag)
    }

    def linearize(dict: OutputBagDict, ctx: BagVarRef): List[Expr] = {
      val ldef = VarDef("l", ctx.tp.tp, VarCnt.inc)//+ getId, ctx.tp.tp)
      val lref = TupleVarRef(ldef)

      val mFlat =
        ForeachUnion(
          ldef,
          ctx,
          Singleton(
            Tuple("k" -> Project(lref, "lbl"), "v" -> betaReduce(dict.flat).asInstanceOf[BagExpr]))
        )
      val mFlatNamed = NamedBag("M_flat"+ getId, mFlat)
      val mFlatDef = VarDef("M_flat", mFlat.tp, currId)
      val mFlatRef = BagVarRef(mFlatDef)

      val labelTps = dict.flatBagTp.tp.attrs.filter(_._2.isInstanceOf[LabelType]).toList

      mFlatNamed ::
        labelTps.flatMap { case (n, _) =>
          val pairDef = VarDef("kv", mFlat.tp.tp, VarCnt.inc)// + getId, mFlat.tp.tp)
          val pairRef = TupleVarRef(pairDef)
          val xDef = VarDef("xF", dict.flatBagTp.tp, VarCnt.inc) //+ getId, dict.flatBagTp.tp)
          val xRef = TupleVarRef(xDef)

          val mCtx =
            ForeachUnion(pairDef, mFlatRef,
              ForeachUnion(
                xDef,
                BagProject(pairRef, "v"),
                Singleton(Tuple("lbl" -> Project(xRef, n)))
              )
            )
          val mCtxNamed = NamedBag("M_ctx"+ getId, mCtx)
          val mCtxDef = VarDef("M_ctx", mCtx.tp, currId)
          val mCtxRef = BagVarRef(mCtxDef)
          
          mCtxNamed ::
            linearize(dict.tupleDict.fields(n).asInstanceOf[OutputBagDict], mCtxRef)
        }
    }
  }
}
