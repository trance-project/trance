package shredding.nrc

import shredding.utils.Utils.Symbol
import shredding.core._

/**
  * Linearization of nested output queries
  */
trait Linearization {
  this: LinearizedNRC with Shredding with Optimizer with Printer with Extensions =>

  type DictMapper = Map[BagDictExpr, BagVarRef]

  case class MaterializationInfo(seq: Sequence, dictMapper: DictMapper) {

    def this(e: Expr, d: DictMapper) = this(Sequence(List(e)), d)

    def append(m: MaterializationInfo): MaterializationInfo =
      MaterializationInfo(Sequence(seq.exprs ++ m.seq.exprs), dictMapper ++ m.dictMapper)

    def prepend(e: Expr): MaterializationInfo =
      MaterializationInfo(Sequence(e :: seq.exprs), dictMapper)
  }

  /**
    * Assumes that a pipeline query will be
    * some Sequence of named NRC expressions
    * followed by an unnamed query in the 
    * flavor of Let named queries .... in unnamed query
    * then the only materialization info we care about is the 
    * unnamed query which uses named queries defined in the Let
    */
  def materialize(e: List[ShredExpr]): MaterializationInfo = {
    val mats = e.map{ sexpr => sexpr match {
        case ShredExpr(lbl1, BagDict(lbl2, Named(VarDef(n, _), e1), tdict)) => 
          materialize(ShredExpr(lbl1, BagDict(lbl2, e1.asInstanceOf[BagExpr], tdict)), n)
        case _ => materialize(sexpr) 
      }
    }
    MaterializationInfo(Sequence(mats.map(m => m.seq.exprs).flatten), mats.last.dictMapper)
  }

  def materialize(e: ShredExpr, n: String = "M"): MaterializationInfo = e.dict match {
    case d: BagDictExpr =>
      Symbol.freshClear()

      // Construct initial context containing e.flat
      val bagFlat = Singleton(Tuple("lbl" -> e.flat.asInstanceOf[TupleAttributeExpr]))
      val initCtxNamed = Named(VarDef(Symbol.fresh(s"${n}_ctx"), bagFlat.tp), bagFlat)
      val initCtxRef = BagVarRef(initCtxNamed.v)

      // Recur
      val matInfo = materializeWithDomains(d, initCtxRef, n)

      matInfo.prepend(initCtxNamed)

    case _ => sys.error("Cannot linearize dict type " + e.dict)
  }

  private def materializeWithDomains(dict: BagDictExpr, ctx: BagVarRef, n: String = "M"): MaterializationInfo = {
    // 1. Iterate over ctx (bag of labels) and produce key-value pairs
    //    consisting of labels from ctx and flat bags from dict
    val ldef = VarDef(Symbol.fresh("l"), ctx.tp.tp)
    val lbl = LabelProject(TupleVarRef(ldef), "lbl")
    val kvpair = Tuple("_1" -> lbl, "_2" -> optimize(dict.lookup(lbl)).asInstanceOf[BagExpr])
    val mDict = ForeachUnion(ldef, ctx,
      BagExtractLabel(LabelProject(TupleVarRef(ldef), "lbl"), Singleton(kvpair)))
    val mDictNamed = Named(VarDef(Symbol.fresh(n+"__D_"), mDict.tp), mDict)
    val mDictRef = BagVarRef(mDictNamed.v)

    // 2. Associate dict with its materialized expression
    val dictMapper = Map(dict -> mDictRef)

    // 3. Create materialization strategy
    val matStrategy = new MaterializationInfo(mDictNamed, dictMapper)

    // 4. For each label type in dict.flatBagTp.tp,
    //    create the context (bag of labels) and recur
    val labelTps = dict.tp.flatTp.tp.attrTps.filter(_._2.isInstanceOf[LabelType]).toList

    val childMatStrategies: List[MaterializationInfo] =
      labelTps.map { case (n, _) =>
        val kvDef = VarDef(Symbol.fresh("kv"), mDict.tp.tp)
        val xDef = VarDef(Symbol.fresh("xF"), dict.tp.flatTp.tp)
        val mCtx =
          DeDup(ForeachUnion(kvDef, mDictRef,
            ForeachUnion(xDef, BagProject(TupleVarRef(kvDef), "_2"),
              Singleton(Tuple("lbl" -> LabelProject(TupleVarRef(xDef), n))))))
        val mCtxNamed = Named(VarDef(Symbol.fresh(s"${n}_ctx"), mCtx.tp), mCtx)
        val mCtxRef = BagVarRef(mCtxNamed.v)
        val bagDict = dict.tupleDict(n)

        bagDict match {
          case b: BagDictExpr =>
            val matInfo = materializeWithDomains(b, mCtxRef, n)
            matInfo.prepend(mCtxNamed)
          case b => sys.error("Unknown dictionary " + b)
        }
      }

    (matStrategy :: childMatStrategies).reduce(_ append _)
  }

  def unshred(e: ShredExpr, dictMapper: Map[BagDictExpr, BagVarRef]): Expr = {
    e.dict match {
      case d: BagDictExpr =>         
        val (exps, lkup) = unshred(e.flat.asInstanceOf[LabelExpr], d, dictMapper)
        Sequence(exps)
      case _ => sys.error("Cannot linearize dict type " + e.dict)
    }
  }

  /**
    Unshred: given n levels of nesting, will build a sequence: 
      at the lowest level:
         newdict_n = lookup on dict_n
      recursive step:
        newdict_n-1 := dict_n-1 join newdict_n
      base case:
        dict_0 = dict0 join newdict_n-1
   */
  private def unshred(lbl: LabelExpr, dict: BagDictExpr, dictMapper: Map[BagDictExpr, BagVarRef]): (List[BagExpr], BagExpr) = {
    val top = dictMapper(dict)
    val kvdef = VarDef(Symbol.fresh("kv"), top.tp.tp)
    val kvref = TupleVarRef(kvdef)
    
    val bag = BagProject(kvref, "_2")
    val tdef = VarDef(Symbol.fresh("t"), bag.tp.tp)
    val tref = TupleVarRef(tdef)
   
    if (!dict.tupleDict.asInstanceOf[TupleDict].fields.exists{ case (k,v) => v != EmptyDict }){
      (Nil, Lookup(lbl, BagDictVarRef(VarDef(top.varDef.name, dict.tp))))
    }else{
      var nseqs = List[BagExpr]()
      val bagExpr = ForeachUnion(kvdef, top,
        //Singleton(Tuple("_1" -> LabelProject(kvref, "_1"), "_2" -> 
          ForeachUnion(tdef, bag,  
            Singleton(Tuple("_1" -> LabelProject(kvref, "_1"), 
            "_2" -> Singleton(Tuple(tref.tp.attrTps.map{
              case (n, _:LabelType) =>
                val (bexp, lkup) = unshred(LabelProject(tref, n), dict.tupleDict(n).asInstanceOf[BagDictExpr], dictMapper)
                nseqs = bexp
                n -> lkup
              case (n, _) => n -> tref(n)
          }))))))
        val bagtype = bagExpr.tp match {
          case BagType(TupleType(fs)) => fs("_2")
          case _ => ???
        }
        val bvr = VarDef("new"+top.varDef.name, BagDictType(bagtype.asInstanceOf[BagType], 
          TupleDictType(Map("nil" -> EmptyDictType))))
        (nseqs :+ Named(VarDef("new"+top.varDef.name, bagExpr.tp), bagExpr), Lookup(lbl, BagDictVarRef(bvr)))
    }
  }

  /* Old linearization approach w/o unshredding */

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
    val kvpair = Tuple("_1" -> lbl, "_2" -> optimize(dict.lookup(lbl)).asInstanceOf[BagExpr])
    val mFlat = ForeachUnion(ldef, ctx,
      BagExtractLabel(LabelProject(TupleVarRef(ldef), "lbl"), Singleton(kvpair)))
    val mFlatNamed = Named(VarDef(Symbol.fresh("M_dict"), mFlat.tp), mFlat)
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
            ForeachUnion(xDef, BagProject(TupleVarRef(kvDef), "_2"),
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
    val mFlatNamed = Named(VarDef(Symbol.fresh("M_dict"), flatBagExpr.tp), flatBagExprRewritten.asInstanceOf[BagExpr])

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
