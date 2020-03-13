package shredding.nrc

import shredding.utils.Utils.Symbol
import shredding.core._

/**
  * Materialization of nested output queries
  */
trait Materializer {
  this: MaterializeNRC with Shredding with Optimizer with Printer with Extensions =>

  case class MaterializedProgram(program: Program, dictMapper: DictionaryMapper) {

    def this(a: Assignment, d: DictionaryMapper) = this(Program(List(a)), d)

    def append(m: MaterializedProgram): MaterializedProgram =
      MaterializedProgram(
        Program(program.statements ++ m.program.statements),
        dictMapper ++ m.dictMapper
      )

    def prepend(a: Assignment): MaterializedProgram =
      MaterializedProgram(Program(a :: program.statements), dictMapper)
  }

  class DictionaryMapper(val bagDictMapper: Map[BagDictExpr, BagExpr],
                         val tupleDictMapper: Map[TupleDictExpr, TupleExpr]) {
    def add(d: BagDictExpr, b: BagExpr): DictionaryMapper =
      new DictionaryMapper(bagDictMapper + (d -> b), tupleDictMapper)

    def add(d: TupleDictExpr, t: TupleExpr): DictionaryMapper =
      new DictionaryMapper(bagDictMapper, tupleDictMapper + (d -> t))

    def ++(that: DictionaryMapper): DictionaryMapper =
      new DictionaryMapper(
        bagDictMapper ++ that.bagDictMapper,
        tupleDictMapper ++ that.tupleDictMapper
      )
  }

  def replaceDictionaries(e: Expr, m: DictionaryMapper): Expr = replace(e, {
//    case v: BagDictExpr if m.bagDictMapper.contains(v) =>
//      m.bagDictMapper(v)
//    case v: TupleDictExpr if m.tupleDictMapper.contains(v) =>
//      m.tupleDictMapper(v)
    case Lookup(l, d) if m.bagDictMapper.contains(d) =>
      MatDictLookup(replaceDictionaries(l, m).asInstanceOf[LabelExpr], m.bagDictMapper(d))
    case l: Let if l.e1.isInstanceOf[DictExpr] =>
      val r1 = replaceDictionaries(l.e1, m)
      val xd = VarDef(l.x.name, r1.tp)
      val r2 = r1 match {
        case b1: BagExpr =>
          replaceDictionaries(l.e2, m.add(BagDictVarRef(l.x), b1))
        case t1: TupleExpr =>
          replaceDictionaries(l.e2, m.add(TupleDictVarRef(l.x), t1))
        case _ =>
          replaceDictionaries(l.e2, m)
      }
      Let(xd, r1, r2)
//    case BagDictProject(v, n) =>
//        .asInstanceOf[TupleDictExpr].apply(n)
//    case TupleDictProject(d) =>
//      replace(d, f).asInstanceOf[BagDictExpr].tupleDict
  })

  def materialize(p: ShredProgram): MaterializedProgram = {
    val dictMapper = new DictionaryMapper(Map.empty, Map.empty)
    p.statements.foldLeft (dictMapper, List.empty[MaterializedProgram]) {
      case ((dictMapper, programs), stmt) =>
        val rewritten =
          ShredAssignment(
            stmt.name,
            ShredExpr(
              replaceDictionaries(stmt.rhs.flat, dictMapper),
              replaceDictionaries(stmt.rhs.dict, dictMapper).asInstanceOf[DictExpr])
          )
        val program = materialize(rewritten)
        (dictMapper ++ program.dictMapper, programs :+ program)
    }._2.reduce(_ append _)
  }

  def materialize(a: ShredAssignment): MaterializedProgram = a.rhs.dict match {
    case d: BagDictExpr =>
      Symbol.freshClear()

      // Construct initial context containing a.rhs.flat
      val bagFlat = Singleton(Tuple(LABEL_ATTR_NAME -> a.rhs.flat.asInstanceOf[LabelExpr]))
      val initCtx = Assignment(Symbol.fresh(s"${a.name}_ctx"), bagFlat)
      val initCtxRef = BagVarRef(VarDef(initCtx.name, initCtx.rhs.tp))

      // Recur
      val matProgram = materializeDomains(a.name, d, initCtxRef)
      matProgram.prepend(initCtx)

    // TODO: other dict expressions
    case d => sys.error("Cannot materialize dict type " + d)
  }

  def materialize(e: ShredExpr): MaterializedProgram =
    materialize(ShredAssignment(Symbol.fresh("Q"), e))

  private def materializeDomains(prefix: String,
                                 dict: BagDictExpr,
                                 ctx: BagVarRef): MaterializedProgram = {

    val ldef = VarDef(Symbol.fresh("l"), ctx.tp.tp)
    val lbl = LabelProject(TupleVarRef(ldef), LABEL_ATTR_NAME)

    // Return expr creating pairs of labels from ctx and flat bags from dict
    def kvPairs(b: BagExpr) =
      ForeachUnion(ldef, ctx,
        BagExtractLabel(
          LabelProject(TupleVarRef(ldef), LABEL_ATTR_NAME),
          Singleton(Tuple(KEY_ATTR_NAME -> lbl, VALUE_ATTR_NAME -> b))))

    def materializeDictionary(kvPairs: BagExpr, dict: BagDictExpr): MaterializedProgram = {
      // 1. Create assignment statement
      val mDict = Assignment(Symbol.fresh(prefix + "__D_"), kvPairs)
      val mDictRef = BagVarRef(VarDef(mDict.name, mDict.rhs.tp))

      // 2. Associate dict with its materialized expression
      val bagDictMapper = Map(
        dict -> mDictRef,
        BagDictVarRef(VarDef(dictName(prefix), dict.tp)) -> mDictRef
      )

      val tupleTp = dict.tp.flatTp.tp
      val labelTypeExists = tupleTp.attrTps.exists(_._2.isInstanceOf[LabelType])

      // 3. For each label type in dict.flatBagTp.tp,
      //    create the context (bag of labels) and recur
      val (childMatStrategies, tupleMatDictFields) =
        if (!labelTypeExists)
          (List.empty[MaterializedProgram], List.empty[(String, BagExpr)])
        else {
          tupleTp.attrTps.collect {
            case (n, _: LabelType) =>
              val kvDef = VarDef(Symbol.fresh("kv"), kvPairs.tp.tp)
              val xDef = VarDef(Symbol.fresh("xF"), dict.tp.flatTp.tp)
              val rhs =
                DeDup(ForeachUnion(kvDef, mDictRef,
                  ForeachUnion(xDef, BagProject(TupleVarRef(kvDef), VALUE_ATTR_NAME),
                    Singleton(Tuple(LABEL_ATTR_NAME -> LabelProject(TupleVarRef(xDef), n))))))
              val mCtx = Assignment(Symbol.fresh(s"${prefix}_ctx"), rhs)
              val mCtxRef = BagVarRef(VarDef(mCtx.name, mCtx.rhs.tp))

              dict.tupleDict(n) match {
                case d: BagDictExpr =>
                  val matProgram = materializeDomains(prefix, d, mCtxRef)
                  val tupleMatDictField =
                    n -> matProgram.dictMapper.bagDictMapper(d)
                  (matProgram.prepend(mCtx), tupleMatDictField)
                case d => sys.error("Unknown dictionary " + d)
              }
          }
        }.unzip

      val tupleDictMapper: Map[TupleDictExpr, TupleExpr] =
        bagDictMapper.map(x =>
          TupleDictProject(x._1) -> Tuple(tupleMatDictFields.toMap)
        )

      // 4. Create materialization strategy
      val dictMapper = new DictionaryMapper(bagDictMapper, tupleDictMapper)
      val matStrategy = new MaterializedProgram(mDict, dictMapper)

      (matStrategy :: childMatStrategies.toList).reduce(_ append _)
    }

    dict match {
      case BagDict(tp, ShredUnion(b1, b2), TupleDictUnion(d1, d2)) if lbl.tp == tp =>
        val dict1 = BagDict(null, b1, d1)
        val dict2 = BagDict(null, b2, d2)
        materializeDictionary(kvPairs(b1), dict1) append
          materializeDictionary(kvPairs(b2), dict2)
      case _ =>
        val b = optimize(dict.lookup(lbl)).asInstanceOf[BagExpr]
        materializeDictionary(kvPairs(b), dict)
    }
  }

  def unshred(p: ShredProgram, dictMapper: DictionaryMapper): Program =
    Program(p.statements.flatMap(unshred(_, dictMapper).statements))

  def unshred(a: ShredAssignment, dictMapper: DictionaryMapper): Program = a.rhs.dict match {
    case d: BagDictExpr =>
      val mappedFlat = replaceDictionaries(a.rhs.flat, dictMapper).asInstanceOf[LabelExpr]
      val mappedDict = replaceDictionaries(d, dictMapper).asInstanceOf[BagDictExpr]
      val (prog, bag) = unshred(mappedFlat, mappedDict, dictMapper)
      Program(prog.statements :+ Assignment(a.name, bag))
    // TODO: other dict expressions
    case d => sys.error("Cannot unshred dict type " + d)
  }

  def unshred(e: ShredExpr, dictMapper: DictionaryMapper): Program =
    unshred(ShredAssignment(Symbol.fresh("Q"), e), dictMapper)

  private def unshred(lbl: LabelExpr, dict: BagDictExpr, dictMapper: DictionaryMapper): (Program, BagExpr) = {

    def unshredDictionary(d: BagDictExpr): (Program, BagExpr) = {
      val matDictVarRef = dictMapper.bagDictMapper(d).asInstanceOf[BagVarRef]
      val kvdef = VarDef(Symbol.fresh("kv"), matDictVarRef.tp.tp)
      val kvref = TupleVarRef(kvdef)

      val valueTp = kvref.tp.attrTps(VALUE_ATTR_NAME).asInstanceOf[BagType]
      val labelTypeExists = valueTp.tp.attrTps.exists(_._2.isInstanceOf[LabelType])

      if (!labelTypeExists)
        (Program(Nil), BagDictVarRef(VarDef(matDictVarRef.name, d.tp)).lookup(lbl))
      else {
        val bag = BagProject(kvref, VALUE_ATTR_NAME)
        val tdef = VarDef(Symbol.fresh("t"), bag.tp.tp)
        val tref = TupleVarRef(tdef)

        val (childPrograms, childTupleAttrs) = tref.tp.attrTps.map {
          case (n, _: LabelType) =>
            val (p, e) = unshred(LabelProject(tref, n), dict.tupleDict(n).asInstanceOf[BagDictExpr], dictMapper)
            (p, n -> e)
          case (n, _) => (Program(Nil), n -> tref(n))
        }.unzip

        val key = LabelProject(kvref, KEY_ATTR_NAME)
        val value = Singleton(Tuple(childTupleAttrs.toMap))

        val kvPairsNested =
          ForeachUnion(kvdef, matDictVarRef,
            Singleton(Tuple(
              KEY_ATTR_NAME -> key,
              VALUE_ATTR_NAME -> ForeachUnion(tdef, bag, value))))

        val newDictAssignment = Assignment("new" + matDictVarRef.name, kvPairsNested)
        val newProgram = childPrograms.reduce(_ ++ _) append newDictAssignment
        val newDictVarRef = BagDictVarRef(VarDef(newDictAssignment.name, BagDictType(lbl.tp, value.tp, null)))
        (newProgram, newDictVarRef.lookup(lbl))
      }
    }

    dict match {
      case BagDict(_, ShredUnion(b1, b2), TupleDictUnion(d1, d2)) =>
        val (prog1, bag1) = unshredDictionary(BagDict(null, b1, d1))
        val (prog2, bag2) = unshredDictionary(BagDict(null, b2, d2))
        (Program(prog1.statements ++ prog2.statements), Union(bag1, bag2))
      case _ => unshredDictionary(dict)
    }
  }

  def materializeNoDomains(e: ShredExpr): MaterializedProgram =
    throw new NotImplementedError()


  //  case class MaterializationInfo(seq: Sequence, dictMapper: DictMapper) {
//
//    def this(e: Expr, d: DictMapper) = this(Sequence(List(e)), d)
//
//    def append(m: MaterializationInfo): MaterializationInfo =
//      MaterializationInfo(Sequence(seq.ee ++ m.seq.ee), dictMapper ++ m.dictMapper)
//
//    def prepend(e: Expr): MaterializationInfo =
//      MaterializationInfo(Sequence(e :: seq.ee), dictMapper)
//  }
//
//  /**
//    * Assumes that a pipeline query will be
//    * some Sequence of named NRC expressions
//    * followed by an unnamed query in the
//    * flavor of Let named queries .... in unnamed query
//    * then the only materialization info we care about is the
//    * unnamed query which uses named queries defined in the Let
//    */
//  def materialize(e: List[ShredExpr]): MaterializationInfo = {
//    val mats = e.map {
//      case ShredExpr(lbl1, BagDict(lbl2, BagNamed(VarDef(n, _), e1), tdict)) =>
//        materialize(ShredExpr(lbl1, BagDict(lbl2, e1.asInstanceOf[BagExpr], tdict)), n)
//      case e1 => materialize(e1)
//    }
//    MaterializationInfo(Sequence(mats.flatMap(m => m.seq.ee)), mats.last.dictMapper)
//  }
//
//  def materialize(e: ShredExpr, n: String = "M"): MaterializationInfo = e.dict match {
//    case d: BagDictExpr =>
//      Symbol.freshClear()
//
//      // Construct initial context containing e.flat
//      val bagFlat = Singleton(Tuple("lbl" -> NewLabel(Set.empty)))//e.flat.asInstanceOf[TupleAttributeExpr]))
//      val initCtxNamed = BagNamed(VarDef(Symbol.fresh(s"${n}_ctx"), bagFlat.tp), bagFlat)
//      val initCtxRef = BagVarRef(initCtxNamed.v)
//
//      // Recur
//      val matInfo = materializeDomains(d, initCtxRef, n)
//
//      matInfo.prepend(initCtxNamed)
//
//    case _ => sys.error("Cannot linearize dict type " + e.dict)
//  }
//
//  private def materializeDomains(dict: BagDictExpr, ctx: BagVarRef, name: String = "M"): MaterializationInfo = {
//    val ldef = VarDef(Symbol.fresh("l"), ctx.tp.tp)
//    val lbl = LabelProject(TupleVarRef(ldef), "lbl")
//
//    // Return expr creating pairs of labels from ctx and flat bags from dict
//    def kvPairs(b: BagExpr) =
//      ForeachUnion(ldef, ctx,
//        BagExtractLabel(
//          LabelProject(TupleVarRef(ldef), "lbl"),
//          Singleton(Tuple("_1" -> lbl, "_2" -> b))))
//
//    def materializeDictionary(kvPairs: BagExpr, dict: BagDictExpr): MaterializationInfo = {
//      // 1. Create named materialized expression
//      val mDictNamed = BagNamed(VarDef(Symbol.fresh(name+"__D_"), kvPairs.tp), kvPairs)
//      val mDictRef = BagVarRef(mDictNamed.v)
//
//      // 2. Associate dict with its materialized expression
//      val dictMapper = Map(dict -> mDictRef)
//
//      // 3. Create materialization strategy
//      val matStrategy = new MaterializationInfo(mDictNamed, dictMapper)
//
//      // 4. For each label type in dict.flatBagTp.tp,
//      //    create the context (bag of labels) and recur
//      val labelTps = dict.tp.flatTp.tp.attrTps.filter(_._2.isInstanceOf[LabelType]).toList
//
//      val childMatStrategies: List[MaterializationInfo] =
//        labelTps.map { case (n, tp) =>
//
////          val lblTp = tp.asInstanceOf[LabelType]
////          val parentLblTp = ctx.tp.tp("lbl").asInstanceOf[LabelType]
////
////          if (lblTp.attrTps.toSet.subsetOf(parentLblTp.attrTps.toSet))
////            dict.tupleDict(n) match {
////              case b: BagDictExpr => materializeDomains(b, ctx)
////              case b => sys.error("Unknown dictionary " + b)
////            }
////          else {
//            val kvDef = VarDef(Symbol.fresh("kv"), kvPairs.tp.tp)
//            val xDef = VarDef(Symbol.fresh("xF"), dict.tp.flatTp.tp)
//            val mCtx =
//              DeDup(ForeachUnion(kvDef, mDictRef,
//                ForeachUnion(xDef, BagProject(TupleVarRef(kvDef), "_2"),
//                  Singleton(Tuple("lbl" -> LabelProject(TupleVarRef(xDef), n))))))
//            val mCtxNamed = BagNamed(VarDef(Symbol.fresh(s"${name}_ctx"), mCtx.tp), mCtx)
//            val mCtxRef = BagVarRef(mCtxNamed.v)
//
//            dict.tupleDict(n) match {
//              case b: BagDictExpr =>
//                val matInfo = materializeDomains(b, mCtxRef)
//                matInfo.prepend(mCtxNamed)
//              case b => sys.error("Unknown dictionary " + b)
//            }
////          }
//        }
//
//      (matStrategy :: childMatStrategies).reduce(_ append _)
//    }
//
//    dict match {
//      case BagDict(l, ShredUnion(b1, b2), TupleDictUnion(d1, d2)) if lbl.tp == l.tp =>
//        val dict1 = BagDict(null, b1, d1)
//        val dict2 = BagDict(null, b2, d2)
//        materializeDictionary(kvPairs(b1), dict1) append
//          materializeDictionary(kvPairs(b2), dict2)
//      case _ =>
//        val b = optimize(dict.lookup(lbl)).asInstanceOf[BagExpr]
//        materializeDictionary(kvPairs(b), dict)
//    }
//  }
//
//  def unshred(e: ShredExpr, dictMapper: Map[BagDictExpr, BagVarRef]): Expr = e.dict match {
//    case d: BagDictExpr =>
//      val (exps, lkup) = unshred(e.flat.asInstanceOf[LabelExpr], d, dictMapper)
//      if (exps.nonEmpty) Sequence(exps)
//      else lkup match { // this is the case where no unshredding will happen
//        case Lookup(lbl, bd) => bd
//        case _ => ???
//      }
//    case _ => sys.error("Cannot linearize dict type " + e.dict)
//  }
//
//  /**
//    Unshred: given n levels of nesting, will build a sequence:
//      at the lowest level:
//         newdict_n = lookup on dict_n
//      recursive step:
//        newdict_n-1 := dict_n-1 join newdict_n
//      base case:
//        dict_0 = dict0 join newdict_n-1
//   */
//  private def unshred(lbl: LabelExpr, dict: BagDictExpr, dictMapper: Map[BagDictExpr, BagVarRef]): (List[BagExpr], BagExpr) = {
//
//    def unshredDictionary(d: BagDictExpr): (List[BagExpr], BagExpr) = {
//      val matDict = dictMapper(d)
//      val kvdef = VarDef(Symbol.fresh("kv"), matDict.tp.tp)
//      val kvref = TupleVarRef(kvdef)
//
//      val valueTp = kvref.tp.attrTps("_2").asInstanceOf[BagType]
//      val labelTypeExist = valueTp.tp.attrTps.exists(_._2.isInstanceOf[LabelType])
//
//      if (!labelTypeExist) (Nil, Lookup(lbl, BagDictVarRef(VarDef(matDict.varDef.name, dict.tp))))
//      else {
//        val bag = BagProject(kvref, "_2")
//        val tdef = VarDef(Symbol.fresh("t"), bag.tp.tp)
//        val tref = TupleVarRef(tdef)
//
//        var nseqs = List[BagExpr]()
//        val bagExpr = ForeachUnion(kvdef, matDict,
//        //Singleton(Tuple("_1" -> LabelProject(kvref, "_1"), "_2" ->
//          ForeachUnion(tdef, bag,
//            Singleton(Tuple("_1" -> LabelProject(kvref, "_1"),
//            "_2" -> Singleton(Tuple(tref.tp.attrTps.map{
//              case (n, _:LabelType) =>
//                val (bexp, lkup) = unshred(LabelProject(tref, n), dict.tupleDict(n).asInstanceOf[BagDictExpr], dictMapper)
//                nseqs = bexp
//                n -> lkup
//              case (n, _) => n -> tref(n)
//          }))))))
//        val bagTp = bagExpr.tp.tp.attrTps("_2").asInstanceOf[BagType]
//        val bvr = VarDef("new"+matDict.varDef.name, BagDictType(bagTp, TupleDictType(Map("nil" -> EmptyDictType))))
//        (nseqs :+ BagNamed(VarDef("new"+matDict.varDef.name, bagExpr.tp), bagExpr), Lookup(lbl, BagDictVarRef(bvr)))
//      }
//    }
//
//    dict match {
//      case BagDict(_, ShredUnion(b1, b2), TupleDictUnion(d1, d2)) =>
//        val dict1 = BagDict(null, b1, d1)
//        val dict2 = BagDict(null, b2, d2)
//        val (seqs1, lkup1) = unshredDictionary(dict1)
//        val (seqs2, lkup2) = unshredDictionary(dict2)
//        (seqs1 ++ seqs2, Union(lkup1, lkup2))
//      case _ =>
//        unshredDictionary(dict)
//    }
//  }

//  /* Old linearization approach w/o unshredding */
//
//  def linearize(e: ShredExpr): Sequence = e.dict match {
//    case d: BagDictExpr =>
//      Symbol.freshClear()
//
//      // Construct variable reference to the initial context
//      // that represents a bag containing e.flat.
//      //val bagFlat = Singleton(Tuple("lbl" -> e.flat.asInstanceOf[TupleAttributeExpr]))
//      val bagFlat = Singleton(Tuple("lbl" ->
//                      NewLabel(labelParameters(e.flat)).asInstanceOf[TupleAttributeExpr]))
//
//      val initCtxNamed = BagNamed(VarDef(Symbol.fresh("M_ctx"), bagFlat.tp), bagFlat)
//      val initCtxRef = BagVarRef(initCtxNamed.v)
//
//      // Let's roll
//      Sequence(initCtxNamed :: linearize(d, initCtxRef))
//
//    case _ => sys.error("Cannot linearize dict type " + e.dict)
//  }
//
//  private def linearize(dict: BagDictExpr, ctx: BagVarRef): List[Expr] = {
//    // 1. Iterate over ctx (bag of labels) and produce key-value pairs
//    //    consisting of labels from ctx and flat bags from dict
//    val ldef = VarDef(Symbol.fresh("l"), ctx.tp.tp)
//    val lbl = LabelProject(TupleVarRef(ldef), "lbl")
//    val kvpair = Tuple("_1" -> lbl, "_2" -> optimize(dict.lookup(lbl)).asInstanceOf[BagExpr])
//    val mFlat = ForeachUnion(ldef, ctx,
//      BagExtractLabel(LabelProject(TupleVarRef(ldef), "lbl"), Singleton(kvpair)))
//    val mFlatNamed = BagNamed(VarDef(Symbol.fresh("M_dict"), mFlat.tp), mFlat)
//    val mFlatRef = BagVarRef(mFlatNamed.v)
//
//    // 2. For each label type in dict.flatBagTp.tp,
//    //    create the context (bag of labels) and recur
//    val labelTps = dict.tp.flatTp.tp.attrTps.filter(_._2.isInstanceOf[LabelType]).toList
//
//    mFlatNamed ::
//      labelTps.flatMap { case (n, _) =>
//        val kvDef = VarDef(Symbol.fresh("kv"), mFlat.tp.tp)
//        val xDef = VarDef(Symbol.fresh("xF"), dict.tp.flatTp.tp)
//        val mCtx =
//          DeDup(ForeachUnion(kvDef, mFlatRef,
//            ForeachUnion(xDef, BagProject(TupleVarRef(kvDef), "_2"),
//              Singleton(Tuple("lbl" -> LabelProject(TupleVarRef(xDef), n))))))
//        val mCtxNamed = BagNamed(VarDef(Symbol.fresh("M_ctx"), mCtx.tp), mCtx)
//        val mCtxRef = BagVarRef(mCtxNamed.v)
//
//        dict.tupleDict(n) match {
//          case b: BagDictExpr => mCtxNamed :: linearize(b, mCtxRef)
//          case b => sys.error("Unknown dictionary " + b)
//        }
//      }
//  }

  /* Experimental */

//  def materializerX(e: ShredExpr): (Sequence, Expr) = e match {
//
//    case ShredExpr(l: LabelExpr, d: BagDictExpr) =>
//
//    case ShredExpr(t: TupleExpr, d: TupleDictExpr) =>
//
//
//    case ShredExpr(p: PrimitiveExpr, EmptyDict) => (Sequence(Nil), p)
//
//    case _ => sys.error("Cannot materialize shredded expr " + quote(e))
//  }


//  def linearizeNoDomains(e: ShredExpr): Sequence = e.dict match {
//    case d: BagDict =>
//      Symbol.freshClear()
//      Sequence(linearizeNoDomains(d))
//    case _ => sys.error("Cannot linearize bag dict type " + e.dict)
//  }
//
//  def linearizeNoDomains(dict: BagDict): List[Expr] = {
//    val flatBagExpr = dict.flat
//    val flatBagExprRewritten = nestingRewriteLossy(flatBagExpr)
//    val mFlatNamed = Named(VarDef(Symbol.fresh("M_dict"), flatBagExprRewritten.tp),
//      flatBagExprRewritten.asInstanceOf[BagExpr])
//
//    val labelTps = dict.tp.flatTp.tp.attrTps.filter(_._2.isInstanceOf[LabelType]).toList
//    mFlatNamed ::
//      labelTps.flatMap { case (n, _) =>
//        dict.tupleDict(n) match {
//          case b: BagDict => linearizeNoDomains(b)
//          case b: BagDictLet => linearizeNoDomains(b.e2.asInstanceOf[BagDict])
//          case b => sys.error("Unknown dictionary " + b)
//        }
//      }
//  }

}
