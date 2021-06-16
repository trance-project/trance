package framework.nrc

import framework.utils.Utils.Symbol
import framework.common._

/**
  * Materialization of nested output queries
  */
trait BaseMaterialization {

  val INPUT_BAG_PREFIX: String = "IBag_"

  val INPUT_MAP_PREFIX: String = "IMap_"

  val INPUT_TUPLE_PREFIX: String = "ITuple_"

  val MAT_BAG_PREFIX: String = "MBag_"

  val MAT_MAP_PREFIX: String = "MMap_"

  val MAT_TUPLE_PREFIX: String = "MTuple_"

  val LABEL_DOMAIN_PREFIX: String = "Dom_"

  val UNSHRED_PREFIX: String = "UDict_"

  def inputBagName(name: String): String = INPUT_BAG_PREFIX + name

  def inputMapName(name: String): String = INPUT_MAP_PREFIX + name

  def inputTupleName(name: String): String = INPUT_TUPLE_PREFIX + name

  def matBagName(name: String): String = MAT_BAG_PREFIX + name

  def matMapName(name: String): String = MAT_MAP_PREFIX + name

  def matTupleName(name: String): String = MAT_TUPLE_PREFIX + name

  def domainName(name: String): String = LABEL_DOMAIN_PREFIX + name

  def unshredDictName(name: String): String = UNSHRED_PREFIX + name

}

trait MaterializationContext extends BaseMaterialization with MaterializationDomain with Printer {
  this: MaterializeNRC =>

  class DictionaryContext(private val mapper: Map[DictExpr, DictNode],
                          private val mexprs: Set[(DictNode, Option[(DictNode, String)])]) {

    def this() = this(Map.empty, Set.empty)

    def ++(c: DictionaryContext): DictionaryContext =
      new DictionaryContext(mapper ++ c.mapper, mexprs ++ c.mexprs)

    def add(d: DictExpr, n: DictNode, parent: Option[(DictNode, String)]): DictionaryContext =
      new DictionaryContext(mapper + (d -> n), mexprs + (n -> parent))

    def add(n: DictNode, parent: Option[(DictNode, String)]): DictionaryContext =
      new DictionaryContext(mapper, mexprs + (n -> parent))

    def contains(d: DictExpr): Boolean = mapper.contains(d)

    def apply(d: DictExpr): DictNode = d match {
      case _: DictVarRef => mapper(d)
      case _: BagDict if mapper.contains(d) => mapper(d)
      case a: BagDict => SDict(a)
      case a: TupleDict =>
        STuple(a.fields.collect { case (n, e: BagDictExpr) => n -> apply(e) })
      case a: BagDictProject =>
        Project(apply(a.tuple), a.field)
      case a: TupleDictProject =>
        STuple(childrenOf(apply(a.dict)))
      case a: DictLet if a.e1.isInstanceOf[DictExpr] =>
        val d1 = a.e1.asInstanceOf[DictExpr]
        val n1 = apply(d1)
        val ctx = add(DictVarRef(a.x.name, d1.tp), n1, None)  // don't care about parent as recur down
        SLet(a.x.name, a.e1, ctx(a.e2))
      case a: DictLet =>
        SLet(a.x.name, a.e1, apply(a.e2))
      case a: DictIfThenElse =>
        SIfThenElse(a.cond, apply(a.e1), apply(a.e2.get))
      case _ =>
        sys.error("[DictionaryContext.apply] Unexpected dictionary " + d)
    }

    def childrenOf(d: DictExpr): Map[String, DictNode] = childrenOf(mapper(d))

    def childrenOf(m: DictNode): Map[String, DictNode] =
      mexprs.collect { case (child, Some((p, f))) if p == m => f -> child }.toMap
  }

  type Context = DictionaryContext

  protected def initContext(p: ShredProgram, ctx: Context): Context =
    inputVars(p).foldLeft (ctx) {
      case (acc, d: BagDictVarRef) if !ctx.contains(d) => addInputDict(d, acc, None)
      case (acc, d: TupleDictVarRef) if !ctx.contains(d) => addInputDict(d, acc, None)
      case (acc, _) => acc
    }

  private def addInputDict(d: BagDictVarRef, ctx: Context, parent: Option[(DictNode, String)]): Context = {
    val matRef = if (parent.isEmpty)
      BagVarRef(inputBagName(d.name), d.tp.flatTp)
    else
      KeyValueMapVarRef(inputMapName(d.name), KeyValueMapType(d.tp.lblTp, d.tp.flatTp))
    val matDict = MaterializedDict(matRef.name, matRef)
    val newCtx = ctx.add(d, matDict, parent)

    d.tp.dictTp.attrTps.foldLeft (newCtx) {
      case (acc, (n, t: BagDictType)) =>
        val child = BagDictVarRef(d.name + "_" + n, t)
        addInputDict(child, acc, Some(matDict -> n))
      case (acc, (_, EmptyDictType)) => acc
    }
  }

  private def addInputDict(d: TupleDictVarRef, ctx: Context, parent: Option[(DictNode, String)]): Context = {
    // TODO:
    sys.error("Not implemented")
  }

  protected def resolveLookup(l: LabelExpr, d: DictNode): BagExpr = d match {
    case a: MBag => a.varRef
    case a: MKeyValueMap => KeyValueMapLookup(l, a.varRef)
    case a: SDict => l match {
        case l: NewLabel => applyLambda(l, a.d.flat)
        case _ => ExtractLabel(l, a.d.flat).asInstanceOf[BagExpr]
      }
    case a: SLet => BagLet(VarDef(a.name, a.e1.tp), a.e1, resolveLookup(l, a.n2))
    case a: SIfThenElse => BagIfThenElse(a.cond, resolveLookup(l, a.n1), Some(resolveLookup(l, a.n2)))
  }

  protected def resolveDict(d: DictNode): Expr = d match {
    case a: MaterializedDict => a.varRef
    case a: SDict => sys.error("Cannot resolve symbolic dictionary " + a)
    case a: SLet => Let(VarDef(a.name, a.e1.tp), a.e1, resolveDict(a.n2))
    case a: SIfThenElse => IfThenElse(a.cond, resolveDict(a.n1), resolveDict(a.n2))
  }

  protected def rewriteUsingContext(e0: Expr, ctx: Context): Expr = replace(e0, {

    case l: Let if l.e1.isInstanceOf[DictExpr] =>
      val d1 = l.e1.asInstanceOf[DictExpr]
      val n1 = ctx(d1)
      val ctx2 = ctx.add(DictVarRef(l.x.name, d1.tp), n1, None)   // don't care about parent as recur down
      rewriteUsingContext(l.e2, ctx2)

    case Lookup(l, d) =>
      val l2 = rewriteUsingContext(l, ctx).asInstanceOf[LabelExpr]
      val b2 = resolveLookup(l2, ctx(d))
      rewriteUsingContext(b2, ctx)

    case d: DictExpr =>
      sys.error("Dictionary found in rewriteUsingContext " + d)
  })
}

trait Materialization extends MaterializationContext {
  this: MaterializeNRC with Optimizer with Printer =>

  class MProgram(val program: Program, val ctx: Context)

  def materialize(p: ShredProgram,
                  ctx: Context = new Context,
                  eliminateDomains: Boolean = false): MProgram = {
    Symbol.freshClear()

    // Create initial context with input dictionaries
    val initCtx = initContext(p, ctx)

    // Materialize each statement starting from empty program
    val emptyProgram = new MProgram(Program(), initCtx)
    p.statements.foldLeft (emptyProgram) { case (acc, s) =>
      val mp = materialize(s, acc.ctx, eliminateDomains)
      new MProgram(acc.program ++ mp.program, mp.ctx)
    }
  }

  private def materialize(a: ShredAssignment, ctx: Context, eliminateDomains: Boolean): MProgram =
    a.rhs match {
      case ShredExpr(_: PrimitiveExpr, EmptyDict) =>
        new MProgram(Program(), ctx)

      case ShredExpr(l: LabelExpr, d: BagDictExpr) =>
        assert(l.tp == d.tp.lblTp)   // sanity check
        val (mexprs, ctx2) =
          materializeDictExpr(d, dictName(a.name), ctx, None, eliminateDomains)
        val p = Program(mexprs.map(m => Assignment(m.name, m.e)))
        new MProgram(p, ctx2)

      case ShredExpr(_: TupleExpr, d: TupleDictExpr) =>
        val (mexprs, ctx2) =
          materializeDictExpr(d, dictName(a.name), ctx, None, eliminateDomains)
        val p = Program(mexprs.map(m => Assignment(m.name, m.e)))
        new MProgram(p, ctx2)

      case _ =>
        sys.error("Materialization not supported for " + quote(a))
    }

  private def materializeDictExpr( dict: DictExpr,
                                   name: String,
                                   ctx: Context,
                                   parent: Option[(MaterializedDict, String)],
                                   eliminateDomains: Boolean): (List[MaterializedDict], Context) =
    dict match {
      case EmptyDict | EmptyDictUnion => (Nil, ctx)

      case _: DictVarRef => (Nil, ctx)

      case BagDict(tp, ShredUnion(b1, b2), TupleDictUnion(d1, d2)) =>
        val (ee1, ctx1) =
          materializeDictExpr(BagDict(tp, b1, d1), name, ctx, parent, eliminateDomains)
        val (ee2, ctx2) =
          materializeDictExpr(BagDict(tp, b2, d2), name, ctx, parent, eliminateDomains)
        (ee1 ++ ee2, ctx1 ++ ctx2)

      case d: BagDict =>
        materializeBagDict(d, name, ctx, parent, eliminateDomains)

      case d: TupleDict =>
        materializeTupleDict(d, name, ctx, parent, eliminateDomains)

      case d: BagDictProject =>       // (Nil, ctx)
        materializeDictExpr(d.tuple, name, ctx, parent, eliminateDomains)

      case d: TupleDictProject =>     // (Nil, ctx)
        materializeDictExpr(d.dict, name, ctx, parent, eliminateDomains)

      case l: DictLet if l.e1.isInstanceOf[DictExpr] =>
        val d1 = l.e1.asInstanceOf[DictExpr]
        val (ee1, ctx1) =
          materializeDictExpr(d1, l.x.name, ctx, parent, eliminateDomains)
        val ctx2 =
          ctx1.add(DictVarRef(l.x.name, d1.tp), ctx1(d1), None)    // don't care about parent as recur down
        val (ee2, ctx3) =
          materializeDictExpr(l.e2, name, ctx2, parent, eliminateDomains)
        (ee1 ++ ee2, ctx3)

      case l: DictLet =>
        val r1 = rewriteUsingContext(l.e1, ctx)
        val (ee2, ctx2) =
          materializeDictExpr(l.e2, name, ctx, parent, eliminateDomains)

        // Prefix each expression in ee2 with a let expression with r1
        val ee2let = ee2.map(m => MaterializedDict(m.name, Let(l.x.name, r1, m.e)))
        (ee2let, ctx2)

      case i: DictIfThenElse =>
        val (ee1, ctx1) =
          materializeDictExpr(i.e1, name, ctx, parent, eliminateDomains)
        val (ee2, ctx2) =
          materializeDictExpr(i.e2.get, name, ctx, parent, eliminateDomains)
        (ee1 ++ ee2, ctx1 ++ ctx2)

      case d: DictUnion =>
        val (ee1, ctx1) =
          materializeDictExpr(d.dict1, name, ctx, parent, eliminateDomains)
        val (ee2, ctx2) =
          materializeDictExpr(d.dict2, name, ctx, parent, eliminateDomains)
        (ee1 ++ ee2, ctx1 ++ ctx2)

      case _ =>
        sys.error("Dictionary materialization not supported for " + quote(dict))
    }

  private def materializeBagDict( dict: BagDict,
                                  name: String,
                                  ctx: Context,
                                  parent: Option[(MaterializedDict, String)],
                                  eliminateDomains: Boolean): (List[MaterializedDict], Context) =
    dict match {
      case BagDict(_, flat, tupleDict: TupleDictExpr) if parent.isEmpty =>
          // 1. Eliminate symbolic dictionaries from flat expression
          val flatBag = rewriteUsingContext(flat, ctx).asInstanceOf[BagExpr]

          // 2. Create named expression
          val suffix = Symbol.fresh(name + "_")
          val mexpr = MaterializedDict(matBagName(suffix), flatBag)

          // 3. Extend context
          val ctx2 = ctx
            .add(dict, mexpr, parent)
            .add(BagDictVarRef(name, dict.tp), mexpr, parent)

          // 4. Materialize children
          val (children, ctx3) =
            materializeDictExpr(tupleDict, suffix, ctx2, Some(mexpr -> "tupleDict"), eliminateDomains)

          (mexpr :: children, ctx3)

      case BagDict(_, flat, tupleDict: TupleDictExpr) =>
        val isEliminated = if (eliminateDomains) eliminateDomain(flat, ctx) else None

        isEliminated match {
          case None =>
            val (parentDict, field) = parent.get

            // 1. Create label domain
            val domain = createLabelDomain(parentDict, field)
            val domainRef = domain.varRef

            // 2. Create named expression
            val tpl = TupleVarRef(Symbol.fresh(name = "l"), domainRef.tp.tp)
            val lbl = LabelProject(tpl, LABEL_ATTR_NAME)
            val dictBag =
              ForeachUnion(tpl, domainRef,
                addOutputField(KEY_ATTR_NAME -> lbl,
                  ExtractLabel(lbl, flat).asInstanceOf[BagExpr]))
            val dictBag2 =
              rewriteUsingContext(dictBag, ctx).asInstanceOf[BagExpr]

            // 3. Create assignment statement
            val suffix = Symbol.fresh(name + "_")
            val mexpr = MaterializedDict(matMapName(suffix), BagToKeyValueMap(dictBag2))

            // 4. Extend context
            val ctx2 = ctx.add(dict, mexpr, parent)

            // 5. Materialize children if needed
            val (children, ctx3) =
              materializeDictExpr(tupleDict, suffix, ctx2, Some(mexpr -> "tupleDict"), eliminateDomains)

            (domain :: mexpr :: children, ctx3)

          case Some(flatBag) =>
            // 1. Create named expression
            val suffix = Symbol.fresh(name + "_")
            val mexpr = MaterializedDict(matMapName(suffix), BagToKeyValueMap(flatBag))

            // 2. Extend context
            val ctx2 = ctx.add(dict, mexpr, parent)

            // 3. Materialize children if needed
            val (children, ctx3) =
              materializeDictExpr(tupleDict, suffix, ctx2, Some(mexpr -> "tupleDict"), eliminateDomains)

            (mexpr :: children, ctx3)
        }

      case _ =>
        sys.error("[materializeBagDict] Unexpected dictionary type: " + dict)
    }

  private def materializeTupleDict(dict: TupleDict,
                                   name: String,
                                   ctx: Context,
                                   parent: Option[(MaterializedDict, String)],
                                   eliminateDomains: Boolean): (List[MaterializedDict], Context) = {
    val (ff2, ee2, ctx2) =
      dict.fields.foldLeft (Map.empty[String, DictNode], List.empty[MaterializedDict], ctx) {
        case (acc, (_, EmptyDict)) => acc

        case ((ff1, ee1, ctx1), (f: String, d: BagDictExpr)) =>
          // Materialize child dictionary
          val p = parent.map(_._1 -> f)
          val (ee2, ctx2) =
            materializeDictExpr(d, name + "_" + f, ctx1, p, eliminateDomains)
          val n = ctx2(d)
          (ff1 + (f -> n), ee1 ++ ee2, ctx2.add(n, p))
        case (_, (_, d)) =>
          sys.error("[materializeTupleDict] Unsupported dictionary type: " + d)
      }
    if (parent.isEmpty)
      (ee2, ctx2.add(TupleDictVarRef(name, dict.tp), STuple(ff2), parent))
    else
      (ee2, ctx2)
  }

  def unshred(p: ShredProgram, ctx: Context): Program = {
    Symbol.freshClear()
    Program(p.statements.flatMap(unshred(_, ctx).statements))
  }

  private def unshred(a: ShredAssignment, ctx: Context): Program = a.rhs match {
//    case ShredExpr(e: PrimitiveExpr, EmptyDict) =>

    case ShredExpr(l: LabelExpr, d: BagDictExpr) =>
      assert(l.tp == d.tp.lblTp)   // sanity check
      val (p, b) = unshredDict(d, l, ctx)
      Program(p.statements :+ Assignment(a.name, b))

    case _ =>
      sys.error("Unshredding not supported for " + quote(a))
  }

  private def unshredDict(d: BagDictExpr,
                          lbl: LabelExpr,
                          ctx: Context): (Program, BagExpr) = {
    d match {
//      case BagDictLet(x, e1: NewLabel, e2: BagDictExpr) =>
//        val ctx2 = ctx.addLabel(LabelVarRef(x.name, e1.tp), isTopLevel = e1.params.isEmpty)
//        unshredDict(e2, lbl, ctx2)
//
//      case BagDictLet(x, e1: BagDictExpr, e2: BagDictExpr) =>
//        val ctx2 = ctx.addDict(e1).addDictAlias(e1, BagDictVarRef(x.name, e1.tp))
//        unshredDict(e2, lbl, ctx2)
//
//      case d2: BagDict =>
//        unshredBagDict(d2, lbl, ctx, isTopLevel = true)

      case _ =>
        sys.error("Dictionary unshredding not supported for "+ quote(d))
    }
  }

  private def unshredBagDict(dict: BagDict, lbl: LabelExpr, ctx: Context, isTopLevel: Boolean): (Program, BagExpr) =
    dict match {
//      case BagDict(tp, ShredUnion(b1, b2), TupleDictUnion(d1, d2)) =>
//        val dict1 = BagDict(tp, b1, d1)
//        val dict2 = BagDict(tp, b2, d2)
//        val (prog1, bag1) = unshredBagDict(dict1, lbl, ctx, isTopLevel)
//        val (prog2, bag2) = unshredBagDict(dict2, lbl, ctx, isTopLevel)
//        (prog1 ++ prog2, Union(bag1, bag2))
//
//      case BagDict(_, _, tupleDict: TupleDict) if isTopLevel && tupleDict.isEmpty =>
//        val bagRef = ctx.matVarRef(dict).asInstanceOf[BagVarRef]
//        (Program(), bagRef)
//
//      case BagDict(_, _, tupleDict: TupleDict) if isTopLevel =>
//        val bagDictRef = ctx.matVarRef(dict).asInstanceOf[BagVarRef]
//        val tupleRef = TupleVarRef(Symbol.fresh(), bagDictRef.tp.tp)
//
//        // 1. Unshred children
//        val (childProgram, childTuple) = unshredTupleDict(tupleDict, tupleRef, ctx)
//
//        // 2. Compute nested object
//        val nestedBag = ForeachUnion(tupleRef.varDef, bagDictRef, Singleton(childTuple))
//
//        (Program(childProgram.statements), nestedBag)
//
//      case BagDict(_, _, tupleDict: TupleDict) if tupleDict.isEmpty =>
//        val matDictRef = ctx.matVarRef(dict).asInstanceOf[MatDictVarRef]
//        (Program(), MatDictLookup(lbl, matDictRef))
//
//      case BagDict(_, _, tupleDict: TupleDict) =>
//        val matDictRef = ctx.matVarRef(dict).asInstanceOf[MatDictVarRef]
//        val kvDict = MatDictToBag(matDictRef)
//        val kvRef = TupleVarRef(Symbol.fresh(name = "kv"), kvDict.tp.tp)
//
//        // 1. Unshred children
//        val (childProgram, childTuple) = unshredTupleDict(tupleDict, kvRef, ctx)
//
//        // 2. Compute nested object
//        val key = LabelProject(kvRef, KEY_ATTR_NAME)
//        val kvPairsNested =
//          BagToMatDict(
//            ForeachUnion(kvRef.varDef, kvDict,
//              Singleton(Tuple(childTuple.fields + (KEY_ATTR_NAME -> key)))))
//
//        // 3. Materialize unshredded dictionary
//        val uName = matDictRef.name.replace(MAT_DICT_PREFIX, UNSHRED_PREFIX)
//        val uDict = MatDictVarRef(uName, kvPairsNested.tp)
//        val uStmt = Assignment(uDict.name, kvPairsNested)
//
//        (Program(childProgram.statements :+ uStmt), MatDictLookup(lbl, uDict))

      case _ =>
        sys.error("[unshredBagDict] Unexpected dictionary type: " + dict)
    }

  private def unshredTupleDict(dict: TupleDict, tuple: TupleVarRef, ctx: Context): (Program, Tuple) =
    dict.fields.foldLeft (Program(), Tuple()) {
      case ((prog, tpl), (n, d: BagDict)) =>
        val lbl = LabelProject(tuple, n)
        val (p1, b1) = unshredBagDict(d, lbl, ctx, isTopLevel = false)
        (prog ++ p1, Tuple(tpl.fields + (n -> b1)))

      case ((prog, tpl), (n, EmptyDict)) =>
        (prog, Tuple(tpl.fields + (n -> tuple(n))))

      case (_, (n, d)) =>
        sys.error("Unexpected dictionary " + n + " = " + d)
    }

}
