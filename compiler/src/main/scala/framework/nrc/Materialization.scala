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

    def add(d: DictExpr, m: DictNode, parent: Option[(DictNode, String)]): DictionaryContext =
      new DictionaryContext(mapper + (d -> m), mexprs + (m -> parent))

    def add(m: DictNode, parent: Option[(DictNode, String)]): DictionaryContext =
      new DictionaryContext(mapper, mexprs + (m -> parent))

    def contains(d: DictExpr): Boolean = mapper.contains(d)

    def apply(d: DictExpr): DictNode = d match {
      case _: DictVarRef => mapper(d)
      case _: BagDict if mapper.contains(d) => mapper(d)
      case a: BagDict =>
        SDict(a)
      case a: TupleDict =>
        STuple(a.fields.collect { case (n, e: BagDictExpr) => n -> apply(e) })
      case a: BagDictProject =>
        Project(apply(a.tuple), a.field)
      case a: TupleDictProject =>
        STuple(childrenOf(apply(a.dict)))
      case a: DictLet if a.e1.isInstanceOf[DictExpr] =>
        val d1 = a.e1.asInstanceOf[DictExpr]
        val n1 = apply(d1)
        val ctx = add(DictVarRef(a.x.name, d1.tp), n1, None)     // don't care about parent as recur down
        SLet(a.x.name, a.e1, ctx(a.e2))
      case a: DictLet =>
        SLet(a.x.name, a.e1, apply(a.e2))
      case a: DictIfThenElse =>
        SIfThenElse(a.cond, apply(a.e1), apply(a.e2.get))
      case _ =>
        sys.error("[DictionaryContext.apply] Unexpected dictionary " + d)
    }

    def childrenOf(d: DictExpr): Map[String, DictNode] = childrenOf(mapper(d))

    def childrenOf(n: DictNode): Map[String, DictNode] = n match {
      case a: MaterializedDict =>
        mexprs.collect { case (child, Some((p, f))) if p == a => f -> child }.toMap
      case a: SDict =>
        val stuple = apply(a.d.dict)
        a.d.dict.tp.attrTps.collect { case (n, _: BagDictType) => n -> Project(stuple, n) }
      case a: STuple =>
        sys.error("[DictionaryContext.childrenOf] Cannot get children of STuple: " + a)
      case a: SLet =>
        val c = childrenOf(a.n2)
        c.map(x => x._1 -> SLet(a.name, a.e1, x._2))
      case a: SIfThenElse =>
        val c1 = childrenOf(a.n1)
        val c2 = childrenOf(a.n2)
        val fields = c1.keys ++ c2.keys
        fields.map(n => n -> SIfThenElse(a.cond, c1(n), c2(n))).toMap
    }

    override def toString(): String = {
      mapper.toString()+"\n"+mexprs.toString()
    }

  }

  type Context = DictionaryContext

  protected def initContext(p: ShredProgram, ctx: Context): Context =
    inputVars(p).foldLeft (ctx) {
      case (acc, d: BagDictVarRef) if !ctx.contains(d) => addInputDict(d, acc, None)
      case (acc, d: TupleDictVarRef) if !ctx.contains(d) => addInputDict(d, acc, None)
      case (acc, _) => acc
    }

  private def addInputDict(d: BagDictVarRef, ctx: Context, parent: Option[(MaterializedDict, String)]): Context = {
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

  private def addInputDict(d: TupleDictVarRef, ctx: Context, parent: Option[(MaterializedDict, String)]): Context = {
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
    case a: STuple => sys.error("[resolveLookup] Cannot get resolve lookup for STuple: " + a)
    case a: SLet => BagLet(VarDef(a.name, a.e1.tp), a.e1, resolveLookup(l, a.n2))
    case a: SIfThenElse => BagIfThenElse(a.cond, resolveLookup(l, a.n1), Some(resolveLookup(l, a.n2)))
    case _ => sys.error("Cannot get resolve lookup for " + d)
  }

  protected def resolveDict(d: DictNode): Expr = d match {
    case a: MaterializedDict => a.varRef
    case a: SDict => sys.error("[resolveDict] Cannot resolve symbolic dictionary " + a)
    case a: STuple => sys.error("[resolveDict] Cannot resolve STuple: " + a)
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

  class MProgram(val program: Program, val ctx: Context){
    override def toString: String = quote(program)
  }


  def materialize(p: ShredProgram,
                  ctx: Context = new Context,
                  eliminateDomains: Boolean = false,
                  inlineLets: Boolean = false): MProgram =
    materialize(p, ctx, MOpts(eliminateDomains, inlineLets))

  def materialize(p: ShredProgram, opts: Set[MaterializationOption]): MProgram =
    materialize(p, new Context, opts)

  def materialize(p: ShredProgram,
                  ctx: Context,
                  opts: Set[MaterializationOption]): MProgram = {
    Symbol.freshClear()

    // Create initial context with input dictionaries
    val initCtx = initContext(p, ctx)

    // Materialize each statement starting from empty program
    val emptyProgram = new MProgram(Program(), initCtx)
    p.statements.foldLeft (emptyProgram) { case (acc, s) =>
      val mp = materialize(s, acc.ctx, opts)
      new MProgram(acc.program ++ mp.program, mp.ctx)
    }
  }

  private def materialize(a: ShredAssignment,
                          ctx: Context,
                          opts: Set[MaterializationOption]): MProgram = {
    a.rhs match {

      case u:ShredUdf=> 
        val (mexprs, ctx2) =
          materializeUdf(u, dictName(a.name), ctx) //, opts, outputDict = true)
        val p = Program(mexprs.map(m => Assignment(m.name, m.e)))
        new MProgram(optimize(p), ctx2)

      case ShredExpr(_: PrimitiveExpr, EmptyDict) =>
        new MProgram(Program(), ctx)

      case ShredExpr(l: LabelExpr, d: BagDictExpr) =>
        assert(l.tp == d.tp.lblTp)   // sanity check
        val (mexprs, ctx2) =
          materializeDictExpr(d, dictName(a.name), ctx, None, opts, outputDict = true)
        val p = Program(mexprs.map(m => Assignment(m.name, m.e)))
        new MProgram(optimize(p), ctx2)

      case ShredExpr(_: TupleExpr, d: TupleDictExpr) =>
        val (mexprs, ctx2) =
          materializeDictExpr(d, dictName(a.name), ctx, None, opts, outputDict = true)
        val p = Program(mexprs.map(m => Assignment(m.name, m.e)))
        new MProgram(optimize(p), ctx2)

      case _ =>
        sys.error("Materialization not supported for " + quote(a))
    }
 }

  private def materializeDictExpr(dict: DictExpr,
                                  name: String,
                                  ctx: Context,
                                  parent: Option[(MaterializedDict, String)],
                                  opts: Set[MaterializationOption],
                                  outputDict: Boolean): (List[MaterializedDict], Context) =
    dict match {
      case EmptyDict | EmptyDictUnion => (Nil, ctx)

      case _: DictVarRef => (Nil, ctx)

      case BagDict(tp, ShredUnion(b1, b2), TupleDictUnion(d1, d2)) =>
        val (ee1, ctx1) =
          materializeDictExpr(BagDict(tp, b1, d1), name, ctx, parent, opts, outputDict)
        val (ee2, ctx2) =
          materializeDictExpr(BagDict(tp, b2, d2), name, ctx, parent, opts, outputDict)
        (ee1 ++ ee2, ctx1 ++ ctx2)

      case d: BagDict =>
        materializeBagDict(d, name, ctx, parent, opts, outputDict)

      case d: TupleDict =>
        materializeTupleDict(d, name, ctx, parent, opts, outputDict)

      case d: BagDictProject =>       // (Nil, ctx)
        materializeDictExpr(d.tuple, name, ctx, parent, opts, outputDict)

      case d: TupleDictProject =>     // (Nil, ctx)
        materializeDictExpr(d.dict, name, ctx, parent, opts, outputDict)

      case l: DictLet if l.e1.isInstanceOf[DictExpr] =>
        val d1 = l.e1.asInstanceOf[DictExpr]
        val (ee1, ctx1) =
          if (opts.contains(MOptInlineLets)) (Nil, ctx)
          else materializeDictExpr(d1, l.x.name, ctx, parent, opts, outputDict = false)
        val ctx2 =
          ctx1.add(DictVarRef(l.x.name, d1.tp), ctx1(d1), None)    // don't care about parent as recur down
        val (ee2, ctx3) =
          materializeDictExpr(l.e2, name, ctx2, parent, opts, outputDict)

        if (outputDict) {
          // Prefix each expression in ee2 with a let expression with r1
          val ee2let = ee2.map(m => MaterializedDict(m.name, Let(ee1, m.e)))
          (ee2let, ctx3)
        }
        else
          (ee1 ++ ee2, ctx3)

      case l: DictLet =>
        val r1 = rewriteUsingContext(l.e1, ctx)
        val (ee2, ctx2) =
          materializeDictExpr(l.e2, name, ctx, parent, opts, outputDict)

        // Prefix each expression in ee2 with a let expression with r1
        val ee2let = ee2.map(m => MaterializedDict(m.name, Let(l.x.name, r1, m.e)))
        (ee2let, ctx2)

      case i: DictIfThenElse =>
        val (ee1, ctx1) =
          materializeDictExpr(i.e1, name, ctx, parent, opts, outputDict)
        val (ee2, ctx2) =
          materializeDictExpr(i.e2.get, name, ctx, parent, opts, outputDict)
        (ee1 ++ ee2, ctx1 ++ ctx2)

      case d: DictUnion =>
        val (ee1, ctx1) =
          materializeDictExpr(d.dict1, name, ctx, parent, opts, outputDict)
        val (ee2, ctx2) =
          materializeDictExpr(d.dict2, name, ctx, parent, opts, outputDict)
        (ee1 ++ ee2, ctx1 ++ ctx2)

      case _ =>
        sys.error("Dictionary materialization not supported for " + quote(dict))
    }


  private def recurseDictType(name: String, tp: Type, top: Boolean = false): Seq[VarRef] = tp match {
    
    case TupleDictType(fs) => fs.flatMap(f => 
      recurseDictType(matMapName(name+"_"+f._1)+"_1", f._2)).toSeq

    case BagDictType(lt, ft, dt) => 
      val bname = if (top) matBagName(name) else name
      recurseDictType(bname, ft) ++ recurseDictType(name, dt)

    case bt:BagType => Seq(BagVarRef(name, bt))

    case _:PrimitiveType => 
      val bname = if (top) matBagName(name) else name
      Seq(VarRef(bname, tp).asInstanceOf[VarRef])

    case _ => Seq()

  }

  private def materializeUdf(udf: ShredUdf, 
                             name: String,
                             ctx:Context): (List[MaterializedDict], Context) = {

    // prepare inputs
    val inputName = udf.dict.asInstanceOf[BagDictVarRef].name+"_1"
    val inputs = recurseDictType(inputName, udf.dict.tp, top = true)

    // prepare udf names
    val onames = recurseDictType(udf.name+"_1", udf.otp, top = true)
    val anames = recurseDictType(name+"_1", udf.otp, top = true)

    val mexpr = onames.zipWithIndex.map{ case (n, i) => 
                  val mname = anames(i).asInstanceOf[VarRef].name
                  val nname = n.asInstanceOf[VarRef].name
                  MUdf(mname, MaterializedUdf(nname, inputs, n.tp, udf.params)) }
    
    // TODO update context with each of the anames 

    (mexpr.toList, ctx)

  }

  private def materializeBagDict(dict: BagDictExpr,
                                 name: String,
                                 ctx: Context,
                                 parent: Option[(MaterializedDict, String)],
                                 opts: Set[MaterializationOption],
                                 outputDict: Boolean): (List[MaterializedDict], Context) =
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
            materializeDictExpr(tupleDict, suffix, ctx2, Some(mexpr -> "tupleDict"), opts, outputDict)

          (mexpr :: children, ctx3)

      case BagDict(_, flat, tupleDict: TupleDictExpr) =>
        val isEliminated =
          if (opts.contains(MOptEliminateDomains)) eliminateDomain(flat, ctx) else None

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
              materializeDictExpr(tupleDict, suffix, ctx2, Some(mexpr -> "tupleDict"), opts, outputDict)

            (domain :: mexpr :: children, ctx3)

          case Some(flatBag) =>
            // 1. Create named expression
            val suffix = Symbol.fresh(name + "_")
            val mexpr = MaterializedDict(matMapName(suffix), BagToKeyValueMap(flatBag))

            // 2. Extend context
            val ctx2 = ctx.add(dict, mexpr, parent)

            // 3. Materialize children if needed
            val (children, ctx3) =
              materializeDictExpr(tupleDict, suffix, ctx2, Some(mexpr -> "tupleDict"), opts, outputDict)

            (mexpr :: children, ctx3)
        }

      case _ =>
        sys.error("[materializeBagDict] Unexpected dictionary type: " + dict)
    }

  private def materializeTupleDict(dict: TupleDict,
                                   name: String,
                                   ctx: Context,
                                   parent: Option[(MaterializedDict, String)],
                                   opts: Set[MaterializationOption],
                                   outputDict: Boolean): (List[MaterializedDict], Context) = {
    val (ff2, ee2, ctx2) =
      dict.fields.foldLeft (Map.empty[String, DictNode], List.empty[MaterializedDict], ctx) {
        case (acc, (_, EmptyDict)) => acc

        case ((ff1, ee1, ctx1), (f: String, d: BagDictExpr)) =>
          // Materialize child dictionary
          val p = parent.map(_._1 -> f)
          val (ee2, ctx2) =
            materializeDictExpr(d, name + "_" + f, ctx1, p, opts, outputDict)
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
    optimize(Program(p.statements.flatMap(unshred(_, ctx).statements)))
  }

  private def unshred(a: ShredAssignment, ctx: Context): Program = a.rhs match {
    case ShredExpr(e: PrimitiveExpr, EmptyDict) =>
      val e2 = rewriteUsingContext(e, ctx)
      Program(Assignment(a.name, e2))

    case ShredExpr(l: LabelExpr, d: BagDictExpr) =>
      assert(l.tp == d.tp.lblTp)   // sanity check
      val l2 = rewriteUsingContext(l, ctx).asInstanceOf[LabelExpr]
      val (prg, bag) = unshredBagDictExpr(d, l2, ctx)
      Program(prg.statements :+ Assignment(a.name, bag))

    case ShredExpr(t: TupleExpr, d: TupleDictExpr) =>
      val t2 = rewriteUsingContext(t, ctx).asInstanceOf[TupleExpr]
      val (prg, tpl) = unshredTupleDictExpr(d, t2, ctx)
      Program(prg.statements :+ Assignment(a.name, tpl))

    case _ =>
      sys.error("Unshredding not supported for " + quote(a))
  }

  private def unshredBagDictExpr(dict: BagDictExpr, lbl: LabelExpr, ctx: Context): (Program, BagExpr) = {
    dict match {
      case BagDict(tp, ShredUnion(b1, b2), TupleDictUnion(d1, d2)) =>
        val (p1, r1) = unshredBagDict(BagDict(tp, b1, d1), lbl, ctx)
        val (p2, r2) = unshredBagDict(BagDict(tp, b2, d2), lbl, ctx)
        (p1 ++ p2, Union(r1, r2))

      case d: BagDict =>
        unshredBagDict(d, lbl, ctx)

      case d: BagDictProject =>
        (Program(), resolveLookup(lbl, ctx(d)))

      case l: BagDictLet if l.e1.isInstanceOf[DictExpr] =>
        val d1 = l.e1.asInstanceOf[DictExpr]
        val ctx2 = ctx.add(DictVarRef(l.x.name, d1.tp), ctx(d1), None)
        unshredBagDictExpr(l.e2, lbl, ctx2)

      case l: BagDictLet =>
        val (prg, bag) = unshredBagDictExpr(l.e2, lbl, ctx)
        (prg, BagLet(l.x, l.e1, bag))

      case i: BagDictIfThenElse =>
        val c = rewriteUsingContext(i.cond, ctx).asInstanceOf[CondExpr]
        val (prg1, bag1) = unshredBagDictExpr(i.e1, lbl, ctx)
        val (prg2, bag2) = unshredBagDictExpr(i.e2.get, lbl, ctx)
        (prg1 ++ prg2, BagIfThenElse(c, bag1, Some(bag2)))

      case d: BagDictUnion =>
        val (prg1, bag1) = unshredBagDictExpr(d.dict1, lbl, ctx)
        val (prg2, bag2) = unshredBagDictExpr(d.dict2, lbl, ctx)
        (prg1 ++ prg2, Union(bag1, bag2))

      case _ =>
        sys.error("Dictionary unshredding not supported for "+ quote(dict))
    }
  }

  private def unshredTupleDictExpr(dict: TupleDictExpr, tuple: TupleExpr, ctx: Context): (Program, TupleExpr) =
    dict match {
      case d: TupleDict =>
        unshredTupleDict(d, tuple, ctx)

      case l: TupleDictLet if l.e1.isInstanceOf[DictExpr] =>
        val d1 = l.e1.asInstanceOf[DictExpr]
        val ctx2 = ctx.add(DictVarRef(l.x.name, d1.tp), ctx(d1), None)
        unshredTupleDictExpr(l.e2, tuple, ctx2)

      case l: TupleDictLet =>
        val (prg, tpl) = unshredTupleDictExpr(l.e2, tuple, ctx)
        (prg, TupleLet(l.x, l.e1, tpl))

      case i: TupleDictIfThenElse =>
        val c = rewriteUsingContext(i.cond, ctx).asInstanceOf[CondExpr]
        val (prg1, tpl1) = unshredTupleDictExpr(i.e1, tuple, ctx)
        val (prg2, tpl2) = unshredTupleDictExpr(i.e2.get, tuple, ctx)
        (prg1 ++ prg2, TupleIfThenElse(c, tpl1, tpl2))

      case _ =>
        sys.error("[unshredTupleDict] Unexpected dictionary type: " + dict)
    }

  private def unshredBagDict(dict: BagDict, lbl: LabelExpr, ctx: Context): (Program, BagExpr) =
    dict match {
      case d: BagDict if d.tupleDict.tp.isEmpty =>
        (Program(), resolveLookup(lbl, ctx(d)))

      case d: BagDict =>
        ctx(dict) match {
          case m: MBag =>
            val bagDictRef = m.varRef
            val tupleRef = TupleVarRef(Symbol.fresh(), bagDictRef.tp.tp)

            // 1. Unshred children
            val (childProgram, childTuple) = unshredTupleDictExpr(d.tupleDict, tupleRef, ctx)

            // 2. Compute nested object
            val nestedBag = ForeachUnion(tupleRef.varDef, bagDictRef, Singleton(childTuple))

            (Program(childProgram.statements), nestedBag)

          case m: MKeyValueMap =>
            val matDictRef = m.varRef
            val kvDict = KeyValueMapToBag(matDictRef)
            val kvRef = TupleVarRef(Symbol.fresh(name = "kv"), kvDict.tp.tp)

            // 1. Unshred children
            val (childProgram, childTuple) = unshredTupleDictExpr(d.tupleDict, kvRef, ctx)

            // 2. Compute nested object
            val key = LabelProject(kvRef, KEY_ATTR_NAME)
            val kvPairsNested =
              BagToKeyValueMap(
                ForeachUnion(kvRef.varDef, kvDict,
                  Singleton(addOutputField(KEY_ATTR_NAME -> key, childTuple))))

            // 3. Materialize unshredded dictionary
            val uName = matDictRef.name.replace(MAT_MAP_PREFIX, UNSHRED_PREFIX)
            val uDict = KeyValueMapVarRef(uName, kvPairsNested.tp)
            val uStmt = Assignment(uDict.name, kvPairsNested)

            (Program(childProgram.statements :+ uStmt), KeyValueMapLookup(lbl, uDict))
          case _ => ???
        }

      case _ =>
        sys.error("[unshredBagDict] Unexpected dictionary type: " + dict)
    }

  private def unshredTupleDict(dict: TupleDict, tuple: TupleExpr, ctx: Context): (Program, TupleExpr) =
    dict.fields.foldLeft (Program(), Tuple()) {
      case ((prg, tpl), (n, d: BagDictExpr)) =>
        val lbl = Project(tuple, n).asInstanceOf[LabelExpr]
        val (p1, b1) = unshredBagDictExpr(d, lbl, ctx)
        (prg ++ p1, Tuple(tpl.fields + (n -> b1)))

      case ((prg, tpl), (n, EmptyDict)) =>
        (prg, Tuple(tpl.fields + (n -> tuple(n))))

      case (_, (n, d)) =>
        sys.error("[unshredTupleDict] Unexpected dictionary " + n + " = " + d)
    }

}