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

  class DictionaryContext(private val mapper: Map[DictExpr, MExpr],
                          private val mexprs: Map[MExpr, Option[(MExpr, String)]]) {

    def this() = this(Map.empty, Map.empty)

    def ++(c: DictionaryContext): DictionaryContext =
      new DictionaryContext(mapper ++ c.mapper, mexprs ++ c.mexprs)

    def add(d: DictExpr, m: MExpr, parent: Option[(MExpr, String)]): DictionaryContext =
      new DictionaryContext(mapper + (d -> m), mexprs + (m -> parent))

    def contains(d: DictExpr): Boolean = mapper.contains(d)

    def apply(d: DictExpr): MExpr = mapper(d)

    def childrenOf(m: MExpr): Map[String, MExpr] =
      mexprs.collect {
        case (child, Some((parent, field))) if parent == m => field -> child
      }

    def find(e: Expr): Option[MExpr] = mexprs.keys.find(_.e == e)
  }

  type Context = DictionaryContext

  protected def initContext(p: ShredProgram, ctx: Context): Context =
    inputVars(p).foldLeft (ctx) {
      case (acc, d: BagDictVarRef) if !ctx.contains(d) => addInputDict(d, acc, None)
      case (acc, d: TupleDictVarRef) if !ctx.contains(d) => addInputDict(d, acc, None)
      case (acc, _) => acc
    }

  private def addInputDict(d: BagDictVarRef, ctx: Context, parent: Option[(MExpr, String)]): Context = {
    val matRef = if (parent.isEmpty)
      BagVarRef(inputBagName(d.name), d.tp.flatTp)
    else
      KeyValueMapVarRef(inputMapName(d.name), KeyValueMapType(d.tp.lblTp, d.tp.flatTp))
    val matExpr = MExpr(matRef.name, matRef)
    val newCtx = ctx.add(d, matExpr, parent)

    d.tp.dictTp.attrTps.foldLeft (newCtx) {
      case (acc, (n, t: BagDictType)) =>
        val child = BagDictVarRef(d.name + "_" + n, t)
        addInputDict(child, acc, Some(matExpr -> n))
      case (acc, (_, EmptyDictType)) => acc
    }
  }

  // TODO:
  private def addInputDict(d: TupleDictVarRef, ctx: Context, parent: Option[(MExpr, String)]): Context = {
    sys.error("Not implemented")
  }

//  def rewriteUsingContext(e0: Expr, ctx0: Context): (Expr, Context) = replace[Context](e0, ctx0, {
//    case (v: VarRef, ctx) if ctx.contains(v.name) =>
//      (VarRef(ctx.varDef(v.name)), ctx)
//
//    case (Lookup(l, d), ctx) if ctx.isTopLevel(d) && ctx.isMaterialized(d) =>
//      assert(ctx.isTopLevel(l))  // sanity check
//      (ctx.matVarRef(d).asInstanceOf[BagVarRef], ctx)
//
//    case (Lookup(l, d), ctx) if ctx.isTopLevel(d) =>
//      ctx.dictDef(d) match {
//        case BagDict(lblTp, flat, _) =>
//          assert(l.tp == lblTp)   // sanity check
//          (flat, ctx)
//        case _ =>
//          sys.error("Unexpected dictionary type: " + d)
//      }
//
//    case (Lookup(l, d), ctx) =>
//      assert(ctx.contains(d))  // sanity check
//      val dict = ctx.matVarRef(d).asInstanceOf[MatDictExpr]
//      val (lbl: LabelExpr, ctx1) = rewriteUsingContext(l, ctx)
//      (MatDictLookup(lbl, dict), ctx1)
//
//    case (BagLet(x, TupleDictProject(d), e2), ctx) =>
//      assert(ctx.contains(d))  // sanity check
//      val newCtx = ctx.children(d).foldLeft (ctx) {
//        case (acc, (n, dict)) =>
//          val tr = TupleDictVarRef(x.name, x.tp.asInstanceOf[TupleDictType])
//          acc.addDictAlias(dict, BagDictProject(tr, n))
//      }
//      rewriteUsingContext(e2, newCtx)
//
//    case (l: NewLabel, ctx) =>
//      val (ps1, ctx1) =
//        l.params.foldLeft (Map.empty[String, LabelParameter], ctx) {
//          case ((acc, ctx), (_, VarRefLabelParameter(l: LabelExpr)))
//            if ctx.contains(l) && ctx.isTopLevel(l) =>
//            (acc, ctx)
//          case ((acc, ctx), (_, VarRefLabelParameter(d: BagDictExpr))) =>
//            assert(ctx.contains(d))  // sanity check
//            (acc, ctx)
//          case ((acc, ctx), (n, ProjectLabelParameter(d: BagDictExpr))) =>
//            assert(ctx.contains(d))  // sanity check
//            val ctx1 = ctx.addDictAlias(d, BagDictVarRef(n, d.tp))
//            (acc, ctx1)
//          case ((acc, ctx), (n, p0)) =>
//            val (p1: LabelParameter, ctx1) = rewriteUsingContext(p0, ctx)
//            (acc + (n -> p1), ctx1)
//        }
//      (NewLabel(ps1, l.id), ctx1)
//
//    case (ForeachUnion(x, e1, e2), ctx) =>
//      val (r1: BagExpr, ctx1) = rewriteUsingContext(e1, ctx)
//      val xd = VarDef(x.name, r1.tp.tp)
//      val ctx2 = ctx1.addVarDef(xd)
//      val (r2: BagExpr, ctx3) = rewriteUsingContext(e2, ctx2)
//      (ForeachUnion(xd, r1, r2), ctx3)
//
//    case (l: Let, ctx) =>
//      val (r1, ctx1) = rewriteUsingContext(l.e1, ctx)
//      val xd = VarDef(l.x.name, r1.tp)
//      val ctx2 = ctx1.addVarDef(xd)
//      val (r2, ctx3) = rewriteUsingContext(l.e2, ctx2)
//      (Let(xd, r1, r2), ctx3)
//
//    case (x: ExtractLabel, ctx) =>
//      val (l: LabelExpr, ctx1) = rewriteUsingContext(x.lbl, ctx)
//      val ctx2 = l.tp.attrTps.foldLeft (ctx1) {
//        case (acc, (n, t)) => acc.addVarDef(VarDef(n, t))
//      }
//      val (r, ctx3) = rewriteUsingContext(x.e, ctx2)
//      (ExtractLabel(l, r), ctx3)
//  })
//}

  protected def rewriteUsingContext(e0: Expr, ctx: Context): Expr = replace(e0, {
    case v: DictVarRef =>
      ctx(v.asInstanceOf[DictExpr]).varRef

    case Lookup(l, d) =>
      rewriteUsingContext(d, ctx) match {
        case e: BagExpr => e
        case e: KeyValueMapExpr =>
          val l2 = rewriteUsingContext(l, ctx).asInstanceOf[LabelExpr]
          KeyValueMapLookup(l2, e)
      }

    case BagDictProject(t, f) =>
      rewriteUsingContext(t, ctx).asInstanceOf[TupleVarRef].apply(f)

    case TupleDictProject(v: BagDictVarRef) =>
      val mparent = ctx(v)
      val children = ctx.childrenOf(mparent)
      val fields = children.map(m => m._1 -> m._2.varRef.asInstanceOf[TupleAttributeExpr])
      Tuple(fields)

    case TupleDictProject(p: BagDictProject) =>
      val mgrandparent = ctx(p.tuple).asInstanceOf[MTuple]
      val mparent = ctx.find(mgrandparent.e(p.field)).get
      val children = ctx.childrenOf(mparent)
      val fields = children.map(m => m._1 -> m._2.varRef.asInstanceOf[TupleAttributeExpr])
      Tuple(fields)

    case l: Let if l.e1.isInstanceOf[DictExpr] =>
      val xref = DictVarRef(l.x.name, l.e1.tp.asInstanceOf[DictType])
      rewriteUsingContext(l.e1, ctx) match {
        case r1: BagExpr =>
          val ctx2 = ctx.add(xref, MBag(l.x.name, r1), None)
          val r2 = rewriteUsingContext(l.e2, ctx2)
          Let(VarDef(l.x.name, r1.tp), r1, r2)
        case r1: KeyValueMapExpr =>
          val ctx2 = ctx.add(xref, MKeyValueMap(l.x.name, r1), None)
          val r2 = rewriteUsingContext(l.e2, ctx2)
          Let(VarDef(l.x.name, r1.tp), r1, r2)
        case r1: TupleExpr =>
          val ctx2 = ctx.add(xref, MTuple(l.x.name, r1), None)
          val r2 = rewriteUsingContext(l.e2, ctx2)
          Let(VarDef(l.x.name, r1.tp), r1, r2)
      }

    case d: TupleDict if d.isEmpty => Tuple()

    case _: BagDict =>
      sys.error("[rewriteUsingContext] Unexpected BagDict")

    case _: TupleDict =>
      sys.error("[rewriteUsingContext] Unexpected TupleDict")
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
        val (mexprs, ctx2) = materializeDictExpr(d, a.name, ctx, None, eliminateDomains)
        val p = Program(mexprs.map(m => Assignment(m.name, m.e)))
        new MProgram(p, ctx2)

      case ShredExpr(t: TupleExpr, d: TupleDictExpr) =>
        // TODO: this case
        sys.error("Materialization not supported for " + quote(a))

      case _ =>
        sys.error("Materialization not supported for " + quote(a))
    }

  private def materializeDictExpr( dict: DictExpr,
                                   name: String,
                                   ctx: Context,
                                   parent: Option[(MExpr, String)],
                                   eliminateDomains: Boolean
                                 ): (List[MExpr], Context) =
    dict match {
      case l: Let if l.e1.isInstanceOf[DictExpr] =>
        val (ee1, ctx1) =
          materializeDictExpr(l.e1.asInstanceOf[DictExpr], l.x.name, ctx, parent, eliminateDomains)
        val (ee2, ctx2) =
          materializeDictExpr(l.e2.asInstanceOf[DictExpr], name, ctx1, parent, eliminateDomains)

        // Prefix each expression in ee2 with a let expression over ee1
        val ee2let = ee2.map(m => MExpr(m.name, Let(ee1, m.e)))
        (ee2let, ctx2)

      case l: Let =>
        val r1 = rewriteUsingContext(l.e1, ctx)
        val (ee2, ctx2) =
          materializeDictExpr(l.e2.asInstanceOf[DictExpr], name, ctx, parent, eliminateDomains)

        // Prefix each expression in ee2 with a let expression with r1
        val ee2let = ee2.map(m => MExpr(m.name, Let(l.x, r1, m.e)))
        (ee2let, ctx2)

      case d: BagDict =>
        materializeBagDict(d, name, ctx, parent, eliminateDomains)

      case d: TupleDict =>
        materializeTupleDict(d, name, ctx, parent, eliminateDomains)

      case d @ TupleDictProject(v: BagDictVarRef) =>
        val mparent = ctx(v)
        val children = ctx.childrenOf(mparent)
        if (children.isEmpty) (Nil, ctx) else {
          val fields = children.map(m => m._1 -> m._2.varRef.asInstanceOf[TupleAttributeExpr])
          val mexpr = MExpr(matTupleName(name), Tuple(fields))
          // Extend context
          val ctx2 = ctx.add(TupleDictVarRef(name, d.tp), mexpr, Some(mparent -> "tupleDict"))
          (List(mexpr), ctx2)
        }

      case d @ TupleDictProject(p: BagDictProject) =>
        val mgrandparent = ctx(p.tuple).asInstanceOf[MTuple]
        val mparent = ctx.find(mgrandparent.e(p.field)).get
        val children = ctx.childrenOf(mparent)
        if (children.isEmpty) (Nil, ctx) else {
          val fields = children.map(m => m._1 -> m._2.varRef.asInstanceOf[TupleAttributeExpr])
          val mexpr = MExpr(matTupleName(name), Tuple(fields))
          // Extend context
          val ctx2 = ctx.add(TupleDictVarRef(name, d.tp), mexpr, Some(mparent -> "tupleDict"))
          (List(mexpr), ctx2)
        }


//        val mtuple = ctx(p.tuple).asInstanceOf[MTuple]
//        val mfield = mtuple.e(p.field)
//        val children = ctx.childrenOf(mfield)
//        val fields = children.map(m => m._1 -> m._2.varRef.asInstanceOf[TupleAttributeExpr])
//        Tuple(fields)

      //      case d: DictVarRef => (ctx(d), Nil)
//
//      case _: BagDictProject => (Nil, ctx)
//
//      case _: BagDictIfThenElse => (Nil, ctx)
//
//      case _: TupleDictIfThenElse => (Nil, ctx)

//      case BagDictLet(x, e1: NewLabel, e2: BagDictExpr) =>
//        val ctx2 = ctx.addLabel(LabelVarRef(x.name, e1.tp), isTopLevel = e1.params.isEmpty)
//        val seq2 = materializeBagDict(e2, name, ctx2, eliminateDomains)
//        val ss2 = seq2.exprs.map(s => s._1 -> makeLet(List(x.name -> e1), s._2))
//        new NamedSequence(ss2, seq2.ctx)

      case _ =>
        sys.error("Dictionary materialization not supported for "+ quote(dict))
    }

  private def materializeBagDict( dict: BagDict,
                                  name: String,
                                  ctx: Context,
                                  parent: Option[(MExpr, String)],
                                  eliminateDomains: Boolean
                                ): (List[MExpr], Context) =
    dict match {
      case BagDict(tp, ShredUnion(b1, b2), TupleDictUnion(d1, d2)) =>
        val (ee1, ctx1) = {
          val bd1 = BagDict(tp, b1, d1)
          materializeBagDict(bd1, name, ctx, parent, eliminateDomains)
        }
        val (ee2, ctx2) = {
          val bd2 = BagDict(tp, b2, d2)
          materializeBagDict(bd2, name, ctx, parent, eliminateDomains)
        }
        (ee1 ++ ee2, ctx1 ++ ctx2)

      case BagDict(_, flat, tupleDict: TupleDictExpr) if parent.isEmpty =>
          // 1. Eliminate symbolic dictionaries from flat expression
          val flatBag = rewriteUsingContext(flat, ctx).asInstanceOf[BagExpr]

          // 2. Create named expression
          val suffix = Symbol.fresh(name + "_")
          val mexpr = MExpr(matBagName(suffix), flatBag)

          // 3. Extend context
          val ctx2 = ctx.add(BagDictVarRef(name, dict.tp), mexpr, parent)

          // 4. Materialize children
          val (children, ctx3) =
            materializeDictExpr(tupleDict, suffix, ctx2, Some(mexpr -> "tupleDict"), eliminateDomains)

          (mexpr :: children, ctx3)

      case BagDict(_, flat, tupleDict: TupleDictExpr) =>
        val (parentDict, field) = parent.get

        // Get new label type for flat lambda expression
        val newLabelTp = parentDict match {
          case d: MBag => d.e.tp.tp(field).asInstanceOf[LabelType]
          case d: MKeyValueMap => d.e.tp.valueTp.tp(field).asInstanceOf[LabelType]
        }

        val eliminated = if (eliminateDomains) eliminateDomain(newLabelTp, flat, ctx) else None

        eliminated match {
          case None =>
            // 1. Create label domain
            val domain = createLabelDomain(parentDict, field)
            val domainRef = domain.varRef

            // 2. Create named expression
            val tpl = TupleVarRef(Symbol.fresh(name = "l"), domainRef.tp.tp)
            val lbl = LabelProject(tpl, LABEL_ATTR_NAME)
            val dictBag =
              ForeachUnion(tpl, domainRef,
                addOutputField(KEY_ATTR_NAME -> lbl, BagExtractLabel(lbl, flat)))
            val dictBag2 =
              rewriteUsingContext(dictBag, ctx).asInstanceOf[BagExpr]

            // 3. Create assignment statement
            val suffix = Symbol.fresh(name + "_")
            val mexpr = MExpr(matMapName(suffix), BagToKeyValueMap(dictBag2))

            // 4. Extend context
            val ctx2 = ctx.add(BagDictVarRef(name, dict.tp), mexpr, parent)

            // 5. Materialize children if needed
            val (children, ctx3) =
              materializeDictExpr(tupleDict, suffix, ctx2, Some(mexpr -> "tupleDict"), eliminateDomains)

            (domain :: mexpr :: children, ctx3)

          case Some((flatBag, flatCtx)) =>
            // 1. Create named expression
            val suffix = Symbol.fresh(name + "_")
            val kvDict = BagToKeyValueMap(flatBag)
            val mexpr = MExpr(matMapName(suffix), kvDict)

            // 2. Extend context
            val ctx2 = flatCtx.add(BagDictVarRef(name, dict.tp), mexpr, parent)

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
                                   parent: Option[(MExpr, String)],
                                   eliminateDomains: Boolean
                                  ): (List[MExpr], Context) = {
    dict.fields.foldLeft (List.empty[MExpr], ctx) {
      case ((alist, actx), (n: String, d: BagDictExpr)) =>
        // Materialize child dictionary
        val cparent = parent.map(_._1 -> n)
        val (clist, cctx) =
          materializeDictExpr(d, name + "_" + n, ctx, cparent, eliminateDomains)
        (alist ++ clist, actx ++ cctx)

      case (acc, (_, EmptyDict)) =>
        acc

      case (_, (_, d)) =>
        sys.error("[materializeTupleDict] Unsupported dictionary type: " + d)
    }
  }


  def unshred(p: ShredProgram, ctx: Context): Program = {
    Symbol.freshClear()
    Program(p.statements.flatMap(unshred(_, ctx).statements))
  }

  private def unshred(a: ShredAssignment, ctx: Context): Program = a.rhs match {
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
