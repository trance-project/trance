package shredding.nrc

import shredding.utils.Utils.Symbol
import shredding.core._

/**
  * Materialization of nested output queries
  */
trait BaseMaterialization {

  val INPUT_BAG_PREFIX: String = "IBag_"

  val INPUT_DICT_PREFIX: String = "IDict_"

  val MAT_BAG_PREFIX: String = "MBag_"

  val MAT_DICT_PREFIX: String = "MDict_"

  val LABEL_DOMAIN_PREFIX: String = "Dom_"

  val UNSHRED_PREFIX: String = "UDict_"

  def inputBagName(name: String): String = INPUT_BAG_PREFIX + name

  def inputDictName(name: String): String = INPUT_DICT_PREFIX + name

  def matBagName(name: String): String = MAT_BAG_PREFIX + name

  def matDictName(name: String): String = MAT_DICT_PREFIX + name

  def domainName(name: String): String = LABEL_DOMAIN_PREFIX + name

  def unshredDictName(name: String): String = UNSHRED_PREFIX + name

}

trait MaterializationContext extends BaseMaterialization {
  this: MaterializeNRC =>

  class DictInfo(val dict: BagDictExpr, val ref: VarRef, val parent: Option[(BagDictExpr, String)])

  class LabelInfo(val isTopLevel: Boolean)

  class Context(private val dictCtx: Map[BagDictExpr, DictInfo],
                private val labelCtx: Map[LabelExpr, LabelInfo],
                private val scope: Map[String, VarDef]) {

    def addDict(dict: BagDictExpr, ref: VarRef, parent: Option[(BagDictExpr, String)]): Context =
      new Context(dictCtx + (dict -> new DictInfo(dict, ref, parent)), labelCtx, scope)

    def addDictAlias(dict: BagDictExpr, alias: BagDictExpr): Context =
      new Context(dictCtx + (alias -> dictCtx(dict)), labelCtx, scope)

    def contains(d: BagDictExpr): Boolean = dictCtx.contains(d)

    def isTopLevel(d: BagDictExpr): Boolean = dictCtx(d).parent.isEmpty

    def matVarRef(d: BagDictExpr): VarRef = dictCtx(d).ref

    def children(d: BagDictExpr): Map[String, BagDictExpr] = {
      val reference = dictCtx(d).dict
      dictCtx.values.collect {
        case i: DictInfo if i.parent.exists(_._1 == reference) =>
          i.parent.get._2 -> i.dict
      }.toMap
    }

    def addLabel(l: LabelExpr, isTopLevel: Boolean): Context =
      new Context(dictCtx, labelCtx + (l -> new LabelInfo(isTopLevel)), scope)

    def contains(l: LabelExpr): Boolean = labelCtx.contains(l)

    def isTopLevel(l: LabelExpr): Boolean = labelCtx(l).isTopLevel

    def addVarDef(v: VarDef): Context =
      new Context(dictCtx, labelCtx, scope + (v.name -> v))

    def removeVarDef(v: VarDef): Context =
      new Context(dictCtx, labelCtx, scope - v.name)

    def contains(n: String): Boolean = scope.contains(n)

    def varDef(n: String): VarDef = scope(n)

    def ++(c: Context): Context =
      new Context(dictCtx ++ c.dictCtx, labelCtx ++ c.labelCtx, scope ++ c.scope)

  }

  def initContext(p: ShredProgram): Context =
    inputVars(p).foldLeft (new Context(Map.empty, Map.empty, Map.empty)) {
      case (acc, d: BagDictVarRef) =>
        addInputDict(d, acc, None)
      case (acc, l: LabelVarRef) =>
        acc.addLabel(l, isTopLevel = true)
      case (acc, _) => acc
    }

  private def addInputDict(d: BagDictVarRef, ctx: Context, parent: Option[(BagDictExpr, String)]): Context = {
    val matRef = if (parent.isEmpty)
      BagVarRef(inputBagName(d.name), d.tp.flatTp)
    else
      MatDictVarRef(inputDictName(d.name), MatDictType(d.tp.lblTp, d.tp.flatTp))
    val newCtx = ctx.addDict(d, matRef, parent)

    d.tp.dictTp.attrTps.foldLeft (newCtx) {
      case (acc, (n, t: BagDictType)) =>
        val child = BagDictVarRef(d.name + "_" + n, t)
        addInputDict(child, acc, Some(d -> n))
      case (acc, (_, EmptyDictType)) => acc
    }
  }

}

trait Materialization extends MaterializationContext {
  this: MaterializeNRC with Optimizer with Printer =>

  class MaterializedProgram(val program: Program, val ctx: Context) {
    def ++(m: MaterializedProgram): MaterializedProgram =
      new MaterializedProgram(program ++ m.program, ctx ++ m.ctx)
  }

  def materialize(p: ShredProgram, eliminateDomains: Boolean = false): MaterializedProgram = {
    Symbol.freshClear()
    // Create initial context with top-level dictionaries
    val ctx = initContext(p)
    // Materialize each statement starting from empty program
    val emptyProgram = new MaterializedProgram(Program(), ctx)
    p.statements.foldLeft (emptyProgram) { case (acc, s) =>
      val mat = materialize(s, acc.ctx, eliminateDomains)
      new MaterializedProgram(acc.program ++ mat.program, mat.ctx)
    }
  }

  private def materialize(a: ShredAssignment, ctx: Context, eliminateDomains: Boolean): MaterializedProgram =
    a.rhs match {
      case ShredExpr(l: NewLabel, d: BagDict) =>
        assert(l.tp == d.lblTp)   // sanity check
        materializeBagDict(d, a.name, ctx, None, None, eliminateDomains)
      case _ =>
        sys.error("Materialization not supported for " + quote(a))
    }

  private def materializeBagDict(dict: BagDict,
                                 name: String,
                                 ctx: Context,
                                 parent: Option[(BagDictExpr, String)],
                                 labelDomain: Option[BagVarRef],
                                 eliminateDomains: Boolean
                                ): MaterializedProgram =
    dict match {
      case BagDict(tp, ShredUnion(b1, b2), TupleDictUnion(d1, d2)) =>
        val dict1 = BagDict(tp, b1, d1)
        val dict2 = BagDict(tp, b2, d2)
        materializeBagDict(dict1, name, ctx, parent, labelDomain, eliminateDomains) ++
          materializeBagDict(dict2, name, ctx, parent, labelDomain, eliminateDomains)

      case BagDict(lblTp, flat, tupleDict: TupleDictExpr) if parent.isEmpty =>
        // 1. Create dictionary bag expression
        val (bag: BagExpr, ctx2) = rewriteUsingContext(flat, ctx)

        // 2. Create assignment statement
        val suffix = Symbol.fresh(name + "_")
        val bagRef = BagVarRef(matBagName(suffix), bag.tp)
        val stmt = Assignment(bagRef.name, bag)

        // 3. Extend context
        val dictCtx =
            ctx2.addDict(dict, bagRef, parent)
              .addDictAlias(dict, BagDictVarRef(dictName(name), dict.tp))
              .addLabel(LabelVarRef(flatName(name), lblTp), parent.isEmpty)

        // 4. Materialize bag dictionary
        val program = new MaterializedProgram(Program(stmt), dictCtx)

        // 5. Materialize children if needed
        tupleDict match {
          case d: TupleDict =>
            program ++ materializeTupleDict(d, suffix, dictCtx, dict, eliminateDomains)
          case _ =>
            program
        }

      case BagDict(_, flat, tupleDict: TupleDictExpr) if labelDomain.nonEmpty =>
        val domainRef = labelDomain.get

        // 1. Create dictionary bag expression
        val tpl = TupleVarRef(Symbol.fresh(name = "l"), domainRef.tp.tp)
        val lbl = LabelProject(tpl, LABEL_ATTR_NAME)
        val (valueBag: BagExpr, ctx2) =
          rewriteUsingContext(BagExtractLabel(lbl, flat), ctx)
        val bag =
          ForeachUnion(tpl, domainRef,
            Singleton(Tuple(KEY_ATTR_NAME -> lbl, VALUE_ATTR_NAME -> valueBag)))

        // 2. Create assignment statement
        val suffix = Symbol.fresh(name + "_")
        val matDict = BagToMatDict(bag)
        val matDictRef = MatDictVarRef(matDictName(suffix), matDict.tp)
        val stmt = Assignment(matDictRef.name, matDict)

        // 3. Extend context
        val dictCtx = ctx2.addDict(dict, matDictRef, parent)

        // 4. Materialize bag dictionary
        val program = new MaterializedProgram(Program(stmt), dictCtx)

        // 5. Materialize children if needed
        tupleDict match {
          case d: TupleDict =>
            program ++ materializeTupleDict(d, suffix, dictCtx, dict, eliminateDomains)
          case _ =>
            program
        }

      case BagDict(_, flat, tupleDict: TupleDictExpr) =>
        assert(eliminateDomains && canEliminateDomain(flat))   // sanity check

        // 1. Create dictionary bag expression
        val (bag: BagExpr, ctx2) =
          rewriteUsingContext(eliminateDomain(flat).get, ctx)

        // 2. Create assignment statement
        val suffix = Symbol.fresh(name + "_")
        val matDict = BagToMatDict(bag)
        val matDictRef = MatDictVarRef(matDictName(suffix), matDict.tp)
        val stmt = Assignment(matDictRef.name, matDict)

        // 3. Extend context
        val dictCtx = ctx2.addDict(dict, matDictRef, parent)

        // 4. Materialize bag dictionary
        val program = new MaterializedProgram(Program(stmt), dictCtx)

        // 5. Materialize children if needed
        tupleDict match {
          case d: TupleDict =>
            program ++ materializeTupleDict(d, suffix, dictCtx, dict, eliminateDomains)
          case _ =>
            program
        }

      case _ =>
        sys.error("[materializeBagDict] Unexpected dictionary type: " + dict)
    }

  private def materializeTupleDict(dict: TupleDict,
                                   name: String,
                                   ctx: Context,
                                   parentDict: BagDict,
                                   eliminateDomains: Boolean
                                  ): MaterializedProgram =
    dict.fields.foldLeft (new MaterializedProgram(Program(), ctx)) {
      case (acc, (n: String, d: BagDict))
        if eliminateDomains && canEliminateDomain(d.flat) =>
        // 1. Materialize child dictionary
        val childMatProgram =
          materializeBagDict(d, name + "_" + n, ctx, Some(parentDict -> n), None, eliminateDomains)

        acc ++ childMatProgram

      case (acc, (n: String, d: BagDict)) =>
        // 1. Create label domain
        val domain = createLabelDomain(ctx.matVarRef(parentDict), ctx.isTopLevel(parentDict), n)
        val domainRef = BagVarRef(domain.name, domain.rhs.tp.asInstanceOf[BagType])

        // 2. Materialize child dictionary
        val childMatProgram =
          materializeBagDict(d, name + "_" + n, ctx, Some(parentDict -> n), Some(domainRef), eliminateDomains)

        val program = Program(domain :: childMatProgram.program.statements)
        acc ++ new MaterializedProgram(program, childMatProgram.ctx)

      case (acc, _) => acc
    }

  private def canEliminateDomain(b: BagExpr): Boolean = b match {
    case ForeachUnion(_, _, BagIfThenElse(c, Singleton(Tuple(_)), None)) =>
      val iv = inputVars(b)
      c match {
        case PrimitiveCmp(OpEq, p1: Project, p2: VarRef) =>
          !iv.contains(p1.tuple) && iv.contains(p2)
        case PrimitiveCmp(OpEq, p1: VarRef, p2: Project) =>
          !iv.contains(p2.tuple) && iv.contains(p1)
        case _ => false
      }
    case _ => false
  }

  private def eliminateDomain(b: BagExpr): Option[BagExpr] = b match {
    case ForeachUnion(x, b1, BagIfThenElse(c, Singleton(Tuple(fs)), None)) =>
      val iv = inputVars(b)
      c match {
        case PrimitiveCmp(OpEq, p1: Project, p2: VarRef)
          if !iv.contains(p1.tuple) && iv.contains(p2) =>
          val lbl = NewLabel(Map(p2.name -> ProjectLabelParameter(p1)))
          val fs1 = fs + (KEY_ATTR_NAME -> lbl)
          Some(
            GroupByKey(
              ForeachUnion(x, b1, Singleton(Tuple(fs1))),
              List(KEY_ATTR_NAME),
              fs.keys.toList,
              VALUE_ATTR_NAME
            )
          )
        case PrimitiveCmp(OpEq, p1: VarRef, p2: Project)
          if !iv.contains(p2.tuple) && iv.contains(p1) =>
          val lbl = NewLabel(Map(p1.name -> ProjectLabelParameter(p2)))
          val fs1 = fs + (KEY_ATTR_NAME -> lbl)
          Some(
            GroupByKey(
              ForeachUnion(x, b1, Singleton(Tuple(fs1))),
              List(KEY_ATTR_NAME),
              fs.keys.toList,
              VALUE_ATTR_NAME
            )
          )
        case _ => None
      }
    case _ => None
  }

  private def createLabelDomain(varRef: VarRef, topLevel: Boolean, field: String): Assignment = {
    val domain = if (topLevel) {
      val bagVarRef = varRef.asInstanceOf[BagVarRef]
      val x = TupleVarRef(Symbol.fresh(), bagVarRef.tp.tp)
      val lbl = LabelProject(x, field)
      DeDup(
        ForeachUnion(x, bagVarRef, Singleton(Tuple(LABEL_ATTR_NAME -> lbl)))
      )
    }
    else {
      val matDictVarRef = varRef.asInstanceOf[MatDictVarRef]
      val kvTp =
        TupleType(
          KEY_ATTR_NAME -> matDictVarRef.tp.keyTp,
          VALUE_ATTR_NAME -> matDictVarRef.tp.valueTp
        )
      val kv = TupleVarRef(Symbol.fresh(name = "kv"), kvTp)
      val values = BagProject(kv, VALUE_ATTR_NAME)
      val x = TupleVarRef(Symbol.fresh(), values.tp.tp)
      val lbl = LabelProject(x, field)
      DeDup(
        ForeachUnion(kv, MatDictToBag(matDictVarRef),
          ForeachUnion(x, values, Singleton(Tuple(LABEL_ATTR_NAME -> lbl))))
      )
    }

    val bagRef = BagVarRef(domainName(Symbol.fresh(field)), domain.tp)
    Assignment(bagRef.name, domain)
  }

  private def rewriteUsingContext(e0: Expr, ctx0: Context): (Expr, Context) = replace[Context](e0, ctx0, {
    case (v: VarRef, ctx) if ctx.contains(v.name) =>
      (VarRef(ctx.varDef(v.name)), ctx)

    case (Lookup(l, d), ctx) if ctx.isTopLevel(d) =>
      assert(ctx.isTopLevel(l))  // sanity check
      (ctx.matVarRef(d).asInstanceOf[BagVarRef], ctx)

    case (Lookup(l, d), ctx) =>
      assert(ctx.contains(d))  // sanity check
      val (lbl: LabelExpr, ctx1) = rewriteUsingContext(l, ctx)
      val dict = ctx.matVarRef(d).asInstanceOf[MatDictExpr]
      (MatDictLookup(lbl, dict), ctx1)

    case (BagLet(x, TupleDictProject(d), e2), ctx) =>
      assert(ctx.contains(d))  // sanity check
      val newCtx = ctx.children(d).foldLeft (ctx) {
        case (acc, (n, dict)) =>
          val tr = TupleDictVarRef(x.name, x.tp.asInstanceOf[TupleDictType])
          acc.addDictAlias(dict, BagDictProject(tr, n))
      }
      rewriteUsingContext(e2, newCtx)

    case (l: NewLabel, ctx) =>
      val (ps1, ctx1) =
        l.params.foldLeft (Map.empty[String, LabelParameter], ctx) {
          case ((acc, ctx), (_, VarRefLabelParameter(l: LabelExpr)))
            if ctx.contains(l) && ctx.isTopLevel(l) =>
            (acc, ctx)
          case ((acc, ctx), (_, VarRefLabelParameter(d: BagDictExpr))) =>
            assert(ctx.contains(d))  // sanity check
            (acc, ctx)
          case ((acc, ctx), (n, ProjectLabelParameter(d: BagDictExpr))) =>
            assert(ctx.contains(d))  // sanity check
            val ctx1 = ctx.addDictAlias(d, BagDictVarRef(n, d.tp))
            (acc, ctx1)
          case ((acc, ctx), (n, p0)) =>
            val (p1: LabelParameter, ctx1) = rewriteUsingContext(p0, ctx)
            (acc + (n -> p1), ctx1)
        }
      (NewLabel(ps1, l.id), ctx1)

    case (ForeachUnion(x, e1, e2), ctx) =>
      val (r1: BagExpr, ctx1) = rewriteUsingContext(e1, ctx)
      val xd = VarDef(x.name, r1.tp.tp)
      val ctx2 = ctx1.addVarDef(xd)
      val (r2: BagExpr, ctx3) = rewriteUsingContext(e2, ctx2)
      (ForeachUnion(xd, r1, r2), ctx3)

    case (l: Let, ctx) =>
      val (r1, ctx1) = rewriteUsingContext(l.e1, ctx)
      val xd = VarDef(l.x.name, r1.tp)
      val ctx2 = ctx1.addVarDef(xd)
      val (r2, ctx3) = rewriteUsingContext(l.e2, ctx2)
      (Let(xd, r1, r2), ctx3)

    case (x: ExtractLabel, ctx) =>
      val (l: LabelExpr, ctx1) = rewriteUsingContext(x.lbl, ctx)
      val ctx2 = l.tp.attrTps.foldLeft (ctx1) {
        case (acc, (n, t)) => acc.addVarDef(VarDef(n, t))
      }
      val (r, ctx3) = rewriteUsingContext(x.e, ctx2)
      (ExtractLabel(l, r), ctx3)
  })

  def unshred(p: ShredProgram, ctx: Context): Program = {
    Symbol.freshClear()
    Program(p.statements.flatMap(unshred(_, ctx).statements))
  }

  private def unshred(a: ShredAssignment, ctx: Context): Program = a.rhs match {
    case ShredExpr(l: NewLabel, d: BagDict) =>
      assert(l.tp == d.lblTp)   // sanity check
      val (p, b) = unshredBagDict(d, l, ctx, isTopLevel = true)
      Program(p.statements :+ Assignment(a.name, b))
    case _ =>
      sys.error("Unshredding not supported for " + quote(a))
  }

  private def unshredBagDict(dict: BagDict, lbl: LabelExpr, ctx: Context, isTopLevel: Boolean): (Program, BagExpr) =
    dict match {
      case BagDict(tp, ShredUnion(b1, b2), TupleDictUnion(d1, d2)) =>
        val dict1 = BagDict(tp, b1, d1)
        val dict2 = BagDict(tp, b2, d2)
        val (prog1, bag1) = unshredBagDict(dict1, lbl, ctx, isTopLevel)
        val (prog2, bag2) = unshredBagDict(dict2, lbl, ctx, isTopLevel)
        (prog1 ++ prog2, Union(bag1, bag2))

      case BagDict(_, _, tupleDict: TupleDict) if isTopLevel && tupleDict.isEmpty =>
        val bagRef = ctx.matVarRef(dict).asInstanceOf[BagVarRef]
        (Program(), bagRef)

      case BagDict(_, _, tupleDict: TupleDict) if isTopLevel =>
        val bagDictRef = ctx.matVarRef(dict).asInstanceOf[BagVarRef]
        val tupleRef = TupleVarRef(Symbol.fresh(), bagDictRef.tp.tp)

        // 1. Unshred children
        val (childProgram, childTuple) = unshredTupleDict(tupleDict, tupleRef, ctx)

        // 2. Compute nested object
        val nestedBag = ForeachUnion(tupleRef.varDef, bagDictRef, Singleton(childTuple))

        (Program(childProgram.statements), nestedBag)

      case BagDict(_, _, tupleDict: TupleDict) if tupleDict.isEmpty =>
        val matDictRef = ctx.matVarRef(dict).asInstanceOf[MatDictVarRef]
        (Program(), MatDictLookup(lbl, matDictRef))

      case BagDict(_, _, tupleDict: TupleDict) =>
        val matDictRef = ctx.matVarRef(dict).asInstanceOf[MatDictVarRef]
        val kvDict = MatDictToBag(matDictRef)
        val kvRef = TupleVarRef(Symbol.fresh(name = "kv"), kvDict.tp.tp)

        val key = LabelProject(kvRef, KEY_ATTR_NAME)
        val value = BagProject(kvRef, VALUE_ATTR_NAME)
        val tupleRef = TupleVarRef(Symbol.fresh(), value.tp.tp)

        // 1. Unshred children
        val (childProgram, childTuple) = unshredTupleDict(tupleDict, tupleRef, ctx)

        // 2. Compute nested object
        val kvPairsNested =
          BagToMatDict(
            ForeachUnion(kvRef.varDef, kvDict,
              Singleton(Tuple(
                KEY_ATTR_NAME -> key,
                VALUE_ATTR_NAME -> ForeachUnion(tupleRef.varDef, value, Singleton(childTuple)))
              )))

        // 3. Materialize unshredded dictionary
        val uName = matDictRef.name.replace(MAT_DICT_PREFIX, UNSHRED_PREFIX)
        val uDict = MatDictVarRef(uName, kvPairsNested.tp)
        val uStmt = Assignment(uDict.name, kvPairsNested)

        (Program(childProgram.statements :+ uStmt), MatDictLookup(lbl, uDict))

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
