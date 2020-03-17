package shredding.nrc

import shredding.utils.Utils.Symbol
import shredding.core._

/**
  * Materialization of nested output queries
  */
trait MaterializerNew {
  this: MaterializeNRC with Optimizer with Printer =>

  val INPUT_DICT_PREFIX: String = "IDict_"

  val MAT_DICT_PREFIX: String = "Mat_"

  val LABEL_DOMAIN_PREFIX: String = "Dom_"

  val ELIMINATE_DOMAINS: Boolean = false

  class DictInfo(val dict: BagDictExpr,
                 val ref: VarRef,
                 val parent: Option[(BagDictExpr, String)])

  class LabelInfo(val isTopLevel: Boolean)

  class Context(private val dicts: Map[BagDictExpr, DictInfo],
                private val labels: Map[LabelExpr, LabelInfo],
                private val scope: Map[String, VarDef]) {

    def this() = this(Map.empty, Map.empty, Map.empty)

    def addDict(dict: BagDictExpr,
                ref: VarRef,
                parent: Option[(BagDictExpr, String)]): Context =
      new Context(dicts + (dict -> new DictInfo(dict, ref, parent)), labels, scope)

    def addDictAlias(dict: BagDictExpr, alias: BagDictExpr): Context =
      new Context(dicts + (alias -> dicts(dict)), labels, scope)

    def contains(d: BagDictExpr): Boolean = dicts.contains(d)

    def isTopLevel(d: BagDictExpr): Boolean = dicts(d).parent.isEmpty

    def matDictRef(d: BagDictExpr): VarRef = dicts(d).ref

    def children(d: BagDictExpr): Map[String, BagDictExpr] = {
      val dict = dicts(d).dict
      dicts.values.collect {
        case i: DictInfo if i.parent.exists(_._1 == dict) =>
          i.parent.get._2 -> i.dict
      }.toMap
    }

    def addLabel(l: LabelExpr, isTopLevel: Boolean): Context =
      new Context(dicts, labels + (l -> new LabelInfo(isTopLevel)), scope)

    def contains(l: LabelExpr): Boolean = labels.contains(l)

    def isTopLevel(l: LabelExpr): Boolean = labels(l).isTopLevel

    def addVarDef(v: VarDef): Context =
      new Context(dicts, labels, scope + (v.name -> v))

    def removeVarDef(v: VarDef): Context =
      new Context(dicts, labels, scope - v.name)

    def contains(n: String): Boolean = scope.contains(n)

    def varDef(n: String): VarDef = scope(n)

    def ++(other: Context): Context =
      new Context(dicts ++ other.dicts, labels ++ other.labels, scope ++ other.scope)

  }

  class MaterializedProgram(val program: Program, val ctx: Context) {
    def ++(m: MaterializedProgram): MaterializedProgram =
      new MaterializedProgram(program ++ m.program, ctx ++ m.ctx)
  }

  def materialize(p: ShredProgram): MaterializedProgram = {
    Symbol.freshClear()

    // Create initial context with top-level dictionaries
    val ctx = inputVars(p).foldLeft (new Context) {
      case (acc, d: BagDictVarRef) =>
        val name = INPUT_DICT_PREFIX + d.name
        val matDict = BagVarRef(VarDef(name, d.tp.flatTp))
        acc.addDict(d, matDict, None)
      case (acc, l: LabelVarRef) =>
        acc.addLabel(l, isTopLevel = true)
      case (acc, _) => acc
    }

    // Materialize each statement starting from empty program
    val emptyProgram = new MaterializedProgram(Program(List.empty), ctx)
    p.statements.foldLeft (emptyProgram) { case (acc, s) =>
      val mat = materialize(s, acc.ctx)
      new MaterializedProgram(acc.program ++ mat.program, mat.ctx)
    }
  }

  def materialize(a: ShredAssignment, ctx: Context): MaterializedProgram = a.rhs match {
    case ShredExpr(l: NewLabel, d: BagDict) =>
      assert(l.tp == d.lblTp)   // sanity check
      materializeBagDict(d, a.name, ctx, None, None)
    case _ =>
      sys.error("Materialization not supported for " + quote(a))
  }

  def materializeBagDict(dict: BagDict,
                         name: String,
                         ctx: Context,
                         parent: Option[(BagDictExpr, String)],
                         labelDomain: Option[BagVarRef]): MaterializedProgram =
    dict match {
      case BagDict(tp, ShredUnion(b1, b2), TupleDictUnion(d1, d2)) =>
        val dict1 = BagDict(tp, b1, d1)
        val dict2 = BagDict(tp, b2, d2)
        materializeBagDict(dict1, name, ctx, parent, labelDomain) ++
          materializeBagDict(dict2, name, ctx, parent, labelDomain)

      case BagDict(lblTp, flat, tupleDict: TupleDict) if parent.isEmpty =>
        // 1. Create dictionary bag expression
        val (bag: BagExpr, ctx2) = rewriteUsingContext(flat, ctx)

        // 2. Create assignment statement
        val suffix = Symbol.fresh(name + "_")
        val bagRef = BagVarRef(VarDef(MAT_DICT_PREFIX + suffix, bag.tp))
        val stmt = Assignment(bagRef.name, bag)

        // 3. Extend context
        val dictCtx =
            ctx2.addDict(dict, bagRef, parent)
              .addDictAlias(dict, BagDictVarRef(VarDef(dictName(name), dict.tp)))
              .addLabel(LabelVarRef(VarDef(flatName(name), lblTp)), parent.isEmpty)

        // 4. Materialize children
        val childPrograms =
          materializeTupleDict(suffix, tupleDict, dict, dictCtx)

        val program = new MaterializedProgram(Program(stmt), dictCtx)
        program ++ childPrograms

      case BagDict(_, flat, tupleDict: TupleDict) if labelDomain.nonEmpty =>
        val domainRef = labelDomain.get

        // 1. Create dictionary bag expression
        val lblDef = VarDef(Symbol.fresh(name = "l"), domainRef.tp.tp)
        val lbl = LabelProject(TupleVarRef(lblDef), LABEL_ATTR_NAME)
        val (valueBag: BagExpr, ctx2) =
          rewriteUsingContext(BagExtractLabel(lbl, flat), ctx)
        val bag =
          ForeachUnion(lblDef, domainRef,
            Singleton(Tuple(KEY_ATTR_NAME -> lbl, VALUE_ATTR_NAME -> valueBag)))

        // 2. Create assignment statement
        val suffix = Symbol.fresh(name + "_")
        val matDict = BagToMatDict(bag)
        val matDictRef = MatDictVarRef(VarDef(MAT_DICT_PREFIX + suffix, matDict.tp))
        val stmt = Assignment(matDictRef.name, matDict)

        // 3. Extend context
        val dictCtx = ctx2.addDict(dict, matDictRef, parent)

        // 4. Materialize children
        val childPrograms =
          materializeTupleDict(suffix, tupleDict, dict, dictCtx)

        val program = new MaterializedProgram(Program(stmt), dictCtx)
        program ++ childPrograms

      case _ =>
        sys.error("Not supported yet")
    }

  def materializeTupleDict(name: String,
                           dict: TupleDict,
                           parentDict: BagDict,
                           ctx: Context): MaterializedProgram = {
    val emptyProgram = new MaterializedProgram(Program(List.empty), ctx)
    dict.fields.foldLeft (emptyProgram) {
      case (acc, (n: String, d: BagDict)) =>
        // 1. Create label domain
        val domain = createLabelDomain(ctx.matDictRef(parentDict), ctx.isTopLevel(parentDict), n)
        val domainRef = BagVarRef(VarDef(domain.name, domain.rhs.tp))

        // 2. Materialize child dictionary
        val childDict =
          materializeBagDict(d, name + "_" + n, ctx, Some(parentDict -> n), Some(domainRef))

        val program = Program(domain :: childDict.program.statements)
        acc ++ new MaterializedProgram(program, childDict.ctx)

      case (acc, _) => acc
    }
  }

  def createLabelDomain(varRef: VarRef, topLevel: Boolean, field: String): Assignment = {
    val domain = if (topLevel) {
      val bagVarRef = varRef.asInstanceOf[BagVarRef]
      val x = VarDef(Symbol.fresh(), bagVarRef.tp.tp)
      val lbl = LabelProject(TupleVarRef(x), field)
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
      val kv = VarDef(Symbol.fresh(name = "kv"), kvTp)
      val values = BagProject(TupleVarRef(kv), VALUE_ATTR_NAME)
      val x = VarDef(Symbol.fresh(), values.tp.tp)
      val lbl = LabelProject(TupleVarRef(x), field)
      DeDup(
        ForeachUnion(kv, MatDictToBag(matDictVarRef),
          ForeachUnion(x, values, Singleton(Tuple(LABEL_ATTR_NAME -> lbl))))
      )
    }

    val name = LABEL_DOMAIN_PREFIX + Symbol.fresh(field)
    val ref = BagVarRef(VarDef(name, domain.tp))
    Assignment(ref.name, domain)
  }

  def rewriteUsingContext(e0: Expr, ctx0: Context): (Expr, Context) = replace[Context](e0, ctx0, {
    case (v: VarRef, ctx) if ctx.contains(v.name) =>
      (VarRef(ctx.varDef(v.name)), ctx)

    case (Lookup(l, d), ctx) if ctx.isTopLevel(d) =>
      assert(ctx.isTopLevel(l))  // sanity check
      (ctx.matDictRef(d).asInstanceOf[BagVarRef], ctx)

    case (Lookup(l, d), ctx) =>
      assert(ctx.contains(d))  // sanity check
      val (lbl: LabelExpr, ctx1) = rewriteUsingContext(l, ctx)
      val dict = ctx.matDictRef(d).asInstanceOf[MatDictExpr]
      (MatDictLookup(lbl, dict), ctx1)
    case (BagLet(x, TupleDictProject(d), e2), ctx) =>
      assert(ctx.contains(d))  // sanity check
      val newCtx = ctx.children(d).foldLeft (ctx) {
        case (acc, (n, dict)) =>
          acc.addDictAlias(dict, BagDictProject(TupleDictVarRef(x), n))
      }
      rewriteUsingContext(e2, newCtx)

    case (l: NewLabel, ctx) =>
      val (ps1, ctx1) =
        l.params.foldLeft (Set.empty[LabelParameter], ctx) {
          case ((acc, ctx), VarRefLabelParameter(l: LabelExpr))
            if ctx.contains(l) && ctx.isTopLevel(l) =>
            (acc, ctx)
          case ((acc, ctx), VarRefLabelParameter(d: BagDictExpr)) =>
            assert(ctx.contains(d))  // sanity check
            (acc, ctx)
          case ((acc, ctx), p @ ProjectLabelParameter(d: BagDictExpr)) =>
            assert(ctx.contains(d))  // sanity check
            val ctx1 = ctx.addDictAlias(d, BagDictVarRef(VarDef(p.name, d.tp)))
            (acc, ctx1)
          case ((acc, ctx), p0) =>
            val (p1: LabelParameter, ctx1) = rewriteUsingContext(p0, ctx)
            (acc + p1, ctx1)
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

//  def unshred(p: ShredProgram, ctx: Context): Program = {
//    Symbol.freshClear()
//    Program(p.statements.flatMap(unshred(_, ctx).statements))
//  }
//
//  def unshred(a: ShredAssignment, ctx: Context): Program = a.rhs match {
//    case ShredExpr(l: NewLabel, d: BagDict) =>
//      assert(l.tp == d.lblTp)   // sanity check
//      unshredBagDict(d)
//    case _ =>
//      sys.error("Unshredding not supported for " + quote(a))
//  }
//
//  def unshredBagDict(dict: BagDict): Program = {
//    null
//  }
}

trait Materializer {
  this: MaterializeNRC with Optimizer with Printer =>

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
      sys.error("Not implemented")
//      MatDictLookup(replaceDictionaries(l, m).asInstanceOf[LabelExpr], m.bagDictMapper(d))
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
      // Sanity check
      assert(a.rhs.flat.isInstanceOf[NewLabel])

      Symbol.freshClear()

      // Construct dummy bag containing a.rhs.flat
      val dummyLabel = a.rhs.flat.asInstanceOf[NewLabel]
      val flat = Assignment(Symbol.fresh(a.name + "__F_"), dummyLabel)
      val flatRef = LabelVarRef(VarDef(flat.name, flat.rhs.tp))

      // Construct initial context
      val dummyBag = Singleton(Tuple(LABEL_ATTR_NAME -> flatRef))
      val initCtx = Assignment(Symbol.fresh(s"${a.name}_ctx"), dummyBag)
      val initCtxRef = BagVarRef(VarDef(initCtx.name, initCtx.rhs.tp))

      // Recur
      val matProgram = materializeDomains(a.name, d, initCtxRef)
      matProgram.prepend(initCtx).prepend(flat)

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
        Singleton(Tuple(KEY_ATTR_NAME -> lbl, VALUE_ATTR_NAME -> b)))

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
        val b = dict.lookup(lbl)
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
