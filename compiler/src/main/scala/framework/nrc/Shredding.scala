package framework.nrc

import framework.common._

/**
  * Common shredding methods
  */
trait BaseShredding {

  def flatName(s: String): String = s + "__F"

  def flatTp(tp: Type): Type = tp match {
    case _: PrimitiveType => tp
    case _: BagType => LabelType()
    case TupleType(as) =>
      TupleType(as.map { case (n, t) => n -> flatTp(t).asInstanceOf[TupleAttributeType] })
    case _ => sys.error("Unknown flat type of " + tp)
  }

  def dictName(s: String): String = s + "__D"

  def dictTp(tp: Type): DictType = tp match {
    case _: PrimitiveType => EmptyDictType
    case BagType(t) =>
      BagDictType(
        LabelType(),
        BagType(flatTp(t).asInstanceOf[TupleType]),
        dictTp(t).asInstanceOf[TupleDictType]
      )
    case TupleType(as) =>
      TupleDictType(as.map { case (n, t) => n -> dictTp(t).asInstanceOf[TupleDictAttributeType] })
    case _ => sys.error("Unknown dict type of " + tp)
  }

}

/**
  * Shredding transformation
  */
trait Shredding extends BaseShredding with Extensions with Printer {
  this: MaterializeNRC =>

  protected def shredEnv(vv: Iterable[VarRef]): Map[String, VarDef] =
    vv.flatMap(shredEnv).toMap

  protected def shredEnv(v: VarRef): Map[String, VarDef] =
    shredEnv(v.name, v.tp)

  protected def shredEnv(n: String, tp: Type): Map[String, VarDef] =
    Map(
      flatName(n) -> VarDef(flatName(n), flatTp(tp)),
      dictName(n) -> VarDef(dictName(n), dictTp(tp))
    )

  def shred(e: Expr): SExpr =
    shred(shredEnv(inputVars(e)), Map.empty, e)

  protected def shred(env: Map[String, VarDef],
                      tpResolver: Map[String, (Type, DictType)],
                      e: Expr): SExpr = e match {

    case _: Const => ShredExpr(e, EmptyDict)

    /* tpResolver ensures correct type when dealing with labels
     * because flatTp always returns empty LabelType()
     */
    case v: VarRef if tpResolver.contains(v.name) =>
      ShredExpr(
        VarRef(flatName(v.name), tpResolver(v.name)._1),
        DictVarRef(dictName(v.name), tpResolver(v.name)._2)
      )

    case v: VarRef =>
      ShredExpr(
        VarRef(flatName(v.name), flatTp(v.tp)),
        DictVarRef(dictName(v.name), dictTp(v.tp))
      )

    case p: Project =>
      val ShredExpr(flat: TupleExpr, dict: TupleDictExpr) = shred(env, tpResolver, p.tuple)
      ShredExpr(Project(flat, p.field), dict(p.field))

    case ForeachUnion(x, e1, e2) =>
      // Shredding e1
      val ShredExpr(l1: LabelExpr, dict1: BagDictExpr) = shred(env, tpResolver, e1)
      val resolved1 = dict1.lookup(l1)
      // Create x^F and x^D
      val xFlat = VarDef(flatName(x.name), resolved1.tp.tp)
      val xDict = VarDef(dictName(x.name), dict1.tp.dictTp)
      // Shredding e2
      val tpResolverEx = tpResolver + (x.name -> (xFlat.tp, xDict.tp.asInstanceOf[DictType]))
      val ShredExpr(l2: LabelExpr, dict2: BagDictExpr) =
        shred(env + (xDict.name -> xDict), tpResolverEx, e2)
      val resolved2 = dict2.lookup(l2)
      // Output flat bag
      val flat =
        BagLet(xDict, dict1.tupleDict,
          ForeachUnion(xFlat, resolved1, resolved2))
      val tupleDict =
        TupleDictLet(xDict, dict1.tupleDict, dict2.tupleDict)
      val lbl = NewLabel(labelParameters(flat, env).toSet)

      ShredExpr(lbl, BagDict(lbl.tp, createLambda(lbl, flat), tupleDict))

    case Union(e1, e2) =>
      val ShredExpr(l1: LabelExpr, dict1: BagDictExpr) = shred(env, tpResolver, e1)
      val ShredExpr(l2: LabelExpr, dict2: BagDictExpr) = shred(env, tpResolver, e2)
      val flat = ShredUnion(dict1.lookup(l1), dict2.lookup(l2))
      val dictUnion = TupleDictUnion(dict1.tupleDict, dict2.tupleDict)
      val lbl = NewLabel(labelParameters(flat, env).toSet)
      ShredExpr(lbl, BagDict(lbl.tp, createLambda(lbl, flat), dictUnion))

    case Singleton(e1) =>
      val ShredExpr(t: TupleExpr, dict: TupleDictExpr) = shred(env, tpResolver, e1)
      val flat = Singleton(t)
      val lbl = NewLabel(labelParameters(flat, env).toSet)
      ShredExpr(lbl, BagDict(lbl.tp, createLambda(lbl, flat), dict))

    case DeDup(e1) =>
      val ShredExpr(lbl1: LabelExpr, dict1: BagDictExpr) = shred(env, tpResolver, e1)
      val flat = DeDup(dict1.lookup(lbl1))
      val lbl = NewLabel(labelParameters(flat, env).toSet)
      ShredExpr(lbl, BagDict(lbl.tp, createLambda(lbl, flat), dict1.tupleDict))

    case Get(e1) =>
      val ShredExpr(flat: TupleExpr, dict: TupleDictExpr) = shred(env, tpResolver, e1)
      ShredExpr(Get(Singleton(flat)), dict)

    case Tuple(fs) =>
      val shredFs = fs.map(f => f._1 -> shred(env, tpResolver, f._2))
      val flatFs = shredFs.map(f => f._1 -> f._2.flat.asInstanceOf[TupleAttributeExpr])
      val dictFs = shredFs.map(f => f._1 -> f._2.dict.asInstanceOf[TupleDictAttributeExpr])
      ShredExpr(Tuple(flatFs), TupleDict(dictFs))

    case l: Let =>
      val se1 = shred(env, tpResolver, l.e1)
      val xFlat = VarDef(flatName(l.x.name), se1.flat.tp)
      val xDict = VarDef(dictName(l.x.name), se1.dict.tp)
      val tpResolverEx = tpResolver + (l.x.name -> (xFlat.tp, xDict.tp.asInstanceOf[DictType]))
      val se2 = shred(env ++ Map(xDict.name -> xDict, xFlat.name -> xFlat), tpResolverEx, l.e2)
      val flat = Let(xDict, se1.dict, Let(xFlat, se1.flat, se2.flat))
      val dict = DictLet(xDict, se1.dict, DictLet(xFlat, se1.flat, se2.dict))
      ShredExpr(flat, dict)

    case c: Cmp =>
      val ShredExpr(c1: PrimitiveExpr, EmptyDict) = shred(env, tpResolver, c.e1)
      val ShredExpr(c2: PrimitiveExpr, EmptyDict) = shred(env, tpResolver, c.e2)
      ShredExpr(Cmp(c.op, c1, c2), EmptyDict)

    case And(e1, e2) =>
      val ShredExpr(c1: CondExpr, EmptyDict) = shred(env, tpResolver, e1)
      val ShredExpr(c2: CondExpr, EmptyDict) = shred(env, tpResolver, e2)
      ShredExpr(And(c1, c2), EmptyDict)

    case Or(e1, e2) =>
      val ShredExpr(c1: CondExpr, EmptyDict) = shred(env, tpResolver, e1)
      val ShredExpr(c2: CondExpr, EmptyDict) = shred(env, tpResolver, e2)
      ShredExpr(Or(c1, c2), EmptyDict)

    case Not(e1) =>
      val ShredExpr(c1: CondExpr, EmptyDict) = shred(env, tpResolver, e1)
      ShredExpr(Not(c1), EmptyDict)

    case i: IfThenElse =>
      val ShredExpr(c: CondExpr, EmptyDict) = shred(env, tpResolver, i.cond)
      val se1 = shred(env, tpResolver, i.e1)
      if (i.e2.isDefined) {
        val se2 = shred(env, tpResolver, i.e2.get)
        ShredExpr(IfThenElse(c, se1.flat, se2.flat), DictIfThenElse(c, se1.dict, se2.dict))
      }
      else
        ShredExpr(IfThenElse(c, se1.flat), se1.dict)

    case a: ArithmeticExpr =>
      val ShredExpr(n1: NumericExpr, EmptyDict) = shred(env, tpResolver, a.e1)
      val ShredExpr(n2: NumericExpr, EmptyDict) = shred(env, tpResolver, a.e2)
      ShredExpr(ArithmeticExpr(a.op, n1, n2), EmptyDict)

    case Count(e1) =>
      val ShredExpr(lbl: LabelExpr, dict1: BagDictExpr) = shred(env, tpResolver, e1)
      ShredExpr(Count(dict1.lookup(lbl)), EmptyDict)

    case Sum(e1, fs) =>
      val ShredExpr(lbl: LabelExpr, dict1: BagDictExpr) = shred(env, tpResolver, e1)
      ShredExpr(Sum(dict1.lookup(lbl), fs), EmptyDict)

    case GroupByKey(e1, ks, vs, n) =>
      val ShredExpr(lbl1: LabelExpr, dict1: BagDictExpr) = shred(env, tpResolver, e1)
      val flat = GroupByKey(dict1.lookup(lbl1), ks, vs, n)
      val lbl = NewLabel(labelParameters(flat, env).toSet)
      ShredExpr(lbl, BagDict(lbl.tp, createLambda(lbl, flat), dict1.tupleDict))

    case ReduceByKey(e1, ks, vs) =>
      val ShredExpr(lbl1: LabelExpr, dict1: BagDictExpr) = shred(env, tpResolver, e1)
      val flat = ReduceByKey(dict1.lookup(lbl1), ks, vs)
      val lbl = NewLabel(labelParameters(flat, env).toSet)
      ShredExpr(lbl, BagDict(lbl.tp, createLambda(lbl, flat), dict1.tupleDict))
      
    case u:Udf => (u.in.tp, u.tp) match {

      // ex. take a nested bag, return the identity
      // materialization restricts input to be a variable
      case (_:BagType, _) => 
         val ShredExpr(lbl: Expr, dict:DictExpr) = shred(env, tpResolver, u.in)
         ShredUdf(u.name, lbl, dict, u.otp)

      // ex. take a string, return something appended to the string
      case (_:PrimitiveType, _:PrimitiveType) => 
        val sresult:SExpr = shred(env, tpResolver, u.in)
        val flat = sresult.flat
        val dict = sresult.dict
        ShredExpr(Udf(u.name, flat, u.otp), dict)

      case _ => ???
    }

    case _ => sys.error("Cannot shred expr " + e)
  }

  def shred(a: Assignment): ShredAssignment =
    shred(shredEnv(inputVars(a)), Map.empty, a)

  protected def shred(env: Map[String, VarDef], tpResolver: Map[String, (Type, DictType)], a: Assignment): ShredAssignment = {
    ShredAssignment(a.name, shred(env, tpResolver, a.rhs))
  }

  def shred(p: Program): ShredProgram =
    shred(p, Map.empty)

  protected def shred(p: Program, tpResolver: Map[String, (Type, DictType)]): ShredProgram =
    shredCtx(p, tpResolver)._1

  def shredCtx(p: Program): (ShredProgram, Map[String, (Type, DictType)]) =
    shredCtx(p, Map.empty)

  def shredCtx(p: Program, tpResolver: Map[String, (Type, DictType)]): (ShredProgram, Map[String, (Type, DictType)]) = {
    val env0 = inputVars(p).flatMap { v =>
      val (ftp, dtp) = tpResolver.getOrElse(v.name, flatTp(v.tp) -> dictTp(v.tp))
      Map(
        flatName(v.name) -> VarDef(flatName(v.name), ftp),
        dictName(v.name) -> VarDef(dictName(v.name), dtp)
      )
    }.toMap
    p.statements.foldLeft(env0, (ShredProgram(), tpResolver)) {
      case ((env, (acc, ctx)), stmt) =>
        val sa = shred(env, ctx, stmt)
        (env +
          (flatName(stmt.name) -> VarDef(flatName(stmt.name), sa.rhs.flat.tp)) +
          (dictName(stmt.name) -> VarDef(dictName(stmt.name), sa.rhs.dict.tp)),
          (ShredProgram(acc.statements :+ sa), ctx + (sa.name -> (sa.rhs.flat.tp, sa.rhs.dict.tp))))
    }._2
  }

}
