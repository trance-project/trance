package shredding.nrc

import shredding.core._

trait Factory {
  this: MaterializeNRC with Implicits =>

  object Const {
    def apply(v: Any, tp: PrimitiveType): PrimitiveExpr = tp match {
      case _: NumericType => NumericConst(v.asInstanceOf[AnyVal], tp.asInstanceOf[NumericType])
      case _ => PrimitiveConst(v, tp)
    }
  }

  object VarRef {
    def apply(varDef: VarDef): Expr = varDef.tp match {
      case _: NumericType => NumericVarRef(varDef)
      case _: PrimitiveType => PrimitiveVarRef(varDef)
      case _: BagType => BagVarRef(varDef)
      case _: TupleType => TupleVarRef(varDef)
      case _: LabelType => LabelVarRef(varDef)
      case _: DictType => DictVarRef(varDef)
      case _: MatDictType => MatDictVarRef(varDef)
      case t => sys.error("Cannot create VarRef for type " + t)
    }

    def apply(n: String, tp: BagType): BagVarRef = BagVarRef(VarDef(n, tp))

    def apply(n: String, tp: TupleType): TupleVarRef = TupleVarRef(VarDef(n, tp))

    def apply(n: String, tp: LabelType): LabelVarRef = LabelVarRef(VarDef(n, tp))

    def apply(n: String, tp: MatDictType): MatDictVarRef = MatDictVarRef(VarDef(n, tp))

    def apply(n: String, tp: Type): Expr = apply(VarDef(n, tp))
  }

  object DictVarRef {
    def apply(varDef: VarDef): DictExpr = varDef.tp match {
      case EmptyDictType => EmptyDict
      case _: BagDictType => BagDictVarRef(varDef)
      case _: TupleDictType => TupleDictVarRef(varDef)
      case t => sys.error("Cannot create DictVarRef for type " + t)
    }

    def apply(n: String, tp: Type): DictExpr = apply(VarDef(n, tp))
  }

  object Project {
    def apply(t: AbstractTuple, field: String): Expr = t match {
      case e: TupleExpr => apply(e, field)
      case e: TupleDictExpr => apply(e, field)
      case _ => sys.error("Cannot create Project for tuple " + t)
    }

    def apply(t: TupleExpr, field: String): TupleAttributeExpr = t match {
      case Tuple(fs) =>
        fs(field)
      case TupleLet(x, e1, e2) =>
        Let(x, e1, e2(field)).asInstanceOf[TupleAttributeExpr]
      case TupleIfThenElse(c, e1, e2) =>
        IfThenElse(c, e1(field), e2(field)).asInstanceOf[TupleAttributeExpr]
      case v: TupleVarRef => v.tp(field) match {
        case _: NumericType => NumericProject(v, field)
        case _: PrimitiveType => PrimitiveProject(v, field)
        case _: BagType => BagProject(v, field)
        case _: LabelType => LabelProject(v, field)
        case tp => sys.error("Cannot create Project for tuple type " + tp)
      }
    }

    def apply(t: TupleDictExpr, field: String): TupleDictAttributeExpr = t match {
      case TupleDict(fs) =>
        fs(field)
      case TupleDictLet(x, e1, e2) =>
        DictLet(x, e1, e2(field).asInstanceOf[DictExpr]).asInstanceOf[TupleDictAttributeExpr]
      case TupleDictIfThenElse(c, e1, e2) =>
        DictIfThenElse(c, e1(field), e2(field)).asInstanceOf[TupleDictAttributeExpr]
      case TupleDictUnion(d1, d2) =>
        DictUnion(d1(field), d2(field)).asInstanceOf[TupleDictAttributeExpr]
      case v: TupleDictVarRef => v.tp(field) match {
        case EmptyDictType => EmptyDict
        case _: BagDictType => BagDictProject(v, field)
        case tp => sys.error("Cannot create Project for dictionary type " + tp)
      }
    }
  }

  object Let {
    def apply(x: VarDef, e1: Expr, e2: Expr): Expr = e2 match {
      case b: NumericExpr => NumericLet(x, e1, b)
      case b: PrimitiveExpr => PrimitiveLet(x, e1, b)
      case b: BagExpr => BagLet(x, e1, b)
      case b: TupleExpr => TupleLet(x, e1, b)
      case b: LabelExpr => LabelLet(x, e1, b)
      case b: DictExpr => DictLet(x, e1, b)
      case _ => sys.error("Cannot create Let for type " + e2.tp)
    }

    def apply(x: VarDef, e1: Expr, e2: DictExpr): DictExpr = DictLet(x, e1, e2)
  }

  object DictLet {
    def apply(x: VarDef, e1: Expr, e2: DictExpr): DictExpr = e2 match {
      case EmptyDict => EmptyDict
      case b: BagDictExpr => BagDictLet(x, e1, b)
      case b: TupleDictExpr => TupleDictLet(x, e1, b)
      case _ => sys.error("Cannot create DictLet for type " + e2.tp)
    }
  }

  object Cmp {
    def apply(op: OpCmp, e1: Expr, e2: Expr): CondExpr = (e1, e2) match {
      case (p1: PrimitiveExpr, p2: PrimitiveExpr) => PrimitiveCmp(op, p1, p2)
      case _ => sys.error("Cannot create Cmp for types " + e1.tp + " and " + e2.tp)
    }
  }

  object IfThenElse {
    def apply(c: CondExpr, e1: Expr, e2: Expr): Expr = (e1, e2) match {
      case (a: NumericExpr, b: NumericExpr) => NumericIfThenElse(c, a, b)
      case (a: PrimitiveExpr, b: PrimitiveExpr) => PrimitiveIfThenElse(c, a, b)
      case (a: BagExpr, b: BagExpr) => BagIfThenElse(c, a, Some(b))
      case (a: TupleExpr, b: TupleExpr) => TupleIfThenElse(c, a, b)
      case (a: LabelExpr, b: LabelExpr) => LabelIfThenElse(c, a, Some(b))
      case (a: DictExpr, b: DictExpr) => DictIfThenElse(c, a, b)
      case _ => sys.error("Cannot create IfThenElse for types " + e1.tp + " and " + e2.tp)
    }

    def apply(c: CondExpr, e: Expr): Expr = e match {
      case a: BagExpr => IfThenElse(c, a)
      case a: LabelExpr => IfThenElse(c, a)
      case _ => sys.error("Cannot create IfThen for type " + e.tp)
    }

    def apply(c: CondExpr, e: BagExpr): BagIfThenElse =
      BagIfThenElse(c, e, None)

    def apply(c: CondExpr, e: LabelExpr): LabelIfThenElse =
      LabelIfThenElse(c, e, None)
  }

  object DictIfThenElse {
    def apply(cond: CondExpr, e1: DictExpr, e2: DictExpr): DictExpr = (e1, e2) match {
      case (EmptyDict, EmptyDict) => EmptyDict
      case (a: BagDictExpr, b: BagDictExpr) => BagDictIfThenElse(cond, a, b)
      case (a: TupleDictExpr, b: TupleDictExpr) => TupleDictIfThenElse(cond, a, b)
      case _ => sys.error("Cannot create IfThenElse for types " + e1.tp + " and " + e2.tp)
    }
  }

  object ExtractLabel {
    def apply(lbl: LabelExpr, e: Expr): Expr = e match {
      case a: NumericExpr => NumericExtractLabel(lbl, a)
      case a: PrimitiveExpr => PrimitiveExtractLabel(lbl, a)
      case a: BagExpr => BagExtractLabel(lbl, a)
      case a: TupleExpr => TupleExtractLabel(lbl, a)
      case a: LabelExpr => LabelExtractLabel(lbl, a)
      case _ => sys.error("Cannot create ExtractLabel for type " + e.tp)
    }
  }

  object DictUnion {
    def apply(d1: DictExpr, d2: DictExpr): DictExpr = (d1, d2) match {
      case (EmptyDict, EmptyDict) => EmptyDictUnion
      case (a: BagDictExpr, b: BagDictExpr) => BagDictUnion(a, b)
      case (a: TupleDictExpr, b: TupleDictExpr) => TupleDictUnion(a, b)
      case _ => sys.error("Cannot create DictUnion for types " + d1.tp + " and " + d2.tp)
    }
  }

}
