package shredding.nrc

import shredding.core._

/**
  * Dictionary extensions
  */
trait Dictionary {
  this: ShredNRC =>

  sealed trait DictExpr extends Expr {
    def tp: DictType
  }

  sealed trait TupleDictAttributeExpr extends DictExpr {
    def tp: TupleDictAttributeType
  }

  case object EmptyDict extends TupleDictAttributeExpr {
    def tp: TupleDictAttributeType = EmptyDictType
  }

  sealed trait BagDictExpr extends TupleDictAttributeExpr {
    def tp: BagDictType
  }

  sealed trait TupleDictExpr extends DictExpr {
    def tp: TupleDictType
  }

  case object DictVarRef {
    def apply(varDef: VarDef): DictExpr = varDef.tp match {
      case EmptyDictType => EmptyDict
      case _: BagDictType => BagDictVarRef(varDef)
      case _: TupleDictType => TupleDictVarRef(varDef)
      case t => sys.error("Cannot create DictVarRef for type " + t)
    }

    def apply(n: String, tp: Type): DictExpr = apply(VarDef(n, tp))
  }

  case class BagDictVarRef(varDef: VarDef) extends BagDictExpr with VarRef {
    override def tp: BagDictType = super.tp.asInstanceOf[BagDictType]
  }

  case class TupleDictVarRef(varDef: VarDef) extends TupleDictExpr with VarRef {
    override def tp: TupleDictType = super.tp.asInstanceOf[TupleDictType]
  }

  case class BagDict(lbl: LabelExpr, flat: BagExpr, dict: TupleDictExpr) extends BagDictExpr {
    val tp: BagDictType = BagDictType(flat.tp, dict.tp)
  }

  case class TupleDict(fields: Map[String, TupleDictAttributeExpr]) extends TupleDictExpr {
    val tp: TupleDictType = TupleDictType(fields.map(f => f._1 -> f._2.tp))
  }

  implicit class TupleDictExprOps(d: TupleDictExpr) {
    def apply(field: String): TupleDictAttributeExpr = d match {
      case TupleDict(fs) => fs(field)
      case TupleDictLet(x, e1, TupleDict(fs)) =>
        DictLet(x, e1, fs(field)).asInstanceOf[TupleDictAttributeExpr]
      case _ => d.tp(field) match {
        case EmptyDictType => EmptyDict
        case _: BagDictType => BagDictProject(d, field)
      }
    }
  }

  case class BagDictProject(dict: TupleDictExpr, field: String) extends BagDictExpr {
    val tp: BagDictType = dict.tp(field).asInstanceOf[BagDictType]
  }

  implicit class BagDictExprOps(d: BagDictExpr) {
    def tupleDict: TupleDictExpr = d match {
      case b: BagDict => b.dict
      case _ => TupleDictProject(d)
    }
  }

  case class TupleDictProject(dict: BagDictExpr) extends TupleDictExpr {
    val tp: TupleDictType = dict.tp.dictTp
  }

  case object DictLet {
    def apply(x: VarDef, e1: Expr, e2: DictExpr): DictExpr = e2.tp match {
      case EmptyDictType => EmptyDict
      case _: BagDictType => BagDictLet(x, e1, e2.asInstanceOf[BagDictExpr])
      case _: TupleDictType => TupleDictLet(x, e1, e2.asInstanceOf[TupleDictExpr])
      case t => sys.error("Cannot create DictLet for type " + t)
    }
  }

  case class BagDictLet(x: VarDef, e1: Expr, e2: BagDictExpr) extends BagDictExpr with Let {
    assert(x.tp == e1.tp)

    val tp: BagDictType = e2.tp
  }

  case class TupleDictLet(x: VarDef, e1: Expr, e2: TupleDictExpr) extends TupleDictExpr with Let {
    assert(x.tp == e1.tp)

    val tp: TupleDictType = e2.tp
  }

  case object DictIfThenElse {
    def apply(cond: Cond, e1: DictExpr, e2: DictExpr): DictExpr = e1.tp match {
      case EmptyDictType =>
        EmptyDict
      case _: TupleDictType =>
        TupleDictIfThenElse(cond, e1.asInstanceOf[TupleDictExpr], e2.asInstanceOf[TupleDictExpr])
      case _: BagDictType =>
        BagDictIfThenElse(cond, e1.asInstanceOf[BagDictExpr], e2.asInstanceOf[BagDictExpr])
    }
  }

  case class TupleDictIfThenElse(cond: Cond, e1: TupleDictExpr, d2: TupleDictExpr) extends TupleDictExpr with IfThenElse {
    assert(e1.tp == d2.tp)

    val tp: TupleDictType = e1.tp

    def e2: Option[TupleDictExpr] = Some(d2)
  }

  case class BagDictIfThenElse(cond: Cond, e1: BagDictExpr, d2: BagDictExpr) extends BagDictExpr with IfThenElse {
    assert(e1.tp == d2.tp)

    val tp: BagDictType = e1.tp

    def e2: Option[BagDictExpr] = Some(d2)
  }


  implicit class DictExprOps(dict1: DictExpr) {
    def union(dict2: DictExpr): DictExpr = (dict1, dict2) match {
      case (EmptyDict, EmptyDict) =>
        EmptyDict
      case (BagDict(l1, f1, d1), BagDict(l2, f2, d2)) =>
        val attrTps = l1.tp.attrTps ++ l2.tp.attrTps
        val vars = attrTps.map { case (n, t) => ShredVarRef(VarDef(n, t)).asInstanceOf[VarRef] }.toSet
        val lbl = NewLabel(vars.map(VarRefLabelParameter(_)))
        BagDict(lbl, Union(f1, f2), d1.union(d2).asInstanceOf[TupleDictExpr])
      case (d1: BagDictExpr, d2: BagDictExpr) =>
        DictUnion(d1, d2)
      case (TupleDict(fields1), TupleDict(fields2)) =>
        assert(fields1.keySet == fields2.keySet)
        TupleDict(fields1.map { case (k1, d1) =>
          k1 -> d1.union(fields2(k1)).asInstanceOf[TupleDictAttributeExpr]
        })
      case (d1: TupleDictExpr, d2: TupleDictExpr) =>
        DictUnion(d1, d2)
      case _ => sys.error("Illegal dictionary union " + dict1 + " and " + dict2)
    }
  }

  case class DictUnion(dict1: DictExpr, dict2: DictExpr) extends DictExpr {
    assert(dict1.tp == dict2.tp)

    val tp: DictType = dict1.tp
  }

}
