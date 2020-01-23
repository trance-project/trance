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

  trait TupleDictAttributeExpr extends DictExpr {
    def tp: TupleDictAttributeType
  }

  case object EmptyDict extends TupleDictAttributeExpr {
    def tp: TupleDictAttributeType = EmptyDictType
  }

  trait BagDictExpr extends TupleDictAttributeExpr {
    def tp: BagDictType
  }

  trait TupleDictExpr extends DictExpr {
    def tp: TupleDictType
  }

  final case class BagDictVarRef(varDef: VarDef) extends BagDictExpr with VarRef {
    override def tp: BagDictType = super.tp.asInstanceOf[BagDictType]
  }

  final case class TupleDictVarRef(varDef: VarDef) extends TupleDictExpr with VarRef {
    override def tp: TupleDictType = super.tp.asInstanceOf[TupleDictType]
  }

  final case class BagDict(lbl: LabelExpr, flat: BagExpr, dict: TupleDictExpr) extends BagDictExpr {
    val tp: BagDictType = BagDictType(flat.tp, dict.tp)
  }

  final case class TupleDict(fields: Map[String, TupleDictAttributeExpr]) extends TupleDictExpr {
    val tp: TupleDictType = TupleDictType(fields.map(f => f._1 -> f._2.tp))
  }

  final case class BagDictProject(dict: TupleDictExpr, field: String) extends BagDictExpr {
    val tp: BagDictType = dict.tp(field).asInstanceOf[BagDictType]
  }

  final case class TupleDictProject(dict: BagDictExpr) extends TupleDictExpr {
    val tp: TupleDictType = dict.tp.dictTp
  }

  final case class BagDictLet(x: VarDef, e1: Expr, e2: BagDictExpr) extends BagDictExpr with Let {
    assert(x.tp == e1.tp)

    val tp: BagDictType = e2.tp
  }

  final case class TupleDictLet(x: VarDef, e1: Expr, e2: TupleDictExpr) extends TupleDictExpr with Let {
    assert(x.tp == e1.tp)

    val tp: TupleDictType = e2.tp
  }

  final case class BagDictIfThenElse(cond: CondExpr, e1: BagDictExpr, d2: BagDictExpr) extends BagDictExpr with IfThenElse {
    assert(e1.tp == d2.tp)

    val tp: BagDictType = e1.tp

    def e2: Option[BagDictExpr] = Some(d2)
  }

  final case class TupleDictIfThenElse(cond: CondExpr, e1: TupleDictExpr, d2: TupleDictExpr) extends TupleDictExpr with IfThenElse {
    assert(e1.tp == d2.tp)

    val tp: TupleDictType = e1.tp

    def e2: Option[TupleDictExpr] = Some(d2)
  }

  sealed trait DictUnion {
    def dict1: DictExpr

    def dict2: DictExpr
  }

  case object EmptyDictUnion extends TupleDictAttributeExpr with DictUnion {
    def dict1: DictExpr = EmptyDict

    def dict2: DictExpr = EmptyDict

    def tp: TupleDictAttributeType = EmptyDictType
  }

  final case class BagDictUnion(dict1: BagDictExpr, dict2: BagDictExpr) extends BagDictExpr with DictUnion {
    assert(dict1.tp == dict2.tp)

    val tp: BagDictType = dict1.tp
  }

  final case class TupleDictUnion(dict1: TupleDictExpr, dict2: TupleDictExpr) extends TupleDictExpr with DictUnion {
    assert(dict1.tp == dict2.tp)

    val tp: TupleDictType = dict1.tp
  }

}
