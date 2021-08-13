package framework.nrc

import framework.common._

/**
  * Dictionary extensions
  */
trait Dictionary {
  this: NRC =>

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

  trait TupleDictExpr extends DictExpr with AbstractTuple {
    def tp: TupleDictType
  }

  trait DictVarRef extends VarRef {
    this: DictExpr =>
  }

  final case class BagDictVarRef(name: String, tp: BagDictType) extends BagDictExpr with DictVarRef

  final case class TupleDictVarRef(name: String, tp: TupleDictType) extends TupleDictExpr with DictVarRef

  final case class BagDict(lblTp: LabelType, flat: BagExpr, dict: TupleDictExpr) extends BagDictExpr {
    val tp: BagDictType = BagDictType(lblTp, flat.tp, dict.tp)
  }

  final case class TupleDict(fields: Map[String, TupleDictAttributeExpr]) extends TupleDictExpr {
    val tp: TupleDictType = TupleDictType(fields.map(f => f._1 -> f._2.tp))
  }

  trait DictProject extends Project { this: DictExpr => }

  final case class BagDictProject(tuple: TupleDictVarRef, field: String) extends BagDictExpr with DictProject {
    override def tp: BagDictType = tuple.tp(field).asInstanceOf[BagDictType]
  }

  final case class TupleDictProject(dict: BagDictExpr) extends TupleDictExpr {
    val tp: TupleDictType = dict.tp.dictTp
  }

  trait DictLet extends Let {
    this: DictExpr =>

    def e2: DictExpr
  }

  final case class BagDictLet(x: VarDef, e1: Expr, e2: BagDictExpr) extends BagDictExpr with DictLet {
    assert(x.tp == e1.tp)

    val tp: BagDictType = e2.tp
  }

  final case class TupleDictLet(x: VarDef, e1: Expr, e2: TupleDictExpr) extends TupleDictExpr with DictLet {
    assert(x.tp == e1.tp)

    val tp: TupleDictType = e2.tp
  }

  trait DictIfThenElse extends IfThenElse {
    this: DictExpr =>

    def e1: DictExpr

    def e2: Option[DictExpr]
  }

  final case class BagDictIfThenElse(cond: CondExpr, e1: BagDictExpr, d2: BagDictExpr) extends BagDictExpr with DictIfThenElse {
    assert(e1.tp == d2.tp)

    val tp: BagDictType = e1.tp

    def e2: Option[BagDictExpr] = Some(d2)
  }

  final case class TupleDictIfThenElse(cond: CondExpr, e1: TupleDictExpr, d2: TupleDictExpr) extends TupleDictExpr with DictIfThenElse {
    assert(e1.tp == d2.tp)

    val tp: TupleDictType = e1.tp

    def e2: Option[TupleDictExpr] = Some(d2)
  }

  sealed trait DictUnion {
    this: DictExpr =>

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
