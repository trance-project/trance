package framework.nrc

import framework.common.{BagType, KeyValueMapType, LabelType, TupleType, VarDef}

trait MaterializeNRC extends ShredNRC with Optimizer {

  val KEY_ATTR_NAME: String = "_1"

  val LABEL_ATTR_NAME: String = "_LABEL"

  trait KeyValueMapExpr extends TupleAttributeExpr {
    def tp: KeyValueMapType
  }

  final case class KeyValueMapVarRef(name: String, tp: KeyValueMapType) extends KeyValueMapExpr with VarRef

  final case class KeyValueMapProject(tuple: TupleVarRef, field: String) extends KeyValueMapExpr with TupleProject {
    override def tp: KeyValueMapType = super.tp.asInstanceOf[KeyValueMapType]
  }

  final case class KeyValueMapLet(x: VarDef, e1: Expr, e2: KeyValueMapExpr) extends KeyValueMapExpr with Let {
    assert(x.tp == e1.tp)

    val tp: KeyValueMapType = e2.tp
  }

  final case class KeyValueMapIfThenElse(cond: CondExpr, e1: KeyValueMapExpr, e2: Option[KeyValueMapExpr]) extends KeyValueMapExpr with IfThenElse {
    assert(e2.isEmpty || e1.tp == e2.get.tp)

    val tp: KeyValueMapType = e1.tp
  }

  final case class BagToKeyValueMap(bag: BagExpr) extends KeyValueMapExpr {
    def tp: KeyValueMapType =
      KeyValueMapType(
        bag.tp.tp(KEY_ATTR_NAME).asInstanceOf[LabelType],
        BagType(TupleType(bag.tp.tp.attrTps.filterKeys(_ != KEY_ATTR_NAME)))
      )
  }

  final case class KeyValueMapToBag(dict: KeyValueMapExpr) extends BagExpr {
    def tp: BagType =
      BagType(TupleType(dict.tp.valueTp.tp.attrTps + (KEY_ATTR_NAME -> dict.tp.keyTp)))
  }

  final case class KeyValueMapLookup(lbl: LabelExpr, dict: KeyValueMapExpr) extends BagExpr {
    assert(lbl.tp == dict.tp.keyTp,
      "Incompatible types " + lbl.tp + " and " + dict.tp.keyTp)

    def tp: BagType = dict.tp.valueTp
  }

  trait NamedExpr {
    def name: String

    def e: Expr

    def varRef: Expr = VarRef(name, e.tp)
  }

  sealed trait DictNode

  trait MaterializedDict extends DictNode with NamedExpr

  final case class MBag(name: String, e: BagExpr) extends MaterializedDict {
    override def varRef: BagExpr = BagVarRef(name, e.tp)
  }

  final case class MKeyValueMap(name: String, e: KeyValueMapExpr) extends MaterializedDict {
    override def varRef: KeyValueMapExpr = KeyValueMapVarRef(name, e.tp)
  }

  trait SymbolicDict extends DictNode

  final case class SDict(d: BagDict) extends SymbolicDict

  final case class STuple(fields: Map[String, DictNode]) extends SymbolicDict

  final case class SLet(name: String, e1: Expr, n2: DictNode) extends SymbolicDict

  final case class SIfThenElse(cond: CondExpr, n1: DictNode, n2: DictNode) extends SymbolicDict

}
