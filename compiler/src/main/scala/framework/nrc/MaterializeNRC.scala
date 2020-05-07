package framework.nrc

import framework.common.{BagType, LabelType, MatDictType, TupleType}

trait MaterializeNRC extends ShredNRC with Optimizer {

  val KEY_ATTR_NAME: String = "_1"

  val LABEL_ATTR_NAME: String = "_LABEL"

  trait MatDictExpr extends Expr {
    def tp: MatDictType
  }

  final case class MatDictVarRef(name: String, tp: MatDictType) extends MatDictExpr with VarRef

  final case class BagToMatDict(bag: BagExpr) extends MatDictExpr {
    def tp: MatDictType =
      MatDictType(
        bag.tp.tp(KEY_ATTR_NAME).asInstanceOf[LabelType],
        BagType(TupleType(bag.tp.tp.attrTps.filterKeys(_ != KEY_ATTR_NAME)))
      )
  }

  final case class MatDictToBag(dict: MatDictExpr) extends BagExpr {
    def tp: BagType =
      BagType(TupleType(dict.tp.valueTp.tp.attrTps + (KEY_ATTR_NAME -> dict.tp.keyTp)))
  }

  final case class MatDictLookup(lbl: LabelExpr, dict: MatDictExpr) extends BagExpr {
    assert(lbl.tp == dict.tp.keyTp,
      "Incompatible types " + lbl.tp + " and " + dict.tp.keyTp)

    def tp: BagType = dict.tp.valueTp
  }

}
