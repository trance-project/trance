package shredding.nrc

import shredding.core.{BagType, LabelType, MatDictType, TupleType, VarDef}

trait MaterializeNRC extends ShredNRC with Optimizer {

  val KEY_ATTR_NAME: String = "_KEY"

  val VALUE_ATTR_NAME: String = "_VALUE"

  val LABEL_ATTR_NAME: String = "_LABEL"

  trait MatDictExpr extends Expr {
    def tp: MatDictType
  }

  final case class MatDictVarRef(name: String, tp: MatDictType) extends MatDictExpr with VarRef

  final case class BagToMatDict(bag: BagExpr) extends MatDictExpr {
    def tp: MatDictType =
      MatDictType(
        bag.tp.tp(KEY_ATTR_NAME).asInstanceOf[LabelType],
        bag.tp.tp(VALUE_ATTR_NAME).asInstanceOf[BagType]
      )
  }

  final case class MatDictToBag(dict: MatDictExpr) extends BagExpr {
    def tp: BagType =
      BagType(TupleType(
        KEY_ATTR_NAME -> dict.tp.keyTp,
        VALUE_ATTR_NAME -> dict.tp.valueTp
      ))
  }

  final case class MatDictLookup(lbl: LabelExpr, dict: MatDictExpr) extends BagExpr {
    assert(dict.tp.keyTp == lbl.tp,
      "Incompatible types " + dict.tp.keyTp + " and " + lbl.tp)

    def tp: BagType = dict.tp.valueTp
  }

}
