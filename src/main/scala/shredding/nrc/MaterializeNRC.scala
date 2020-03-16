package shredding.nrc

import shredding.core.BagType

trait MaterializeNRC extends ShredNRC with Optimizer {

  val KEY_ATTR_NAME: String = "_KEY"

  val VALUE_ATTR_NAME: String = "_VALUE"

  val LABEL_ATTR_NAME: String = "_LABEL"

  final case class MatDictLookup(lbl: LabelExpr, bag: BagExpr) extends BagExpr {
    assert(bag.tp.tp(KEY_ATTR_NAME) == lbl.tp,
      "Incompatible types " + bag.tp.tp(KEY_ATTR_NAME) + " and " + lbl.tp)

    assert(bag.tp.tp(VALUE_ATTR_NAME).isInstanceOf[BagType],
      "Value attribute has wrong type: " + bag.tp.tp(VALUE_ATTR_NAME))

    def tp: BagType = bag.tp.tp(VALUE_ATTR_NAME).asInstanceOf[BagType]
  }

}
