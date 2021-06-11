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
  }

  trait MaterializedExpr extends NamedExpr

  type MExpr = MaterializedExpr

  final case class MBag(name: String, e: BagExpr) extends MExpr {
    def varRef: BagVarRef = BagVarRef(name, e.tp)
  }

  final case class MKeyValueMap(name: String, e: KeyValueMapExpr) extends MExpr {
    def varRef: KeyValueMapVarRef = KeyValueMapVarRef(name, e.tp)
  }

  final case class MTuple(fields: Map[String, MExpr]) extends MExpr {
    def name: String = "MTuple"

    def e: Tuple = Tuple(fields.map(x => x._1 -> x._2.e.asInstanceOf[TupleAttributeExpr]))

    def apply(n: String): MExpr = fields(n)
  }

  final case class MLet(n: String, e1: Expr, m2: MExpr) extends MExpr {
    def name: String = "MLet"

    def e: Expr = Let(n, e1, m2.e)
  }

  final case class MIfThenElse(c: CondExpr, m1: MExpr, m2: MExpr) extends MExpr {
    def name: String = "MIfThenElse"

    def e: Expr = IfThenElse(c, m1.e, m2.e)
  }

}
