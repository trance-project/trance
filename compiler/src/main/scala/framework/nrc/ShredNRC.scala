package framework.nrc

import framework.common._

/**
  * Shredding NRC extension
  */
trait ShredNRC extends NRC with BaseShredding with Label with Dictionary {

  /**
    * Shredded expression E is a pair of E^flat and E^dict
    */

  final case class ShredExpr(flat: Expr, dict: DictExpr)

  final case class ShredUnion(e1: BagExpr, e2: BagExpr) extends BagExpr {
     // Could be a heterogeneous union where labels in e1 and e2
     // for the same attribute encapsulate different parameters
     override def tp: BagType = e1.tp
  }

  final case class Lookup(lbl: LabelExpr, dict: BagDictExpr) extends BagExpr {
    def tp: BagType = dict.tp.flatTp
  }

  final case class ShredAssignment(name: String, rhs: ShredExpr)

  final case class ShredProgram(statements: List[ShredAssignment])

  object ShredProgram {
    def apply(statements: ShredAssignment*): ShredProgram = ShredProgram(List(statements: _*))
  }

}
