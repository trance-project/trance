package shredding.nrc

import shredding.core._

/**
  * Shredding NRC extension
  */
trait ShredNRC extends NRC with Label with Dictionary {

  /**
    * Shredded expression E is a pair of E^flat and E^dict
    */

  final case class ShredExpr(flat: Expr, dict: DictExpr)

  final case class ShredNamed(v: VarDef, e: ShredExpr)

  final case class ShredSequence(exprs: List[ShredNamed])

  final case class Lookup(lbl: LabelExpr, dict: BagDictExpr) extends BagExpr {
    def tp: BagType = dict.tp.flatTp
  }

  implicit class LookupOps(d: BagDictExpr) {
    def lookup(lbl: LabelExpr): BagExpr = lbl match {
      case LabelIfThenElse(c, l1, l2) =>
        BagIfThenElse(c, d.lookup(l1), l2.map(d.lookup))
      case _ => d match {
        case b: BagDict if b.lbl.tp == lbl.tp => b.flat
        case _ => Lookup(lbl, d)
      }
    }
  }

  case object ShredVarRef {
    def apply(varDef: VarDef): Expr = varDef.tp match {
      case _: LabelType => LabelVarRef(varDef)
      case _ => VarRef(varDef)
    }

    def apply(n: String, tp: Type): Expr = apply(VarDef(n, tp))
  }

  case object ShredProject {
    def apply(tuple: TupleExpr, field: String): TupleAttributeExpr = tuple match {
      case t: Tuple => t.fields(field)
      case _ => tuple.tp(field) match {
        case _: LabelType => LabelProject(tuple, field)
        case _ => tuple(field)
      }
    }
  }

  case object ShredLet {
    def apply(x: VarDef, e1: Expr, e2: Expr): Expr = e2.tp match {
      case _: LabelType => LabelLet(x, e1, e2.asInstanceOf[LabelExpr])
      case _: DictType => DictLet(x, e1, e2.asInstanceOf[DictExpr])
      case _ => Let(x, e1, e2)
    }
  }

  case object ShredIfThenElse {
    def apply(c: Cond, e1: Expr, e2: Expr): Expr = e1.tp match {
      case _: LabelType =>
        LabelIfThenElse(c, e1.asInstanceOf[LabelExpr], Some(e2.asInstanceOf[LabelExpr]))
      case _: DictType =>
        DictIfThenElse(c, e1.asInstanceOf[DictExpr], e2.asInstanceOf[DictExpr])
      case _ => IfThenElse(c, e1, e2)
    }

    def apply(c: Cond, e1: Expr): Expr = e1.tp match {
      case _: LabelType => LabelIfThenElse(c, e1.asInstanceOf[LabelExpr], None)
      case _ => IfThenElse(c, e1)
    }
  }


}
