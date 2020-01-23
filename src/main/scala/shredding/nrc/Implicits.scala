package shredding.nrc

import shredding.core.{OpDivide, OpEq, OpGe, OpGt, OpMinus, OpMod, OpMultiply, OpNe, OpPlus}

trait Implicits {
  this: ShredNRC =>

  implicit class ExprOps(e: Expr) {
    def asPrimitive: PrimitiveExpr = e.asInstanceOf[PrimitiveExpr]

    def asNumeric: NumericExpr = e.asInstanceOf[NumericExpr]

    def asCond: CondExpr = e.asInstanceOf[CondExpr]

    def asBag: BagExpr = e.asInstanceOf[BagExpr]

    def asTuple: TupleExpr = e.asInstanceOf[TupleExpr]

    def asLabel: LabelExpr = e.asInstanceOf[LabelExpr]

    def asDict: DictExpr = e.asInstanceOf[DictExpr]

    def asBagDict: BagDictExpr = e.asInstanceOf[BagDictExpr]

    def asTupleDict: TupleDictExpr = e.asInstanceOf[TupleDictExpr]
  }

  implicit class TupleExprOps(tuple: TupleExpr) {
    def apply(field: String): TupleAttributeExpr = Project(tuple, field)
  }

  implicit class TupleDictExprOps(tuple: TupleDictExpr) {
    def apply(field: String): TupleDictAttributeExpr = Project(tuple, field)
  }

  implicit class CmpOps(e1: PrimitiveExpr) {
    def <(e2: PrimitiveExpr): CondExpr = Cmp(OpGt, e2, e1)

    def <=(e2: PrimitiveExpr): CondExpr = Cmp(OpGe, e2, e1)

    def >(e2: PrimitiveExpr): CondExpr = Cmp(OpGt, e1, e2)

    def >=(e2: PrimitiveExpr): CondExpr = Cmp(OpGe, e1, e2)

    def ==(e2: PrimitiveExpr): CondExpr = Cmp(OpEq, e1, e2)

    def !=(e2: PrimitiveExpr): CondExpr = Cmp(OpNe, e1, e2)
  }

  implicit class CondExprOps(c1: CondExpr) {
    def &&(c2: CondExpr): CondExpr = And(c1, c2)

    def ||(c2: CondExpr): CondExpr = Or(c1, c2)

    def not: CondExpr = Not(c1)
  }

  implicit class ArithmeticOps(e1: NumericExpr) {
    def +(e2: NumericExpr): ArithmeticExpr = ArithmeticExpr(OpPlus, e1, e2)

    def -(e2: NumericExpr): ArithmeticExpr = ArithmeticExpr(OpMinus, e1, e2)

    def *(e2: NumericExpr): ArithmeticExpr = ArithmeticExpr(OpMultiply, e1, e2)

    def /(e2: NumericExpr): ArithmeticExpr = ArithmeticExpr(OpDivide, e1, e2)

    def mod(e2: NumericExpr): ArithmeticExpr = ArithmeticExpr(OpMod, e1, e2)
  }

  implicit class BagDictExprOps(d: BagDictExpr) {
    def tupleDict: TupleDictExpr = d match {
      case b: BagDict => b.dict
      case BagDictUnion(d1, d2) => TupleDictUnion(d1.tupleDict, d2.tupleDict)
      case _ => TupleDictProject(d)
    }
  }

  implicit class LookupOps(d: BagDictExpr) {
    def lookup(lbl: LabelExpr): BagExpr = lbl match {
      case LabelLet(x, e1, l2) =>
        BagLet(x, e1, d.lookup(l2))
      case LabelIfThenElse(c, l1, l2) =>
        BagIfThenElse(c, d.lookup(l1), l2.map(d.lookup))
      case _ => d match {
        case b: BagDict if b.lbl.tp == lbl.tp => b.flat
        case _ => Lookup(lbl, d)
      }
    }
  }

}
