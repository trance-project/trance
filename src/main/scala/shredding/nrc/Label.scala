package shredding.nrc

import shredding.core._

/**
  * Label extensions
  */
trait Label {
  this: ShredNRC =>

  sealed trait LabelExpr extends TupleAttributeExpr {
    def tp: LabelType
  }

  case class LabelVarRef(varDef: VarDef) extends LabelExpr with VarRef {
    override def tp: LabelType = super.tp.asInstanceOf[LabelType]
  }

  case class LabelProject(tuple: TupleExpr, field: String) extends LabelExpr with Project {
    override def tp: LabelType = super.tp.asInstanceOf[LabelType]
  }

  case class LabelLet(x: VarDef, e1: Expr, e2: LabelExpr) extends LabelExpr with Let {
    assert(x.tp == e1.tp)

    val tp: LabelType = e2.tp
  }

  case class LabelIfThenElse(cond: Cond, e1: LabelExpr, e2: Option[LabelExpr]) extends LabelExpr with IfThenElse {
    assert(e2.isEmpty || e1.tp == e2.get.tp)

    val tp: LabelType = e1.tp
  }

  trait ExtractLabel {
    def lbl: LabelExpr

    def e: Expr
  }

  case object ExtractLabel {
    def apply(lbl: LabelExpr, e: Expr): Expr = e.tp match {
      case _: PrimitiveType => PrimitiveExtractLabel(lbl, e.asInstanceOf[PrimitiveExpr])
      case _: BagType => BagExtractLabel(lbl, e.asInstanceOf[BagExpr])
      case _: TupleType => TupleExtractLabel(lbl, e.asInstanceOf[TupleExpr])
      case _: LabelType => LabelExtractLabel(lbl, e.asInstanceOf[LabelExpr])
      case t => sys.error("Cannot create ExtractLabel for type " + t)
    }
  }

  case class PrimitiveExtractLabel(lbl: LabelExpr, e: PrimitiveExpr) extends PrimitiveExpr with ExtractLabel {
    def tp: PrimitiveType = e.tp
  }

  case class BagExtractLabel(lbl: LabelExpr, e: BagExpr) extends BagExpr with ExtractLabel {
    def tp: BagType = e.tp
  }

  case class TupleExtractLabel(lbl: LabelExpr, e: TupleExpr) extends TupleExpr with ExtractLabel {
    def tp: TupleType = e.tp
  }

  case class LabelExtractLabel(lbl: LabelExpr, e: LabelExpr) extends LabelExpr with ExtractLabel {
    def tp: LabelType = e.tp
  }

  trait LabelParameter {
    def name: String

    def tp: Type
  }

  case class VarRefLabelParameter(v: VarRef) extends LabelParameter {
    def name: String = v.name

    def tp: Type = v.tp
  }

  case class ProjectLabelParameter(p: Project) extends LabelParameter {
    def name: String = p.tuple.asInstanceOf[TupleVarRef].name + "." + p.field

    def tp: Type = p.tp
  }

  object NewLabel {
    private var currId = 0

    def getNextId: Int = {
      currId += 1
      currId
    }

    implicit def orderingById: Ordering[NewLabel] = Ordering.by(e => e.id)
  }

  case class NewLabel(vars: Set[LabelParameter] = Set.empty) extends LabelExpr {
    val id: Int = NewLabel.getNextId

    val tp: LabelType = LabelType(vars.map(v => v.name -> v.tp).toMap)

    override def equals(that: Any): Boolean = that match {
      case that: NewLabel => this.id == that.id
      case _ => false
    }

    override def hashCode: Int = id.hashCode()

    override def toString: String =
      s"Label(${(id :: vars.map(_.toString).toList).mkString(", ")}"
  }
}
