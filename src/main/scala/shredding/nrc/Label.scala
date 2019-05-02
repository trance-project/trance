package shredding.nrc

import shredding.core.{LabelType, VarDef}

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

  object Label {
    private var currId = 0

    def getNextId: Int = {
      currId += 1
      currId
    }

    implicit def orderingById: Ordering[Label] = Ordering.by(e => e.id)
  }

  case class Label(vars: Set[VarRef] = Set.empty) extends LabelExpr {
    val id: Int = Label.getNextId

    val tp: LabelType = LabelType(vars.map(v => v.name -> v.tp).toMap)

    override def equals(that: Any): Boolean = that match {
      case that: Label => this.id == that.id
      case _ => false
    }

    override def hashCode: Int = id.hashCode()

    override def toString: String =
      s"Label(${(id :: vars.map(_.toString).toList).mkString(", ")}"
  }
}
