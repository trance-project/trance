package shredding.core

/**
  * Runtime labels appearing in shredded input relations
  */
trait Label

object LabelDef {
  private var currId = 0

  implicit def orderingById: Ordering[LabelDef] = Ordering.by(e => e.id)
}

case class LabelDef() extends Label{
  val id: Int = {
    LabelDef.currId += 1; LabelDef.currId
  }

  def tp: LabelType = LabelType("id" -> IntType)

  override def equals(that: Any): Boolean = that match {
    case that: LabelDef => this.id == that.id
    case _ => false
  }

  override def hashCode: Int = id.hashCode()

  override def toString: String = s"LabelDef($id)"
}
