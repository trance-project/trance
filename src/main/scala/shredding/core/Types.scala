package shredding.core

/**
  * NRC type system
  */
sealed trait Type{
  def isLabel: Boolean = false
}

sealed trait TupleAttributeType extends Type

sealed trait LabelAttributeType extends Type

sealed trait PrimitiveType extends TupleAttributeType with LabelAttributeType

case object BoolType extends PrimitiveType

case object IntType extends PrimitiveType

case object StringType extends PrimitiveType

case class BagType(tp: TupleType) extends TupleAttributeType

case class TupleType(attrs: Map[String, TupleAttributeType]) extends Type with LabelAttributeType{
  override def isLabel: Boolean = attrs.filter{ case (k,v) => v.isLabel}.nonEmpty
}

case class LabelType(attrs: Map[String, LabelAttributeType]) extends TupleAttributeType with LabelAttributeType{
  override def isLabel: Boolean = true
}

/**
  * Helper objects for creating tupled types
  */
object TupleType {
  def apply(attrs: (String, TupleAttributeType)*): TupleType = TupleType(Map(attrs: _*))
}

object LabelType {
  def apply(attrs: (String, LabelAttributeType)*): LabelType = LabelType(Map(attrs: _*))
}
