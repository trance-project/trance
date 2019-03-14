package shredding.nrc2

/**
  * NRC type system
  */
sealed trait Type

trait TupleAttributeType extends Type

trait LabelAttributeType extends TupleAttributeType

trait PrimitiveType extends LabelAttributeType

case object IntType extends PrimitiveType

case object StringType extends PrimitiveType

case class LabelType(attrs: Map[String, LabelAttributeType]) extends LabelAttributeType

case class BagType(tp: TupleType) extends TupleAttributeType

case class TupleType(attrs: Map[String, TupleAttributeType]) extends Type

/**
  * Helper objects for creating tupled types
  */
object TupleType {
  def apply(attrs: (String, TupleAttributeType)*): TupleType = TupleType(Map(attrs: _*))
}

object LabelType {
  def apply(attrs: (String, LabelAttributeType)*): LabelType = LabelType(Map(attrs: _*))
}