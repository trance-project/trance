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

case object LabelType extends LabelAttributeType

case class BagType(tp: TupleType) extends TupleAttributeType

case class TupleType(attrs: Map[String, TupleAttributeType]) extends Type

/**
  * Helper object for creating tupled types
  */
object TupleType {
  def apply(attrs: (String, TupleAttributeType)*): TupleType = TupleType(Map(attrs: _*))
}