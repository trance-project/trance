package shredding.algebra

sealed trait Type{
  def isLabel: Boolean = false
}

trait TupleAttributeType extends Type

trait PrimitiveType extends TupleAttributeType

case object BoolType extends PrimitiveType

case object IntType extends PrimitiveType

case object StringType extends PrimitiveType

case class BagType(tp: TupleType) extends TupleAttributeType

case class TupleType(attrTps: Map[String, TupleAttributeType]) extends Type {
  def apply(n: String): TupleAttributeType = attrTps(n)
}
