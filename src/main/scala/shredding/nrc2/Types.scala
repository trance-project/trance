package shredding.nrc2

/**
  * NRC type system
  */
sealed trait Type
trait AttributeType extends Type
trait PrimitiveType extends AttributeType

case object IntType extends PrimitiveType
case object StringType extends PrimitiveType
case object BoolType extends PrimitiveType
case class BagType(tp: TupleType) extends AttributeType
case class TupleType(tps: Map[String, AttributeType]) extends Type

object TupleType {
  def apply(tps: (String, AttributeType)*): TupleType = tps match {
    case Nil => TupleType(Map[String, AttributeType]())
    case _ => TupleType(Map(tps: _*))
  }
}
