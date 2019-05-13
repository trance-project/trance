package shredding.core

/**
  * NRC type system: primitive types, bag type, tuple type
  */
sealed trait Type

trait TupleAttributeType extends Type

trait PrimitiveType extends TupleAttributeType

case object BoolType extends PrimitiveType

case object IntType extends PrimitiveType

case object StringType extends PrimitiveType

trait TTuple extends Type

case class BagType(tp: TTuple) extends TupleAttributeType

case class TupleType(attrTps: Map[String, TupleAttributeType]) extends TTuple {
  def apply(n: String): TupleAttributeType = attrTps(n)
}

object TupleType {
  def apply(attrTps: (String, TupleAttributeType)*): TupleType = TupleType(Map(attrTps: _*))
}

/***
  * Shredding type extensions: label type and dictionary type
  *
  */
case class LabelType(attrTps: Map[String, Type]) extends TupleAttributeType {
  def apply(n: String): Type = attrTps(n)

  override def equals(that: Any): Boolean = that match {
    case that: LabelType => this.attrTps == that.attrTps
    case _ => false
  }

}

object LabelType {
  def apply(attrTps: (String, Type)*): LabelType = LabelType(Map(attrTps: _*))
}

trait DictType extends Type

trait TupleDictAttributeType extends DictType

case object EmptyDictType extends TupleDictAttributeType

case class BagDictType(flatTp: BagType, dictTp: TupleDictType) extends TupleDictAttributeType

case class TupleDictType(attrTps: Map[String, TupleDictAttributeType]) extends DictType {
  def apply(n: String): TupleDictAttributeType = attrTps(n)
}

object TupleDictType {
  def apply(attrTps: (String, TupleDictAttributeType)*): TupleDictType = TupleDictType(Map(attrTps: _*))
}

/**
  * Algebra Types
  */
case class KVTupleType(e1: Type, e2: Type) extends TTuple
