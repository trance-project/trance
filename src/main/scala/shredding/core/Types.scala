package shredding.core

/**
  * NRC type system: primitive types, bag type, tuple type
  */
sealed trait Type{
  def isLabel: Boolean = false
}

sealed trait TupleAttributeType extends Type

sealed trait PrimitiveType extends TupleAttributeType

case object BoolType extends PrimitiveType

case object IntType extends PrimitiveType

case object StringType extends PrimitiveType

case class BagType(tp: TupleType) extends TupleAttributeType

case class TupleType(attrTps: Map[String, TupleAttributeType]) extends Type {
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
}

object LabelType {
  def apply(attrTps: (String, Type)*): LabelType = LabelType(Map(attrTps: _*))
}

sealed trait DictType extends Type

sealed trait TupleDictAttributeType extends DictType

case object EmptyDictType extends TupleDictAttributeType

case class BagDictType(flatTp: BagType, dictTp: TupleDictType) extends TupleDictAttributeType

case class TupleDictType(attrTps: Map[String, TupleDictAttributeType]) extends DictType {
  def apply(n: String): TupleDictAttributeType = attrTps(n)
}

object TupleDictType {
  def apply(attrTps: (String, TupleDictAttributeType)*): TupleDictType = TupleDictType(Map(attrTps: _*))
}
