package shredding.core

/**
  * NRC type system
  */
sealed trait Type

sealed trait TupleAttributeType extends Type

sealed trait LabelAttributeType extends Type

sealed trait PrimitiveType extends TupleAttributeType with LabelAttributeType

case object BoolType extends PrimitiveType

case object IntType extends PrimitiveType

case object StringType extends PrimitiveType

case class BagType(tp: TupleType) extends TupleAttributeType

case class TupleType(attrTps: Map[String, TupleAttributeType]) extends Type with LabelAttributeType

object TupleType {
  def apply(attrs: (String, TupleAttributeType)*): TupleType = TupleType(Map(attrs: _*))
}

/***
  * Shredding type extensions
  *
  */
case class LabelType(attrs: Map[String, LabelAttributeType]) extends TupleAttributeType with LabelAttributeType

object LabelType {
  def apply(attrs: (String, LabelAttributeType)*): LabelType = LabelType(Map(attrs: _*))
}

sealed trait DictType extends LabelAttributeType

sealed trait TupleDictAttributeType extends DictType

case object EmptyDictType extends TupleDictAttributeType

case class BagDictType(flatTp: BagType, dictTp: TupleDictType) extends TupleDictAttributeType

case class TupleDictType(attrTps: Map[String, TupleDictAttributeType]) extends DictType

object TupleDictType {
  def apply(attrs: (String, TupleDictAttributeType)*): TupleDictType = TupleDictType(Map(attrs: _*))
}
