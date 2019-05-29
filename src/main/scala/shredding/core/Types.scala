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
  * Calculus types
  */


case class BagCType(tp: Type) extends Type

case object EmptyCType extends Type

case class RecordCType(attrTps: Map[String, Type]) extends Type {
  def apply(n: String): Type = attrTps(n)
}

object RecordCType {
  def apply(attrTps: (String, Type)*): RecordCType = RecordCType(Map(attrTps: _*))
}

case class TTupleType(attrTps: Type*) extends Type {
  def apply(n: Int): Type = attrTps(n)
}

case class KVTupleCType(e1: Type, e2: Type) extends Type{
  def apply(n: String): Type = n match {
    case "key" => e1
    case "0" => e1
    case "value"  => e2
    case "1" => e2
    case _ => e2 match {
      case t:RecordCType => t(n)
      case t:KVTupleCType => t(n)
      case t => sys.error(n)
      // todo other instances
    }
  }
  def _1 = e1
  def _2 = e2
}

trait TDict extends Type
trait TTupleDict extends Type

case object EmptyDictCType extends TDict with TTupleDict

case class BagDictCType(flatTp: BagCType, dictTp: TTupleDict) extends TDict {
  def apply(n: String): Type = n match {
    case "lbl" => flatTp.tp.asInstanceOf[KVTupleCType].e1
    case "flat" => flatTp.tp.asInstanceOf[KVTupleCType].e2
    case "tupleDict" => dictTp
    case "0" => flatTp
    case "1" => dictTp
  }
  def lbl: LabelType = flatTp.tp.asInstanceOf[KVTupleCType]._1.asInstanceOf[LabelType]
  def flat: BagCType = flatTp.tp.asInstanceOf[KVTupleCType]._2.asInstanceOf[BagCType]
  def _1 = flatTp
  def _2 = dictTp
}

case class TupleDictCType(attrTps: Map[String, TDict]) extends TTupleDict {
  def apply(n: String): TDict = attrTps(n)
}

object TupleDictCType {
  def apply(attrTps: (String, TDict)*): TupleDictCType = TupleDictCType(Map(attrTps: _*))
}

