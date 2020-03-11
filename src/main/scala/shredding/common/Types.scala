package shredding.core

/**
  * NRC type system: primitive types, bag type, tuple type
  */

sealed trait Type

sealed trait TupleAttributeType extends Type

sealed trait PrimitiveType extends TupleAttributeType

sealed trait NumericType extends PrimitiveType

case object BoolType extends PrimitiveType

case object StringType extends PrimitiveType

case object IntType extends NumericType

case object LongType extends NumericType

case object DoubleType extends NumericType

object NumericType {
  def resolve(tp1: NumericType, tp2: NumericType): NumericType = (tp1, tp2) match {
    case (DoubleType, _) | (_, DoubleType) => DoubleType
    case (LongType, _) | (_, LongType) => LongType
    case (IntType, _) | (_, IntType) => IntType
    case _ => sys.error("Cannot resolve types " + tp1 + " and " + tp2)
  }
}

final case class BagType(tp: TupleType) extends TupleAttributeType

final case class TupleType(attrTps: Map[String, TupleAttributeType]) extends Type {
  def apply(n: String): TupleAttributeType = attrTps(n)
}

object TupleType {
  def apply(attrTps: (String, TupleAttributeType)*): TupleType = TupleType(Map(attrTps: _*))
}

/***
  * Shredding type extensions: label type and dictionary type
  *
  */
final case class LabelType(attrTps: Map[String, Type]) extends TupleAttributeType {
  def apply(n: String): Type = attrTps(n)

  override def equals(that: Any): Boolean = that match {
    case that: LabelType => this.attrTps == that.attrTps
    case that:RecordCType => this.attrTps == that.attrTps
    case _ => false
  }
}

object LabelType {
  def apply(attrTps: (String, Type)*): LabelType = LabelType(Map(attrTps: _*))
}

sealed trait DictType extends Type

sealed trait TupleDictAttributeType extends DictType

case object EmptyDictType extends TupleDictAttributeType

final case class BagDictType(flatTp: BagType, dictTp: TupleDictType) extends TupleDictAttributeType

final case class TupleDictType(attrTps: Map[String, TupleDictAttributeType]) extends DictType {
  def apply(n: String): TupleDictAttributeType = attrTps(n)
}

object TupleDictType {
  def apply(attrTps: (String, TupleDictAttributeType)*): TupleDictType = TupleDictType(Map(attrTps: _*))
}

/**
  * Types used for WMCC
  *
  * TODO: move elsewhere?
  */

case class TypeSet(tp: Map[Type, String]) extends Type 

case class BagCType(tp: Type) extends Type

case object EmptyCType extends Type

case class RecordCType(attrTps: Map[String, Type]) extends Type {
  def apply(n: String): Type = attrTps(n)
  override def equals(that: Any): Boolean = that match {
    case that: LabelType => this.attrTps == that.attrTps
    case that:RecordCType => this.attrTps == that.attrTps
    case _ => false
  }

}

object RecordCType {
  def apply(attrTps: (String, Type)*): RecordCType = RecordCType(Map(attrTps: _*))
}

case class TTupleType(attrTps: List[Type]) extends Type {
  def apply(n: Int): Type = attrTps(n)
}

trait TDict extends Type
trait TTupleDict extends Type

case object EmptyDictCType extends TDict with TTupleDict

case class BagDictCType(flatTp: BagCType, dictTp: TTupleDict) extends TDict {
  def apply(n: String): Type = n match {
    case "lbl" => flatTp.tp.asInstanceOf[TTupleType](0)
    case "flat" => flatTp.tp match {
      case TTupleType(fs) => fs(1)
      case _ => flatTp
    }
    case "_1" => flatTp
    case "_2" => dictTp
  }
  def lbl: LabelType = flatTp.tp.asInstanceOf[TTupleType](0).asInstanceOf[LabelType]
  def flat: BagCType = flatTp.tp match {
    case ttp @ TTupleType(List(IntType, RecordCType(_))) => BagCType(ttp)
    case TTupleType(fs) => fs(1).asInstanceOf[BagCType]
    case _ => flatTp //sys.error("type not supported in flat bag")
  }
  def _1 = flatTp
  def _2 = dictTp
}

case class TupleDictCType(attrTps: Map[String, TDict]) extends TTupleDict {
  def apply(n: String): TDict = attrTps(n)
}

object TupleDictCType {
  def apply(attrTps: (String, TDict)*): TupleDictCType = TupleDictCType(Map(attrTps: _*))
}
