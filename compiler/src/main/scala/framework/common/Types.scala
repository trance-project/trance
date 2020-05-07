package framework.common

/**
  * NRC type system: primitive types, bag type, tuple type
  */

sealed trait Type { self =>

  def isPartiallyShredded: Boolean = false

  def isPrimitive: Boolean = self match {
    case IntType => true
    case DoubleType => true
    case _ => false
  }

  def isDict: Boolean = false

  def flat: BagCType = self match {
    case _:BagCType => self.flat
    case _:BagDictCType => self.flat
    case _ => sys.error("unsupported call to flat")
  }

  // For debugging
  // override def toString: String = ""
}

trait ReducibleType

trait TupleAttributeType extends Type

trait PrimitiveType extends TupleAttributeType
case object BoolType extends PrimitiveType
case object StringType extends PrimitiveType

trait NumericType extends PrimitiveType with ReducibleType
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

final case class BagType(tp: TupleType) extends TupleAttributeType with ReducibleType

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

  // TODO: Remove this method
  override def equals(that: Any): Boolean = that match {
    case that: LabelType => this.attrTps == that.attrTps
    case that: RecordCType => this.attrTps == that.attrTps
    case _ => false
  }
}

object LabelType {
  def apply(attrTps: (String, Type)*): LabelType = LabelType(Map(attrTps: _*))
}

trait DictType extends Type

trait TupleDictAttributeType extends DictType

case object EmptyDictType extends TupleDictAttributeType

final case class BagDictType(lblTp: LabelType, flatTp: BagType, dictTp: TupleDictType) extends TupleDictAttributeType

final case class TupleDictType(attrTps: Map[String, TupleDictAttributeType]) extends DictType {
  def apply(n: String): TupleDictAttributeType = attrTps(n)
}

object TupleDictType {
  def apply(attrTps: (String, TupleDictAttributeType)*): TupleDictType = TupleDictType(Map(attrTps: _*))
}

final case class MatDictType(keyTp: LabelType, valueTp: BagType) extends Type


/**
  * Types used for Calculus - these types ease some restrictions 
  * to support easier translation to plan operators / optimizations
  * There are three main differences:
  * i) alternating tuple bag restriction is removed
  * ii) named tuples are records
  * iii) tuples without names (useful for unnesting and code generation)
  *
  */

final case class TypeSet(tp: Map[Type, String]) extends Type 

final case class SetType(tp: Type) extends Type

final case class OptionType(tp: Type) extends Type 

final case class BagCType(tp: Type) extends Type {

  override def isDict: Boolean = tp match {
    case TTupleType(fs) => fs.head.isInstanceOf[LabelType]
    case _ => false
  }

}

final case class MatDictCType(keyTp: LabelType, valueTp: BagCType) extends Type {
  override def isDict: Boolean = true
}

case object EmptyCType extends Type

final case class RecordCType(attrTps: Map[String, Type]) extends Type {
  def apply(n: String): Type = attrTps(n)

  def canEqual(a: Any) = a.isInstanceOf[RecordCType]
  override def equals(that: Any): Boolean = that match {
    case RecordCType(fs) => fs.toList == attrTps.toList
    case _ => false
  }

  override def hashCode: Int = this.attrTps.toList.hashCode

}

object RecordCType {
  def apply(attrTps: (String, Type)*): RecordCType = RecordCType(Map(attrTps: _*))
}

final case class TTupleType(attrTps: List[Type]) extends Type {
  def apply(n: Int): Type = attrTps(n)
  override def isDict: Boolean = 
    attrTps.size == 2 && attrTps.head.isInstanceOf[LabelType]
}


/** Dictionary types for calculus
  * Important for scala generator (local evluation) and catching 
  * lambdas that are not normalized in the NRC phase
  */

trait TDict extends Type
trait TTupleDict extends Type

final case object EmptyDictCType extends TDict with TTupleDict

final case class BagDictCType(flatTp: BagCType, dictTp: TTupleDict) extends TDict { self =>
  
  def apply(n: String): Type = n match {
    case "lbl" => flatTp.tp.asInstanceOf[TTupleType](0)
    case "flat" => flatTp.tp match {
      case TTupleType(fs) => fs(1)
      case _ => flatTp
    }
    case "_1" => flatTp
    case "_2" => dictTp
  }

  override def isPartiallyShredded: Boolean = flatTp.tp match {
    case RecordCType(ms) => ms.filter(_._2.isInstanceOf[BagCType]).nonEmpty
    case _ => false
  }
  override def isDict: Boolean = self("lbl") match {
    case EmptyCType => false
    case _ => true
  }
}

final case class TupleDictCType(attrTps: Map[String, TDict]) extends TTupleDict {
  def apply(n: String): TDict = attrTps(n)
}

object TupleDictCType {
  def apply(attrTps: (String, TDict)*): TupleDictCType = TupleDictCType(Map(attrTps: _*))
}
