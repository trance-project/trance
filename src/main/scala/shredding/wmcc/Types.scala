package shredding.wmcc

import shredding.core._

case class BagCType(tp: Type) extends Type

case object EmptyCType extends Type

case class RecordCType(attrTps: Map[String, Type]) extends Type {
  def apply(n: String): Type = attrTps(n)
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
    case "flat" => flatTp.tp.asInstanceOf[TTupleType](1)
    case "_1" => flatTp
    case "_2" => dictTp
  }
  def lbl: LabelType = flatTp.tp.asInstanceOf[TTupleType](0).asInstanceOf[LabelType]
  def flat: BagCType = flatTp.tp.asInstanceOf[TTupleType](1).asInstanceOf[BagCType]
  def _1 = flatTp
  def _2 = dictTp
}

case class TupleDictCType(attrTps: Map[String, TDict]) extends TTupleDict {
  def apply(n: String): TDict = attrTps(n)
}

object TupleDictCType {
  def apply(attrTps: (String, TDict)*): TupleDictCType = TupleDictCType(Map(attrTps: _*))
}
