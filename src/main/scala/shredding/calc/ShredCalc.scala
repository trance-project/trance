package shredding.calc

import shredding.core._

/**
  * Extensions to the comprehension calculus needed for Shredding
  */
trait ShredCalc extends Calc {

  sealed trait LabelCalc extends TupleAttributeCalc {
    def tp:LabelType
  }

  case class LabelVar(varDef: VarDef) extends LabelCalc with Var {
    override def tp: LabelType = varDef.tp.asInstanceOf[LabelType]
  }

  case class LabelProject(tuple: TupleCalc, field: String) extends LabelCalc with Proj {
    override def tp: LabelType = tuple.tp.attrTps(field).asInstanceOf[LabelType]
  }

  case class BindLabel(x: VarDef, e: LabelCalc) extends LabelCalc with Bind{ 
    assert(x.tp == e.tp)
    val tp: LabelType = e.tp 
    override def isBind = true
  }
  

  // label id is inherited from NRC
  case class CLabel(id: Int, vars: Map[String, CompCalc]) extends LabelCalc {
    val tp: LabelType = LabelType(vars.map(f => f._1 -> f._2.tp))

    override def equals(that: Any): Boolean = that match {
      case that: CLabel => this.id == that.id
      case _ => false
    }

    override def hashCode: Int = id.hashCode()

  }

  object CLabel{
    def apply(id: Int, vars: (String, CompCalc)*): CLabel = CLabel(id, Map(vars:_*))
  }


  sealed trait DictCalc extends CompCalc {
    def tp: DictType
  }

  sealed trait TupleDictAttributeCalc extends DictCalc {
    def tp: TupleDictAttributeType
  }

  case object EmptyCDict extends TupleDictAttributeCalc {
    def tp: TupleDictAttributeType = EmptyDictType 
  }
  
  sealed trait BagDictCalc extends TupleDictAttributeCalc {
    def tp: BagDictType
  }

  sealed trait TupleDictCalc extends DictCalc {
    def tp: TupleDictType
  }

  case object DictVar {
    def apply(varDef: VarDef): DictCalc = varDef.tp match {
      case EmptyDictType => EmptyCDict
      case _:BagDictType => BagDictVar(varDef)
      case _:TupleDictType => TupleDictVar(varDef)
      case t => sys.error("Cannot create DictVar for type " +t)
    }

    def apply(n: String, tp: Type): DictCalc = apply(VarDef(n, tp))
  }

  case class BagDictVar(varDef: VarDef) extends BagDictCalc with Var {
    override def tp: BagDictType = super.tp.asInstanceOf[BagDictType]
  }

  case class TupleDictVar(varDef: VarDef) extends TupleDictCalc with Var {
    override def tp: TupleDictType = super.tp.asInstanceOf[TupleDictType]
  }

  case class BagCDict(lbl: LabelCalc, flat: BagCalc, dict: TupleDictCalc) extends BagDictCalc {
    val tp: BagDictType = BagDictType(flat.tp, dict.tp)
  }

  case class TupleCDict(fields: Map[String, TupleDictAttributeCalc]) extends TupleDictCalc {
    val tp: TupleDictType = TupleDictType(fields.map(f => f._1 -> f._2.tp))
  }

  case class BagDictProj(dict: TupleDictCalc, field: String) extends BagDictCalc {
    val tp: BagDictType = dict.tp(field).asInstanceOf[BagDictType]
  }

  case class TupleDictProj(dict: BagDictCalc) extends TupleDictCalc {
    val tp: TupleDictType = dict.tp.dictTp
  }

  case class DictCUnion(dict1: DictCalc, dict2: DictCalc) extends DictCalc {
    assert(dict1.tp == dict2.tp)
    val tp: DictType = dict1.tp
  }

  case class CLookup(lbl: LabelCalc, dict: BagCDict) extends BagCalc{
    def tp: BagType = dict.flat.tp
  }

}
