package shredding.calc

import shredding.core._
import shredding.nrc.{Dictionary, ShreddedNRC}

/**
  * Base NRC expressions
  */
trait BaseCalc {

  sealed trait CompCalc extends Serializable { self =>
    
    def tp: Type
   
    def isOutermost: Boolean = false
    def isGenerator: Boolean = false
    def isBind: Boolean = false
    def isEmptyGenerator: Boolean = false
    def isMergeGenerator: Boolean = false
    def isIfGenerator: Boolean = false 
    def isBagCompGenerator: Boolean = false
    def hasGenerator: Boolean = false 
    def hasBind: Boolean = false
    def hasEmptyGenerator: Boolean = false 
    def hasMergeGenerator: Boolean = false 
    def hasIfGenerator: Boolean = false
    def hasBagCompGenerator: Boolean = false
  }

  trait TupleAttributeCalc extends CompCalc {
    def tp: TupleAttributeType
  }

  trait LabelAttributeCalc extends CompCalc {
    def tp: LabelAttributeType
  }

  trait PrimitiveCalc extends TupleAttributeCalc with LabelAttributeCalc {
    def tp: PrimitiveType 
  }

  trait BagCalc extends TupleAttributeCalc {
    def tp: BagType   
  }

  trait TupleCalc extends CompCalc with LabelAttributeCalc {
    def tp: TupleType 
  }

  trait LabelCalc extends TupleAttributeCalc with LabelAttributeCalc {
    def tp: LabelType
  }

}

/**
  * Comprehension calculus constructs
  */

trait Calc extends BaseCalc {  

  /**
    * Any of the base types (int, string, ...)
    */
  case class Constant(x: Any, tp: PrimitiveType) extends PrimitiveCalc


  /**
    * Variable wrappers
    */    
  trait Var extends CompCalc {
    def varDef: VarDef
    def name: String = varDef.name
    def tp: Type = varDef.tp
  }

  object Var {
    def apply(d: VarDef): Var = d.tp match {
      case t:PrimitiveType => PrimitiveVar(d)
      case t:BagType => BagVar(d)
      case t:TupleType => TupleVar(d)
      case t:LabelType => LabelVar(d) 
      case _ => throw new IllegalArgumentException(s"cannot create Var with type ${d.tp}")
    }
  }

  case class PrimitiveVar(varDef: VarDef) extends PrimitiveCalc with Var {
    override val tp: PrimitiveType = super.tp.asInstanceOf[PrimitiveType]
  }

  case class LabelVar(varDef: VarDef) extends LabelCalc with Var {
    override val tp: LabelType = super.tp.asInstanceOf[LabelType]
  }

  case class BagVar(varDef: VarDef) extends BagCalc with Var {
    override val tp: BagType = super.tp.asInstanceOf[BagType]
  }

  case class TupleVar(varDef: VarDef) extends TupleCalc with Var {
    override val tp: TupleType = super.tp.asInstanceOf[TupleType]
  }

  trait Proj extends CompCalc{
    def tuple: TupleCalc
    def field: String
  }

  case object Proj{
    def apply(tuple: TupleCalc, field: String): TupleAttributeCalc = tuple.tp.attrs(field) match {
      case t:PrimitiveType => ProjToPrimitive(tuple, field)
      case t:BagType => ProjToBag(tuple, field)
      case t:LabelType => ProjToLabel(tuple, field)
      case t => sys.error("Unknown type in Proj.apply: " + t)
    }
  }

  case class ProjToPrimitive(tuple: TupleCalc, field: String) extends PrimitiveCalc with Proj {
    val tp: PrimitiveType = tuple.tp.attrs(field).asInstanceOf[PrimitiveType]
  }

  case class ProjToBag(tuple: TupleCalc, field: String) extends BagCalc with Proj {
    val tp: BagType = tuple.tp.attrs(field).asInstanceOf[BagType]
  }

  case class ProjToLabel(tuple: TupleCalc, field: String) extends LabelCalc with Proj {
    val tp: LabelType = tuple.tp.attrs(field).asInstanceOf[LabelType]
  }

  trait Comprehension extends CompCalc{
    def e: CompCalc
    def qs: List[CompCalc]
    override def isOutermost = true  
    override def hasGenerator = qs.map(_.isGenerator).contains(true)
    override def hasBind = qs.map(_.isBind).contains(true)
    override def hasEmptyGenerator = qs.map(_.isEmptyGenerator).contains(true)
    override def hasMergeGenerator = qs.map(_.isMergeGenerator).contains(true)
    override def hasIfGenerator = qs.map(_.isIfGenerator).contains(true)
    override def hasBagCompGenerator = qs.map(_.isBagCompGenerator).contains(true)
  }

  case object Comprehension{
    def apply(e: CompCalc, qs: List[CompCalc]): Comprehension = e.tp match {
      case t:PrimitiveType => CountComp(e.asInstanceOf[PrimitiveCalc], qs)
      case t:TupleType => BagComp(e.asInstanceOf[TupleCalc], qs)
      case _ => sys.error("Unsupported type in a monoid")
    }
  }

  /**
    * Bag comprehension representing union over a bag: { e | qs ... }
    */
  case class BagComp(e: TupleCalc, qs: List[CompCalc]) extends BagCalc with Comprehension{
    val tp: BagType = BagType(e.tp)
  }

  /**
    * Merge is union (e1 U e2)
    * Used in N8 of normalization
    */
  case class Merge(e1: BagCalc, e2: BagCalc) extends BagCalc {
    assert(e1.tp == e2.tp)
    val tp: BagType = e1.tp
  }

  /**
    * Singleton construct and zero type
    */
  case class Sng(e: TupleCalc) extends BagCalc { val tp: BagType = BagType(e.tp) }
  case class Zero() extends BagCalc{
    val tp: BagType = BagType(TupleType())
  }

  /**
    * (A1 = e1, ..., An = en)
    */
  // change this to attrs
  case class Tup(fields: Map[String, TupleAttributeCalc]) extends TupleCalc {
    val tp: TupleType = TupleType(fields.map(f => f._1 -> f._2.tp))
  }
  object Tup{
    def apply(fs: (String, TupleAttributeCalc)*): Tup = Tup(Map(fs:_*))
  }

  /**
    * if pred then { e1 | qs1 ... } else { e2 | qs2 ... }
    */
  case class IfStmt(cond: PrimitiveCalc, e1: BagCalc, e2: Option[BagCalc] = None) extends BagCalc {
    assert(cond.tp == BoolType)
    assert(e2.isEmpty || e1.tp == e2.get.tp) 
    val tp: BagType = e1.tp
  }

  /**
    * Primitive monoid - count
    */
  case class CountComp(e: PrimitiveCalc, qs: List[CompCalc]) extends PrimitiveCalc with Comprehension{
    // enforce e to not be a bag type
    val tp: PrimitiveType = IntType 
  }

  /**
    * Represents an input relation
    */
  case class InputR(n: String, tuples: List[Any], tp: BagType) extends BagCalc

  /**
    * Binding of a variable to an expression 
    * v <- {e} => v bind e (N6)
    * { e1 | ..., v <- {e | r }, .. } => {e1 | ..., r, v bind e, ... } (N8)
    */
  trait Bind extends CompCalc
  /**
    * Binding of a source to a variable denotate an iteration: v <- X 
    */
  case class Generator(x: VarDef, e: BagCalc) extends BagCalc with Bind{
    assert(x.tp == e.tp.tp)
    val tp: BagType = e.tp
    override def isGenerator = true
    override def isEmptyGenerator = e match {
      case z @ Zero() => true
      case z @ Sng(Tup(e)) => e.isEmpty
      case _ => false
    }
    override def isMergeGenerator = e match {
      case z @ Merge(e1, e2) => true
      case _ => false
    }
    override def isIfGenerator = e match {
      case z @ IfStmt(c, e1, e2) => true
      case _ => false
    }

    override def isBagCompGenerator = e match {
      case z @ BagComp(e1, qs1) => true
      case _ => false
    }

  }

  case class BindPrimitive(x: VarDef, e: PrimitiveCalc) extends PrimitiveCalc with Bind{ 
    assert(x.tp == e.tp) 
    val tp: PrimitiveType = e.tp
    override def isBind = true
  }

  case class BindTuple(x: VarDef, e: TupleCalc) extends TupleCalc with Bind{ 
    assert(x.tp == e.tp)
    val tp: TupleType = e.tp 
    override def isBind = true
  }

  object Bind{
    def apply(x: VarDef, v: CompCalc): Bind = v.tp match {
      case t: TupleType => BindTuple(x, v.asInstanceOf[TupleCalc])
      case t: PrimitiveType => BindPrimitive(x, v.asInstanceOf[PrimitiveCalc])
      case t: BagType => Generator(x, v.asInstanceOf[BagCalc])
      case _ => throw new IllegalArgumentException(s"cannot bind VarDef(${x.name})")
    }
  }

  /**
    * Condition types
    */
  case class Conditional(op: OpCmp, e1: CompCalc, e2: CompCalc) extends PrimitiveCalc{ 
    val tp: PrimitiveType = BoolType
  }

  case class NotCondition(e1: CompCalc) extends PrimitiveCalc { 
    assert(e1.tp == BoolType)
    val tp: PrimitiveType = BoolType 
  }

  case class AndCondition(e1: CompCalc, e2: CompCalc) extends PrimitiveCalc { 
    assert(e1.tp == e2.tp)
    assert(e1.tp == BoolType)
    val tp: PrimitiveType = BoolType
  }

  case class OrCondition(e1: CompCalc, e2: CompCalc) extends PrimitiveCalc {
    assert(e1.tp == e2.tp)
    assert(e1.tp == BoolType)
    val tp: PrimitiveType = BoolType
  }

  case class CNamed(n: String, e: CompCalc) extends CompCalc {
    val tp: Type = e.tp 
  }  
  
  case class CSequence(exprs: List[CompCalc]) extends CompCalc {
    val tp: TupleType = TupleType()
  }

}

trait ShreddedCalc extends Calc 
  with Dictionary with ShreddedNRC {

  case class CLabel(vars: Set[Var] = Set.empty, id: Int) extends LabelCalc {
    val tp: LabelType = LabelType(vars.map(r => r.name -> r.tp.asInstanceOf[LabelAttributeType]).toMap)
    
    override def equals(that: Any): Boolean = that match {
      case that: CLabel => this.id == that.id
      case _ => false
    }

    override def hashCode: Int = id.hashCode()

  }

  case class CLookup(lbl: LabelCalc, dict: BagDict) extends BagCalc{
    def tp: BagType = dict.flatBagTp
  }
}
