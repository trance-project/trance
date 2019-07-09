package shredding.wmcc

import shredding.core._

/**
  * Weighted Monad Comprehension Calculus (WMCC) expression nodes 
  * includes WMCC nodes for shredding extensions, and 
  * algebra data operators for translating WMCC to plans 
  */


sealed trait CExpr {
  def tp: Type
  def wvars: List[Variable] = List() // remove this 
}

case class InputRef(data: String, tp: Type) extends CExpr 

case class Input(data: List[CExpr]) extends CExpr{
  def tp: BagCType = data match {
    case Nil => BagCType(EmptyCType)
    case _ => BagCType(data.head.tp) 
  }
}

case class Constant(data: Any) extends CExpr{
  def tp: PrimitiveType = data match {
    case _:Int => IntType
    case _:String => StringType
    case _:Boolean => BoolType
  }
}

case object EmptySng extends CExpr {
  def tp: BagCType = BagCType(EmptyCType)
}

case class Sng(e1: CExpr) extends CExpr {
  def tp: BagCType = BagCType(e1.tp)
}

case class WeightedSng(e1: CExpr, w: CExpr) extends CExpr{
  def tp: BagCType = BagCType(e1.tp)
}

case object CUnit extends CExpr {
  def tp: Type = EmptyCType
}

case class Record(fields: Map[String, CExpr]) extends CExpr{
  def tp: RecordCType = RecordCType(fields.map(f => f._1 -> f._2.tp))
  def apply(n: String) = fields(n)
}

case class Tuple(fields: List[CExpr]) extends CExpr {
  def tp: TTupleType = TTupleType(fields.map(_.tp))
  def apply(n: String) = n match {
    case "_1" => fields(0)
    case "_2" => fields(1) 
  } 
  def apply(n: Int) = fields(n)
}

case class Equals(e1: CExpr, e2: CExpr) extends CExpr {
  def tp: PrimitiveType = BoolType
}

case class Lt(e1: CExpr, e2: CExpr) extends CExpr {
  def tp: PrimitiveType = BoolType
}

case class Lte(e1: CExpr, e2: CExpr) extends CExpr{
  def tp: PrimitiveType = BoolType
}

case class Gt(e1: CExpr, e2: CExpr) extends CExpr {
  def tp: PrimitiveType = BoolType
}

case class Gte(e1: CExpr, e2: CExpr) extends CExpr {
  def tp: PrimitiveType = BoolType
}

case class And(e1: CExpr, e2: CExpr) extends CExpr {
  def tp: PrimitiveType = BoolType
}

case class Not(e1: CExpr) extends CExpr{
  def tp: PrimitiveType = BoolType
}

case class Or(e1: CExpr, e2: CExpr) extends CExpr{
  def tp: PrimitiveType = BoolType
}

case class Project(e1: CExpr, field: String) extends CExpr { self =>
  def tp: Type = e1.tp match {
    case t:RecordCType => t.attrTps(field) 
    case t:TTupleType => field match {
      case "_1" => t(0)
      case "_2" => t(1)
      case  _ => t(field.toInt)
    }
    case t:LabelType => t(field)
    case t:TupleDictCType => t(field)
    case t:BagDictCType => t(field)
    case _ => sys.error("unsupported projection index "+self)
  }

}

case class If(cond: CExpr, e1: CExpr, e2: Option[CExpr]) extends CExpr {
  assert(cond.tp == BoolType)
  val tp: Type = e1.tp
}

case class Merge(e1: CExpr, e2: CExpr) extends CExpr {
  def tp: BagCType = e1.tp.asInstanceOf[BagCType]  //disjoint types?
}

case class Comprehension(e1: CExpr, v: Variable, p: CExpr, e: CExpr) extends CExpr {
  def tp: Type = e.tp match {
    case t:RecordCType => BagCType(t)
    case t => t //primitive
  }
}

case class CDeDup(e1: CExpr) extends CExpr{
  def tp: BagCType = e1.tp.asInstanceOf[BagCType]
}

// replace all occurences of x with e1 in e1
case class Bind(x: CExpr, e1: CExpr, e: CExpr) extends CExpr {
  def tp: Type = e.tp
  override def wvars = e1 match {
    case v:Variable => e.wvars :+ v
    case _ => e.wvars :+ x.asInstanceOf[Variable]
  }
}

case class CNamed(name: String, e: CExpr) extends CExpr {
  def tp: Type = e.tp
}

case class LinearCSet(exprs: List[CExpr]) extends CExpr {
  def tp: Type = EmptyCType
  def getTypeMap: Map[Type, String] = exprs.map{ e => e match {
    case CNamed(n, e1) => (e1.tp.asInstanceOf[BagCType].tp -> s"Rec$n")
    case e1 => (e1.tp.asInstanceOf[BagCType].tp -> s"Record${Variable.newId}")
  }}.toMap
}

/**
  * Shred extensions
  * Labels are just Records, ie. Label(x: x, y: y) 
  * Extract nodes are just projections on the attributes of these labels
  * ie. a subquery "for label in domain union x" 
  * is represented as "for label in domain union label.x"
  */

case class CLookup(lbl: CExpr, dict: CExpr) extends CExpr {
  def tp: BagCType = dict.tp.asInstanceOf[BagDictCType].flat
}

case object EmptyCDict extends CExpr {
  def tp: TDict = EmptyDictCType
}

case class BagCDict(lbl: CExpr, flat: CExpr, dict: CExpr) extends CExpr {
  def tp: BagDictCType = 
    BagDictCType(BagCType(TTupleType(List(lbl.tp, flat.tp))), dict.tp.asInstanceOf[TTupleDict])
  def apply(n: String) = n match {
    case "lbl" => lbl
    case "flat" => flat
    //case "_1" => List(lbl, flat)
    case "_2" => dict
  }
  //def lambda = Tuple(List(lbl, flat))
  //def _1 = flat
  def _2 = dict
}

case class TupleCDict(fields: Map[String, CExpr]) extends CExpr {
  def tp: TupleDictCType = TupleDictCType(fields.map(f => f._1 -> f._2.tp.asInstanceOf[TDict]))
  def apply(n: String) = fields(n)
}

object TupleCDict {
  def apply(fields: (String, CExpr)*): TupleCDict = TupleCDict(Map(fields:_*))
}

// turn into a comprehension?
case class DictCUnion(d1: CExpr, d2: CExpr) extends CExpr {
  def tp: BagDictCType = d1.asInstanceOf[BagDictCType]
}

/**
  * Algebra data operators for creating plans from WMCC
  * These are defined as an extension off of the WMCC nodes 
  * since WMCC nodes are used to represent inputs, constants, tuples, bags, etc.
  */

case class Select(x: CExpr, v: Variable, p: CExpr) extends CExpr {
  def tp: Type = x.tp
  // def tpMap: Map[Variable, Type] = Map(v -> x.asInstanceOf[BagCType].tp)
  override def wvars = List(v)
  // todo equality should support filter
  override def equals(that: Any): Boolean = that match {
    case Select(x1, v1, p1) if (x1 == x) => true
    case _ => false
  }
}

case class Reduce(e1: CExpr, v: List[Variable], e2: CExpr, p: CExpr) extends CExpr {
  def tp: Type = e2.tp match {
    case t:RecordCType => BagCType(t)
    case t => t
  }
  // def tpMap: Map[Variable, Type] = e1.tpMap // head of plan
  override def wvars = e1.wvars
}

// { (v1, v2) | v1 <- e1, v2 <- e2(v1), p((v1, v2)) } 
case class Unnest(e1: CExpr, v1: List[Variable], e2: CExpr, v2: Variable, p: CExpr) extends CExpr {
  def tp: Type = BagCType(TTupleType(List(e1.tp.asInstanceOf[BagCType].tp, v2.tp)))
  // def tpMap: Map[Variable, Type] = e1.tp ++ (v2 -> v2.tp)
  override def wvars = e1.wvars :+ v2
  override def equals(that: Any): Boolean = that match {
    case Unnest(e11, v11, e21, v21, p1) if (e1 == e11 && e21 == e2) => true
    case _ => false
  }
}

case class OuterUnnest(e1: CExpr, v1: List[Variable], e2: CExpr, v2: Variable, p: CExpr) extends CExpr {
  def tp: Type = BagCType(TTupleType(List(e1.tp.asInstanceOf[BagCType].tp, v2.tp)))
  // def tpMap: Map[Variable, Type] = e1.tp ++ (v2 -> v2.tp)
  override def wvars = e1.wvars :+ v2
  override def equals(that: Any): Boolean = that match {
    case OuterUnnest(e11, v11, e21, v21, p1) if (e1 == e11 && e21 == e2) => true
    case _ => false
  }
}

case class Nest(e1: CExpr, v1: List[Variable], f: CExpr, e: CExpr, v2: Variable, p: CExpr) extends CExpr {
  def tp: Type = BagCType(v2.tp) // check 
  // def tpMap: Map[Variable, Type] = e1.tp ++ (v2 -> v2.tp)
  override def wvars = { 
    val uvars = f match {
      case Bind(v1, t @ Tuple(fs), v2) => fs
      case Tuple(fs) => fs
      case v:Variable => List(v)
      case _ => sys.error(s"unsupported $f")
    }
    e1.wvars.filter(uvars.contains(_)) :+ v2
  }
}

case class NestBlock(e1: CExpr, v1: List[Variable], f: CExpr, e: CExpr, v2: Variable, p: CExpr, e2: CExpr, v3: Variable) extends CExpr {
  def tp: Type = BagCType(v2.tp)
}

case class OuterJoin(e1: CExpr, e2: CExpr, v1: List[Variable], p1: CExpr, v2: Variable, p2: CExpr) extends CExpr {
  def tp: BagCType = BagCType(TTupleType(List(e1.tp.asInstanceOf[BagCType].tp, v2.tp)))
  override def wvars = {
    e1.wvars :+ v2
  }
}

// unnests an inner bag, without unnesting before a downstream join
case class Lookup(e1: CExpr, e2: CExpr, v1: List[Variable], p1: CExpr, v2: Variable, p2: CExpr, p3: CExpr) extends CExpr {
  def tp:BagCType = BagCType(TTupleType(List(e1.tp.asInstanceOf[BagCType].tp, v2.tp)))
  override def wvars = e1.wvars :+ v2
}

case class Join(e1: CExpr, e2: CExpr, v1: List[Variable], p1: CExpr, v2: Variable, p2: CExpr) extends CExpr {
  def tp: BagCType = BagCType(TTupleType(List(e1.tp.asInstanceOf[BagCType].tp, v2.tp)))
  override def wvars = e1.wvars :+ v2
}

case class Variable(name: String, override val tp: Type) extends CExpr { self =>
  
  // equals with a label check
  // check if deprecated (was used in unnesting before labels were represented as records)
  def lequals(that: CExpr): Boolean = that match {
    case that: Variable => this.equals(that)
    //case Bind(v, e1, e2) => 
    case Project(v, f) => this.lequals(v)
    case t if that.tp.isInstanceOf[LabelType] =>
      that.tp.asInstanceOf[LabelType].attrTps.keys.toList.contains(this.name)
    case t if that.tp.isInstanceOf[RecordCType] => // new label representation
      that.tp.asInstanceOf[RecordCType].attrTps.keys.toList.contains(this.name)
    case _ => false  
  }

  override def hashCode: Int = (name, tp).hashCode()
  def quote: String = self.name

}

object Variable {
  private var lastId = 1
  def fresh(tp: Type): Variable = {
    val id = newId()
    Variable(s"x$id", tp)
  }
  def newId(): Int = {
    val id = lastId
    lastId += 1
    id
  }
}


