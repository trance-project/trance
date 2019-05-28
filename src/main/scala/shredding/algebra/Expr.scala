package shredding.algebra

import shredding.core._

sealed trait CExpr {
  def tp: Type
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

case object CUnit extends CExpr {
  def tp: Type = EmptyCType
}

case class Record(fields: Map[String, CExpr]) extends CExpr{
  def tp: RecordCType = RecordCType(fields.map(f => f._1 -> f._2.tp))
  def apply(n: String) = fields(n)
}

case class Tuple(fields: CExpr*) extends CExpr {
  def tp: TTupleType = TTupleType(fields.map(_.tp):_*)
  def apply(n: Int) = fields(n)
}

object Tuple {
  def apply(fields: Seq[CExpr]): CExpr = Tuple(fields:_*)
}

case class KVTuple(e1: CExpr, e2: CExpr) extends CExpr{
  def tp: KVTupleCType = KVTupleCType(e1.tp, e2.tp)
  def apply(n: String) = n match {
    case "key" => e1
    case "0" => e1
    case "value"  => e2
    case "1" => e2
  }
  def _1 = e1
  def _2 = e2
}

case class Equals(e1: CExpr, e2: CExpr) extends CExpr {
  def tp: PrimitiveType = BoolType
}

case class NEquals(e1: CExpr, e2: CExpr) extends CExpr{
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
    case t:KVTupleCType => t(field)
    case t:LabelType => t(field)
    case t:TupleDictCType => t(field)
    case t:BagDictCType => t(field)
    case _ => sys.error("unsupported projection index "+self)
  }
}

case class If(cond: CExpr, e1: CExpr, e2: Option[CExpr]) extends CExpr {
  assert(cond.tp == BoolType)
  // disjoint types?
  val tp: Type = e1.tp
}

case class Merge(e1: CExpr, e2: CExpr) extends CExpr {
  def tp: BagCType = e1.tp.asInstanceOf[BagCType]  //disjoint types?
}

// reorder variables, confusing to create
case class Comprehension(e1: CExpr, v: Variable, p: CExpr, e: CExpr) extends CExpr {
  def tp: Type = e.tp match {
    case t:RecordCType => BagCType(t)
    case t:KVTupleCType => BagCType(t)
    case t => t //primitive
  }
}

case class CDeDup(e1: CExpr) extends CExpr{
  def tp: BagCType = e1.asInstanceOf[BagCType]
}

// replace all occurences of x with e1 in e1
case class Bind(x: CExpr, e1: CExpr, e: CExpr) extends CExpr {
  def tp: Type = e.tp
}

case class CNamed(name: String, e: CExpr) extends CExpr {
  def tp: Type = e.tp
}

case class LinearCSet(exprs: List[CExpr]) extends CExpr {
  def tp: Type = EmptyCType // arbitrary type for a set of expressions
}

/**
  * Shred extensions
  */

case class Label(id: Int, vars: Map[String, CExpr]) extends CExpr {
  val tp: LabelType = LabelType(vars.map(f => f._1 -> f._2.tp))
  def apply(n: String) = vars(n)
  override def equals(that: Any): Boolean = that match {
    case that: Label => this.id == that.id
    case _ => false
  }
  
  override def hashCode: Int = id.hashCode()
  def quote: String = s"Label${id}(${vars.map(f => f._1 +"->"+f._2).mkString(",")})"
}

object Label {
  def apply(id: Int, vars: (String, CExpr)*): Label = Label(id, Map(vars:_*))
  private var lastId = 1
  def fresh(vars: Map[String, CExpr] = Map[String,CExpr]()): Label = {
    val id = lastId
    lastId += 1
    Label(id, vars)
  }
}

case class CLookup(lbl: CExpr, dict: CExpr) extends CExpr {
  def tp: BagCType = dict.tp.asInstanceOf[BagDictCType]._1
}

case object EmptyCDict extends CExpr {
  def tp: TDict = EmptyDictCType
}

case class BagCDict(lbl: CExpr, flat: CExpr, dict: CExpr) extends CExpr {
  def tp: BagDictCType = 
    BagDictCType(BagCType(KVTupleCType(lbl.tp, flat.tp)), dict.tp.asInstanceOf[TTupleDict])
  def apply(n: String) = n match {
    case "lbl" => lbl
    case "flat" => flat
    case "tupleDict" => dict
    case "0" => KVTuple(lbl, flat)
    case "1" => dict
  }
  def lambda = KVTuple(lbl, flat)
  def _1 = flat
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
  * Algebra extensions
  */

case class Select(x: CExpr, v: Variable, p: CExpr) extends CExpr {
  def tp: Type = x.tp
}
case class Reduce(e1: CExpr, v: List[Variable], e2: CExpr, p: CExpr) extends CExpr {
  def tp: Type = e2.tp match {
    case t:RecordCType => BagCType(t)
    case t => t
  }
}

// { (v1, v2) | v1 <- e1, v2 <- e2(v1), p((v1, v2)) } 
case class Unnest(e1: CExpr, v1: List[Variable], e2: CExpr, v2: Variable, p: CExpr) extends CExpr {
  def tp: Type = BagCType(KVTupleCType(e1.tp.asInstanceOf[BagCType].tp, e2.tp.asInstanceOf[BagCType].tp))
}

case class OuterUnnest(e1: CExpr, v1: List[Variable], e2: CExpr, v2: Variable, p: CExpr) extends CExpr {
  def tp: Type = BagCType(KVTupleCType(e1.tp.asInstanceOf[BagCType].tp, e2.tp.asInstanceOf[BagCType].tp))
}

case class Nest(e1: CExpr, v1: List[Variable], f: CExpr, e: CExpr, v2: Variable, p: CExpr) extends CExpr {
  def tp: Type = BagCType(v2.tp) // KVTupleCType(et.tp, BagType(t.tp))
}

case class OuterJoin(e1: CExpr, e2: CExpr, v1: List[Variable], p1: CExpr, v2: Variable, p2: CExpr) extends CExpr {
  def tp: BagCType = e1.tp match {
    case BagCType(RecordCType(fs)) if fs.size == 1 && fs.contains("lbl") =>
      BagCType(e2.tp.asInstanceOf[BagCType].tp)
    case _ => BagCType(KVTupleCType(e1.tp, e2.tp))
  }
}

case class Join(e1: CExpr, e2: CExpr, v1: List[Variable], p1: CExpr, v2: Variable, p2: CExpr) extends CExpr {
  def tp: BagCType = BagCType(KVTupleCType(e1.tp, e2.tp))
}


case class Variable(name: String, override val tp: Type) extends CExpr { self =>
  override def equals(that: Any): Boolean = that match {
    case that: Variable => this.name == that.name && this.tp == that.tp
    case _ => false
  }

  override def hashCode: Int = (name, tp).hashCode()
  def quote: String = self.name

}

object Variable {
  private var lastId = 1
  def fresh(tp: Type): Variable = {
    val id = lastId
    lastId += 1
    Variable(s"x$id", tp)
  }
}
