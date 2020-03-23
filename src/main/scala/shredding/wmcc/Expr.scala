package shredding.wmcc

import shredding.core._

/**
  * Weighted Monad Comprehension Calculus (WMCC) expression nodes 
  * includes WMCC nodes for shredding extensions, and 
  * algebra data operators for translating WMCC to plans 
  */

trait CExpr {
  def tp: Type
  def nullValue: CExpr = tp match {
    case IntType => Constant(-1)
    case DoubleType => Constant(-1.0)
    case _ => Null
  }
  def zero: String = tp match {
    case IntType => "0"
    case DoubleType => "0.0"
    case _ => "Iterable()"
  }
  def wvars: List[Variable] = List() // remove this, only using for printing
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

case object Null extends CExpr {
  def tp: Type = EmptyCType
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

case class Label(fields: Map[String, CExpr]) extends CExpr{
  def tp: LabelType = LabelType(fields.map(f => f._1 -> f._2.tp))
  def apply(n: String) = fields(n)
}

case class Record(fields: Map[String, CExpr]) extends CExpr{
  def tp: RecordCType = RecordCType(fields.map(f => f._1 -> f._2.tp))
  def apply(n: String) = fields(n)
  def project(n: List[String]) = Record(fields.filter(f => n.contains(f._1)))
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

case class Multiply(e1: CExpr, e2: CExpr) extends CExpr{
  def tp: PrimitiveType = (e1.tp, e2.tp) match {
    case (DoubleType, _) => DoubleType
    case (_, DoubleType) => DoubleType
    case (IntType, _) => IntType
    case _ => sys.error("type not supported")
  }
}

case class Project(e1: CExpr, field: String) extends CExpr { self =>
  def tp: Type = e1.tp match {
    case t:RecordCType => t.attrTps get field match {
      case Some(fi) => fi
      case _ => sys.error(s"$field not found in $t")
    }
    case t @ TTupleType(List(EmptyCType, RecordCType(fs))) if ( field != "_1" && field != "_2") => fs(field)
    case t:TTupleType => field match {
      case "_1" => t(0)
      case "_2" => t(1)
      case  _ => println(t); t(field.toInt)
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

trait CombineOp extends CExpr {
  def tp: BagCType
  def e1: CExpr
  def v1: Variable
  def keys: List[String]
  def values: List[String]
}

case class CReduceBy(e1: CExpr, v1: Variable, keys: List[String], values: List[String]) extends CExpr with CombineOp {
  val e1Tp: RecordCType = v1.tp.asInstanceOf[RecordCType]
  val keysTp: RecordCType = RecordCType(keys.map(n => n -> e1Tp(n)).toMap)
  val valuesTp: RecordCType = RecordCType(values.map(n => n -> e1Tp(n)).toMap)
  def tp: BagCType = BagCType(RecordCType(keysTp.attrTps ++ valuesTp.attrTps))
}

case class CGroupBy(e1: CExpr, v1: Variable, keys: List[String], values: List[String]) extends CExpr with CombineOp {
  val e1Tp: RecordCType = v1.tp.asInstanceOf[RecordCType]
  val keysTp: RecordCType = RecordCType(keys.map(n => n -> e1Tp(n)).toMap)
  val valuesTp: BagCType = BagCType(RecordCType(values.map(n => n -> e1Tp(n)).toMap))
  def tp: BagCType = BagCType(RecordCType(keysTp.attrTps ++ Map("_2" -> valuesTp)))
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
  def tp: BagCType = dict.tp.flat
  //def tp: BagCType = dict.tp.asInstanceOf[BagDictCType].flatTp
}

case object EmptyCDict extends CExpr {
  def tp: TDict = EmptyDictCType
}

case class BagCDict(lblTp: LabelType, flat: CExpr, dict: CExpr) extends CExpr {
  def tp: BagDictCType = 
    BagDictCType(BagCType(TTupleType(List(lblTp, flat.tp))), dict.tp.asInstanceOf[TTupleDict])
  def apply(n: String) = n match {
//    case "lbl" => lbl
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

case class Select(x: CExpr, v: Variable, p: CExpr, e: CExpr) extends CExpr {
  def tp: Type = x.tp
  override def wvars = List(v)
}

case class Reduce(e1: CExpr, v: List[Variable], e2: CExpr, p: CExpr) extends CExpr {
  def tp: Type = e2.tp match {
    case t:RecordCType => BagCType(t)
    case t:TTupleType => BagCType(t)
    case t => t
  }
  override def wvars = e1.wvars
}

// { (v1, v2) | v1 <- e1, v2 <- e2(v1), p((v1, v2)) } 
case class Unnest(e1: CExpr, v1: List[Variable], e2: CExpr, v2: Variable, p: CExpr, value: CExpr) extends CExpr {
  val bagproj = e2 match {
    case Project(_, field) => field
    case Bind(_, Project(_, field), _) => field
    case _ => ???
  }
  def tp: Type = e1.tp match {
    case btp:BagDictCType => 
      val ntp = RecordCType(btp.flatTp.tp.asInstanceOf[TTupleType].attrTps.last.asInstanceOf[BagCType].tp.asInstanceOf[RecordCType].attrTps - bagproj)
      BagCType(TTupleType(List(ntp, v2.tp)))
    case btp:BagCType => 
      val ntp = RecordCType(btp.tp.asInstanceOf[TTupleType].attrTps.last.asInstanceOf[BagCType].tp.asInstanceOf[RecordCType].attrTps - bagproj)
      BagCType(TTupleType(List(ntp, v2.tp)))
    case _ => ???
  }
  // def tpMap: Map[Variable, Type] = e1.tp ++ (v2 -> v2.tp)
  override def wvars = e1.wvars :+ v2
  // override def equals(that: Any): Boolean = that match {
  //   case Unnest(e11, v11, e21, v21, p1) => e21 == e2
  //   case OuterUnnest(e11, v11, e21, v21, p1) => e21 == e2
  //   case e => false
  // }
}

case class OuterUnnest(e1: CExpr, v1: List[Variable], e2: CExpr, v2: Variable, p: CExpr, value: CExpr) extends CExpr {
  def tp: Type = e1.tp match {
    case btp:BagDictCType => 
      BagCType(TTupleType(List(btp.flatTp.tp, v2.tp)))
    case btp:BagCType => 
      BagCType(TTupleType(List(btp.tp, v2.tp)))
    case _ => ???
  }
  override def wvars = e1.wvars :+ v2
}

case class Nest(e1: CExpr, v1: List[Variable], f: CExpr, e: CExpr, v2: Variable, p: CExpr, g: CExpr, distinctKeys: Boolean) extends CExpr {
  def tp: Type = BagCType(v2.tp)
  // only using this for printing, consider removing
  override def wvars = { 
    val uvars = f match {
      case Bind(v1, t @ Tuple(fs), v2) => fs
      case Tuple(fs) => fs
      case v:Variable => List(v)
      case Bind(v1, Project(v2, f), v3) => List(v2)
      case Record(fs) => Nil
      case _ => sys.error(s"unsupported $f")
    }
    e1.wvars.filter(uvars.contains(_)) :+ v2
  }
}

case class OuterJoin(e1: CExpr, e2: CExpr, v1: List[Variable], p1: CExpr, v2: Variable, p2: CExpr, proj1: CExpr, proj2: CExpr) extends CExpr {
  // def tp: BagCType = BagCType(TTupleType(List(e1.tp.asInstanceOf[BagCType].tp, v2.tp)))
  def tp: BagCType = BagCType(TTupleType(List(proj1.tp, proj2.tp)))
  override def wvars = {
    e1.wvars :+ v2
  }
}

// unnests an inner bag, without unnesting before a downstream join
case class Lookup(e1: CExpr, e2: CExpr, v1: List[Variable], p1: CExpr, v2: Variable, p2: CExpr, p3: CExpr) extends CExpr {

  val valueBagType = p3 match { 
    // lookup used in unshredding
    case Variable(_, TTupleType(fs)) if fs.size == 2 => fs.last
    // lookup iterator
    case _ => v2.tp match {
      case TTupleType(fs) => fs.last.asInstanceOf[BagCType].tp
      case t => t
    }
  }

  def tp:BagCType = e1.tp match {
    case BagCType(lbl) => BagCType(TTupleType(List(lbl, valueBagType)))
    // case btp:BagDictCType => 
    //   BagCType(TTupleType(List(btp.flatTp.tp, valueBagType)))
    case _ => sys.error(s"unsupported ${e1.tp}")
  }
  override def wvars = e1.wvars :+ v2
}

// omitting filter for now
// and nulls
case class CoGroup(e1: CExpr, e2: CExpr, v1: List[Variable], v2: Variable, k1: CExpr, 
  k2: CExpr, value: CExpr) extends CExpr {
  def tp:BagCType = e1.tp match {
    case BagCType(tup) => BagCType(TTupleType(List(tup, BagCType(value.tp))))
    case _ => ???
  }
}

case class OuterLookup(e1: CExpr, e2: CExpr, v1: List[Variable], p1: CExpr, v2: Variable, p2: CExpr, p3: CExpr) extends CExpr {
  def tp:BagCType = e1.tp match {
    case BagCType(tup) => BagCType(TTupleType(List(tup, v2.tp)))
    case btp:BagDictCType => BagCType(TTupleType(List(btp.flat, v2.tp)))
    case _ => ???
  }
  override def wvars = e1.wvars :+ v2
}

case class Join(e1: CExpr, e2: CExpr, v1: List[Variable], p1: CExpr, v2: Variable, p2: CExpr, proj1: CExpr, proj2: CExpr) extends CExpr {
  def tp: BagCType = BagCType(TTupleType(List(proj1.tp, proj2.tp)))
  // e1.tp match {
  //   case btp:BagCType => BagCType(TTupleType(List(btp.tp, v2.tp)))
  //   case BagDictCType(flat, tdict) => BagCType(TTupleType(List(flat.tp, v2.tp)))
  //   case _ => ???
  // } 
  override def wvars = e1.wvars :+ v2
}


case class LocalAgg(iter: CExpr, key: CExpr, value: CExpr, filt: CExpr) extends CExpr {
  def tp: BagCType = BagCType(TTupleType(List(key.tp, value.tp)))
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
  
  // variable is referenced more than just for a join condition in a lookup
  // used to avoid joins on domains
  // for now collects a set of expressions for which this variable is referenced
  // def isReferenced(that: CExpr): List[CExpr] = that match {
  //   case Reduce(e1, v, e2, p) => isReferenced(e1) ++ isReferenced(e2) ++ isReferenced(p)
  //   case Nest(e1, v1, f, e, v2, p, g) =>
  //     isReferenced(e1) ++ isReferenced(f) ++ isReferenced(e) ++ isReferenced(p) ++ isReferenced(g)
  //   case Join(e1, e2, v1, p1, v2, p2) =>
  //     isReferenced(e1) ++ isReferenced(e2) ++ isReferenced(p1) ++ isReferenced(p2)
  //   case Tuple(fs) => fs.flatMap(isReferenced(_))
  //   case r @ Record(_) => r.fields.flatMap(m => isReferenced(m._2)).toList
  //   case Equals(e1, e2) => if (isReferenced(e1).nonEmpty || isReferenced(e2).nonEmpty) List(that) else Nil
  //   case Project(v:Variable, f) if v.name == this.name => List(that)
  //   case Project(v, f) => isReferenced(v) match {
  //     case Nil => Nil
  //     case l => List(that)
  //   }
  //   case v @ Variable(n, tp) if n == this.name => List(v)
  //   case _ => Nil
  // }
  override def hashCode: Int = (name, tp).hashCode()
  def quote: String = self.name

}

object Variable {
  private var lastId = 1
  def fresh(tp: Type): Variable = {
    val id = newId()
    Variable(s"x$id", tp)
  }
  def fresh(key: Type, value: Type): Variable = {
    fresh(TTupleType(List(key, value)))
  }
  def freshFromBag(tp: Type): Variable = {
    val id = newId()
    tp match {
      case BagDictCType(BagCType(TTupleType(List(EmptyCType, BagCType(tup)))), tdict) => Variable(s"x$id", tup)
      case BagCType(TTupleType(List(EmptyCType, BagCType(tup)))) =>  Variable(s"x$id", tup)
      case BagDictCType(flat, dict) => Variable(s"x$id",flat.tp)
      case BagCType(tup) => Variable(s"x$id", tup)
      case _ => Variable(s"x$id", tp)
    }
  }
  def fresh(n: String = "x"): String = s"$n${newId()}"
  def newId(): Int = {
    val id = lastId
    lastId += 1
    id
  }
}

/** Optimizer Extensions **/

trait Extensions {

  def substitute(e: CExpr, vold: Variable, vnew: Variable): CExpr = e match {
    case Record(fs) => Record(fs.map{ 
      case (attr, e2) => attr -> substitute(e2, vold, vnew)})
    case Project(v, f) => 
      Project(substitute(v, vold, vnew), f)
    case v @ Variable(_,_) => 
      if (v == vold) Variable(vnew.name, v.tp) else v
    case _ => e
  }

  def fapply(e: CExpr, funct: PartialFunction[CExpr, CExpr]): CExpr = 
    funct.applyOrElse(e, (ex: CExpr) => ex match {
      case Reduce(d, v, f, p) => Reduce(fapply(d, funct), v, f, p)
      case Nest(e1, v1, f, e, v, p, g, dk) => Nest(fapply(e1, funct), v1, f, e, v, p, g, dk)
      case Unnest(e1, v1, f, v2, p, value) => Unnest(fapply(e1, funct), v1, f, v2, p, value)
      case OuterUnnest(e1, v1, f, v2, p, value) => OuterUnnest(fapply(e1, funct), v1, f, v2, p, value)
      case Join(e1, e2, v1, p1, v2, p2, proj1, proj2) => Join(fapply(e1, funct), fapply(e2, funct), v1, p1, v2, p2, proj1, proj2)
      case OuterJoin(e1, e2, v1, p1, v2, p2, proj1, proj2) => OuterJoin(fapply(e1, funct), fapply(e2, funct), v1, p1, v2, p2, proj1, proj2)
      case Lookup(e1, e2, v1, p1, v2, p2, p3) => Lookup(fapply(e1, funct), e2, v1, p1, v2, p2, p3)
      case CDeDup(e1) => CDeDup(fapply(e1, funct))
      case CNamed(n, e1) => CNamed(n, fapply(e1, funct))
      case LinearCSet(es) => LinearCSet(es.map(e1 => fapply(e1, funct)))
      case _ => ex
    })
}

