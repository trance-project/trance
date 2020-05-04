package shredding.plans

import shredding.core._

/**
  * Plan Operators and Calculus
  */

trait CExpr {

  def tp: Type

  // experimental for batch
  // style unnesting
  def ftp: Type = tp

  def nullValue: CExpr = tp match {
    case IntType => Constant(-1)
    case DoubleType => Constant(-1.0)
    case _ => Null
  }
  def wvars: List[Variable] = List()
}

case class InputRef(data: String, tp: Type) extends CExpr 

// Deprecated
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

case object Index extends CExpr {
  def tp: Type = LongType
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

// Deprecated
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
    case BagCType(tup) => e.tp
    case t => BagCType(t)
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
  def valuesTp: Type
}

case class CReduceBy(e1: CExpr, v1: Variable, keys: List[String], values: List[String]) extends CExpr with CombineOp {
  val e1Tp: RecordCType = v1.tp match {
    case rt:RecordCType => rt
    case _ => sys.error(s"unsupported type ${v1.tp}")
  }
  val keysTp: RecordCType = RecordCType(keys.map(n => n -> e1Tp(n)).toMap)
  val valuesTp: RecordCType = RecordCType(values.map(n => n -> e1Tp(n)).toMap)
  def tp: BagCType = e1Tp.attrTps get "_1" match {
    case Some(TTupleType(fs)) => 
      BagCType(TTupleType(fs :+ RecordCType((e1Tp.attrTps - "_1") ++ valuesTp.attrTps)))
    case _ => 
      BagCType(RecordCType(keysTp.attrTps ++ valuesTp.attrTps))
  }

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

/** Extensions for intermediate NRC used in shredding */

case class CLookup(lbl: CExpr, dict: CExpr) extends CExpr {
  def tp: BagCType = dict.tp match {
    case MatDictCType(lbl, dict) => dict
    case _ => sys.error(s"not supported ${dict.tp}")
  }
}

case class FlatDict(e: CExpr) extends CExpr {
  def tp: BagCType = e.tp match {
    case MatDictCType(lbl, BagCType(RecordCType(ms))) => 
      BagCType(RecordCType(ms + ("_1" -> lbl)))
    case _ => sys.error(s"unsupported type ${e.tp}")
  }
}

case class GroupDict(e: CExpr) extends CExpr {

  def tp: MatDictCType = e.tp match {

    case BagCType(RecordCType(ms)) => 
      val lbl = ms get "_1" match {
        case Some(l:LabelType) => l
        case _ => sys.error("invalid bag")
      }
      MatDictCType(lbl, BagCType(RecordCType(ms - "_1")))

    case BagCType(TTupleType(ms)) if ms.size == 2 =>
      val lbl = ms.head match {
        case l:LabelType => l
        case _ => sys.error("invalid bag")
      }
      val bag = ms.last match {
        case bt:BagCType => bt
        case bt => BagCType(bt)
      }
      MatDictCType(lbl, bag)

    case _ => sys.error(s"unsupported type ${e.tp}")
  }
}

case object EmptyCDict extends CExpr {
  def tp: TDict = EmptyDictCType
}

// Deprecate all these expressions?
case class BagCDict(lblTp: LabelType, flat: CExpr, dict: CExpr) extends CExpr {
  def tp: BagDictCType = 
    BagDictCType(BagCType(TTupleType(List(lblTp, flat.tp))), dict.tp.asInstanceOf[TTupleDict])
  def apply(n: String) = n match {
    case "flat" => flat
    case "_2" => dict
  }
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

/** Operators of the plan language **/ 

case class Select(x: CExpr, v: Variable, p: CExpr, e: CExpr) extends CExpr {
  def tp: Type = e.tp match {
    case rt:RecordCType => BagCType(rt)
    case _ => x.tp
  }

  override def ftp: Type = e.tp match {
    case RecordCType(ms) => ms get "_1" match {
      case Some(TTupleType(ls)) => 
        val inner = ls.flatMap{l => l match {
          case RecordCType(ms1) => ms1 
          case _ => ???
        }}.toMap
        BagCType(RecordCType(inner ++ (ms - "_1")))
      case _ => BagCType(RecordCType(ms))
    }
    case _ => tp
  }

  override def wvars = List(v)
}

case class Reduce(e1: CExpr, v: List[Variable], e2: CExpr, p: CExpr) extends CExpr {
  def tp: Type = e2.tp match {
    case t:BagCType => t
    case t => BagCType(t) 
  }

  override def ftp: Type = e2.ftp match {
    case RecordCType(ms) => ms get "_1" match {
      case Some(a) => 
        BagCType(RecordCType(Map("_1" -> a) ++
          (ms - "_1").map{ case (attr, expr) => (attr, OptionType(expr))}))
      case _ => BagCType(RecordCType(ms))
    }
    case _ => ???
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
    case BagCType(TTupleType(fs)) => 
      val ntp = fs.last match {
        case BagCType(RecordCType(ms)) => RecordCType((ms - bagproj))
        case RecordCType(ms) => RecordCType((ms - bagproj))
        case _ => ???
      }
      BagCType(TTupleType(List(ntp, value.tp)))
    case BagCType(RecordCType(fs)) => 
      BagCType(TTupleType(List(RecordCType(fs - bagproj), value.tp)))
    case _ => ???
  }
  override def wvars = e1.wvars :+ v2
}

case class OuterUnnest(e1: CExpr, v1: List[Variable], e2: CExpr, v2: Variable, p: CExpr, value: CExpr) extends CExpr {
  
  val bagproj = e2 match {
    case Project(_, field) => field
    case Bind(_, Project(_, field), _) => field
    // local agg
    case CReduceBy(Project(_, field), _, _, _) => field
    case _ => ???
  }

  def tp: Type = e1.tp match {
    case BagCType(TTupleType(fs)) => 
      val ntp = fs.last match {
        case BagCType(RecordCType(ms)) => TTupleType(fs.dropRight(1) :+ RecordCType((ms - bagproj)))
        case RecordCType(ms) => TTupleType(fs.dropRight(1) :+ RecordCType((ms - bagproj)))
        case _ => ???
      }
      BagCType(TTupleType(List(ntp, value.tp)))
    case BagCType(RecordCType(fs)) => 
      val unnestedBag = value.tp 
      val dropOld = RecordCType((fs - bagproj))
      BagCType(TTupleType(List(RecordCType((fs - bagproj)), value.tp)))
    case _ => ???
  }

  override def ftp: BagCType = e1.tp match {
    case BagCType(RecordCType(fs)) => 
      val unnestedBag = value.tp match {
        case RecordCType(ls) => ls.map(m => m._1 -> OptionType(m._2))
        case _ => ???
      }
      val dropOld = (fs - bagproj)
      BagCType(RecordCType(dropOld ++ unnestedBag))
    case _ => sys.error(s"not supported ${e1.tp}")
  }

  override def wvars = e1.wvars :+ v2
}

case class Nest(e1: CExpr, v1: List[Variable], f: CExpr, e: CExpr, v2: Variable, p: CExpr, g: CExpr, distinctKeys: Boolean) extends CExpr {
  
  def tp: Type = BagCType(v2.tp)

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
  
  def tp: BagCType = BagCType(TTupleType(List(proj1.tp, proj2.tp)))

  override def ftp: BagCType = (e1.ftp, e2.ftp) match {
    case (BagCType(RecordCType(ms1)), BagCType(RecordCType(ms2))) => 
      BagCType(RecordCType(ms1 ++ ms2.map(m => m._1 -> OptionType(m._2))))
    case _ => sys.error(s"issue with ${proj1.ftp} ${proj2.ftp}")
  }

  override def wvars = e1.wvars :+ v2
}

// unnests an inner bag, without unnesting before a downstream join
case class Lookup(e1: CExpr, e2: CExpr, v1: List[Variable], p1: CExpr, v2: Variable, p2: CExpr, p3: CExpr) extends CExpr {

  val valueBagType = p3 match { 
    case Variable(_, TTupleType(fs)) if fs.size == 2 => fs.last
    case _ => v2.tp match {
      case TTupleType(fs) => fs.last.asInstanceOf[BagCType].tp
      case t => t
    }
  }

  def tp:BagCType = e1.tp match {
    case BagCType(lbl) => BagCType(TTupleType(List(lbl, valueBagType)))
    case _ => sys.error(s"unsupported ${e1.tp}")
  }
  override def wvars = e1.wvars :+ v2
}

case class CoGroup(e1: CExpr, e2: CExpr, v1: List[Variable], v2: Variable, k1: CExpr, k2: CExpr, value: CExpr) extends CExpr {
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
  override def wvars = e1.wvars :+ v2
}

case class LocalAgg(iter: CExpr, key: CExpr, value: CExpr, filt: CExpr) extends CExpr {
  def tp: BagCType = BagCType(TTupleType(List(key.tp, value.tp)))
}

case class Variable(name: String, override val tp: Type) extends CExpr { self =>
  
  // equals with a label check
  def lequals(that: CExpr): Boolean = that match {
    case that: Variable => this.equals(that)
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


