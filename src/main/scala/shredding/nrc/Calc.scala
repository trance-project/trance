package shredding.nrc

/**
  * Comprehension calculus
  *
  */

sealed trait Calc { self =>
  def tp: Type 
  
  def substitute(e2: Calc, v: VarDef): Calc = self match {
    case y if self == e2 => Var(v)
    case t:Tup => Tup(t.fields.map(f => f._1 -> f._2.substitute(e2, v).asInstanceOf[AttributeCalc]))
    case Pred(e3) => Pred(e3.substitute(e2, v))  
    case _ => self
  }

  // gets an outermost expression in the body (C12)
  def isOutermost: Boolean = self match {
    case t:BagComp => true
    case _ => false
  }

  def isGenerator: Boolean = self match {
    case t:Generator => true
    case _ => false
  }

  def isEmptyGenerator: Boolean = self match {
    case Generator(x, z @ Zero()) => true
    case _ => false
  }

  def hasGenerator: Boolean = self match {
    case BagComp(e, qs) => qs.map(_.isGenerator).contains(true)
    case _ => false
  }

  def hasEmptyGenerator: Boolean = self match {
    case BagComp(e, qs) => qs.map(_.isEmptyGenerator).contains(true) 
    case _ => false
  }

  def normalize: Calc = self
  
  def qualifiers: List[Calc] = self match {
    case Generator(x, v @ Sng(e)) => List(Bind(x, e))
    case Generator(x, v @ BagComp(e, qs)) => qs.map(_.qualifiers).flatten :+ Bind(x, e)
    case _ => List(self)
  }

}

trait AttributeCalc extends Calc { self =>
  def tp: AttributeType 
  override def substitute(e2: Calc, v: VarDef): AttributeCalc = self.tp match {
    case y if self == e2 => Var(v).asInstanceOf[AttributeCalc]
    case t:BagType => self.asInstanceOf[BagCalc].substitute(e2, v)
    case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].substitute(e2, v)
    case _ => self
  }

  override def normalize: AttributeCalc = self

}

trait PrimitiveCalc extends AttributeCalc{ self =>
  
  def tp: PrimitiveType 
  
  override def substitute(e2: Calc, v: VarDef): PrimitiveCalc = self match {
    case y if self == e2 => Var(v).asInstanceOf[PrimitiveCalc]
    case Pred(e1) => Pred(e1.substitute(e2, v))
    case _ => self
  }

  override def normalize: PrimitiveCalc = self

}

trait BagCalc extends AttributeCalc { self =>
  
  def tp: BagType 
  
  override def substitute(e2: Calc, v: VarDef): BagCalc = self match {
    case y if self == e2 => Var(v).asInstanceOf[BagCalc]
    case IfStmt(cond, e3, e4) => e4 match {
      case Some(s) => IfStmt(cond.map(_.substitute(e2, v)), e3.substitute(e2, v), Option(s.substitute(e2, v)))
      case None => IfStmt(cond.map(_.substitute(e2, v)), e3.substitute(e2, v), None)
    }
    case Sng(e1) => Sng(e1.substitute(e2, v))
    case Merge(e1, e3) => Merge(e1.substitute(e2, v), e3.substitute(e2, v))
    case BagComp(e1, qs) => BagComp(e1.substitute(e2, v), qs.map(_.substitute(e2, v)))
    case _ => self 
  }

  override def normalize: BagCalc = self match {
    case BagComp(e, qs) => 
      if (self.hasEmptyGenerator) Zero()
      else BagComp(e, qs.map(_.qualifiers).flatten)
    case _ => self
  }
}

trait TupleCalc extends Calc { self => 
  def tp: TupleType 
  override def substitute(e2: Calc, v: VarDef): TupleCalc = self match {
    case y if self == e2 => Var(v).asInstanceOf[TupleCalc]
    case t:Tup => Tup(t.fields.map(f => f._1 -> f._2.substitute(e2, v)))
    case _ => self 
  }  

  override def normalize: TupleCalc = self

}

/**
  * Comprehension calculus constructs
  */

/**
  * Variable wrappers
  */    
trait Var extends Calc {
  def n: String
  def field: Option[String]
  def tp: Type
}
trait AttributeVar extends AttributeCalc with Var { def tp: AttributeType }
case class PrimitiveVar(n: String, field: Option[String], tp: PrimitiveType) extends PrimitiveCalc with AttributeVar
case class BagVar(n: String, field: Option[String], tp: BagType) extends BagCalc with AttributeVar
case class TupleVar(n: String, field: Option[String], tp: TupleType) extends TupleCalc with Var

object Var {
  def apply(d: VarDef): Var = d match {
    case v: PrimitiveVarDef => PrimitiveVar(v.n, None, v.tp)
    case v: BagVarDef => BagVar(v.n, None, v.tp)
    case v: TupleVarDef => TupleVar(v.n, None, v.tp)
    case _ => throw new IllegalArgumentException(s"cannot create Var(${d.n})")
  }

  def apply(d: Var, f: String): AttributeVar = d match {
    case v: TupleVar => v.tp.tps(f) match {
      case IntType => PrimitiveVar(v.n, Some(f), IntType)
      case StringType => PrimitiveVar(v.n, Some(f), StringType)
      case BoolType => PrimitiveVar(v.n, Some(f), BoolType)
      case t: BagType => BagVar(v.n, Some(f), t)
    }
    case _ => throw new IllegalArgumentException(s"cannot create Var(${d.n}, $f)")
  }

}

/**
  * Any of the base types (int, string, ...)
  */
case class Constant(x: String, tp: PrimitiveType) extends PrimitiveCalc


/**
  * Binding of a variable to an expression 
  * v <- {e} => v bind e (N6)
  * { e1 | ..., v <- {e | r }, .. } => {e1 | ..., r, v bind e, ... } (N8)
  */
trait Bind extends Calc
/**
  * Binding of a source to a variable denotate an iteration: v <- X 
  */
case class Generator(x: VarDef, e: BagCalc) extends BagCalc with Bind{
  assert(x.tp == e.tp.tp)
  val tp: BagType = e.tp
}
case class BindPrimitive(x: VarDef, e: PrimitiveCalc) extends PrimitiveCalc with Bind{ 
  assert(x.tp == e.tp) 
  val tp: PrimitiveType = e.tp
}
case class BindTuple(x: VarDef, e: TupleCalc) extends TupleCalc with Bind{ 
  assert(x.tp == e.tp)
  val tp: TupleType = e.tp 
}

object Bind{
  def apply(x: VarDef, v: Calc): Bind = v.tp match {
    case t: TupleType => BindTuple(x, v.asInstanceOf[TupleCalc])
    case t: PrimitiveType => BindPrimitive(x, v.asInstanceOf[PrimitiveCalc])
    case t: BagType => Generator(x, v.asInstanceOf[BagCalc])
    case _ => throw new IllegalArgumentException(s"cannot bind VarDef(${x.n})")
  }
}

/**
  * Conditionals are of the form e1 op e2, and are considered predicates
  * when inside a comprehension 
  */
case class Conditional(op: OpCmp, e1: AttributeCalc, e2: AttributeCalc){ self =>
  def normalize = Conditional(op, e1.normalize, e2.normalize)
  def substitute(e3: Calc, v: VarDef): Conditional = Conditional(op, e1.substitute(e2, v), e2.substitute(e3, v))
}
case class Pred(cond: Conditional) extends PrimitiveCalc { val tp: PrimitiveType = BoolType }

/**
  * Bag comprehension representing union over a bag: { e | qs ... }
  */
case class BagComp(e: TupleCalc, qs: List[Calc]) extends BagCalc{
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
case class Tup(fields: Map[String, AttributeCalc]) extends TupleCalc {
  val tp: TupleType = TupleType(fields.map(f => f._1 -> f._2.tp))
}
object Tup{
  def apply(fs: (String, AttributeCalc)*): Tup = Tup(Map(fs:_*))
}

/**
  * if pred then { e1 | qs1 ... } else { e2 | qs2 ... }
  */
case class IfStmt(cond: List[Conditional], e1: BagCalc, e2: Option[BagCalc] = None) extends BagCalc {
  assert(e2.isEmpty || e1.tp == e2.get.tp) 
  val tp: BagType = e1.tp
}

/**
  * Represents an input relation
  */
case class InputR(n: String, b: PhysicalBag) extends BagCalc{
  val tp: BagType = b.tp
}

/**
  * Labels? to be used with shredding
  */
case class CLabel(vars: List[Var], flat: BagCalc) extends BagCalc{
  val tp: BagType = flat.tp
}

