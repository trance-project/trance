package shredding.nrc

/**
  * Comprehension calculus
  *
  */

sealed trait Calc { self =>
  def tp: Type 
  
  /**
    * Sends call to proper substitution function
    */
  def substitute(e2: Calc, v: VarDef): Calc = self.tp match {
    case y if self == e2 => Var(v)
    case t:BagType => self.asInstanceOf[BagCalc].substitute(e2, v)
    case t:TupleType => self.asInstanceOf[TupleCalc].substitute(e2, v)
    case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].substitute(e2, v)
    case t:AttributeType => self.asInstanceOf[AttributeCalc].substitute(e2, v)
    case _ => throw new IllegalArgumentException(s"cannot substitute ${self}")
  }

  /**
    * Sends call to proper normalization function
    */
  def normalize: Calc = self.tp match {
    case t:BagType => self.asInstanceOf[BagCalc].normalize
    case t:TupleType => self.asInstanceOf[TupleCalc].normalize
    case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].normalize
    case t:AttributeType => self.asInstanceOf[AttributeCalc].normalize
    case _ => throw new IllegalArgumentException(s"cannot normalize ${self}")
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
    case Generator(x, z @ Sng(Tup(e))) => e.isEmpty
    case _ => false
  }

  def isMergeGenerator: Boolean = self match {
    case Generator(x, z @ Merge(e1, e2)) => true
    case _ => false
  }

  def isIfGenerator: Boolean = self match {
    case Generator(x, z @ IfStmt(c, e1, e2)) => true
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

  def hasMergeGenerator: Boolean = self match {
    case BagComp(e, qs) => qs.map(_.isMergeGenerator).contains(true)
    case _ => false
  }

  def hasIfGenerator: Boolean = self match {
    case BagComp(e, qs) => qs.map(_.isIfGenerator).contains(true)
    case _ => false
  }

  def qualifiers: List[Calc] = self match {
    case Generator(x, v @ Sng(e)) => List(Bind(x, e))
    case Generator(x, v @ BagComp(e, qs)) => qs.map(_.qualifiers).flatten :+ Bind(x, e)
    case _ => List(self)
  }

}


/**
  * AttributeCalc is a trait that is associated to an element of a tuple
  */
trait AttributeCalc extends Calc { self =>
  def tp: AttributeType 
  override def substitute(e2: Calc, v: VarDef): AttributeCalc = self.tp match {
      case y if self == e2 => Var(v).asInstanceOf[AttributeCalc]
      case t:BagType => self.asInstanceOf[BagCalc].substitute(e2, v)
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].substitute(e2, v)
      case _ => throw new IllegalArgumentException(s"cannot normalize ${self}")
  }

  override def normalize: AttributeCalc = self.tp match {
    case t:BagType => self.asInstanceOf[BagCalc].normalize
    case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].normalize
    case _ => self
  }

}

trait PrimitiveCalc extends AttributeCalc{ self =>
  
  def tp: PrimitiveType 
  
  override def substitute(e2: Calc, v: VarDef): PrimitiveCalc = self match {
    case y if self == e2 => Var(v).asInstanceOf[PrimitiveCalc]
    case Conditional(o, e1, e3) => Conditional(o, e1.substitute(e2, v), e3.substitute(e2, v)) 
    case NotCondition(e1) => NotCondition(e1.substitute(e2, v))
    case AndCondition(e1, e3) => AndCondition(e1.substitute(e2, v), e3.substitute(e2, v))
    case OrCondition(e1, e3) => OrCondition(e1.substitute(e2, v), e3.substitute(e2, v))
    case BindPrimitive(x,y) => BindPrimitive(x, y.substitute(e2, v))
    // tuple normalization rule handled in the context of a let
    case PrimitiveVar(x, p @ Some(a), tp) => (v.n == x, e2) match {
      case (true, Tup(fs)) => fs.get(a).get.asInstanceOf[PrimitiveCalc]
      case _ => self
    }
    case _ => self
  }

  // this should handle N10 +{ +{ e | r } | s } => +{ e | s, r }
  override def normalize: PrimitiveCalc = self /**match {
    case CountComp(e1, e2) => e1 match {
      case CountComp(e3, e4) => CountComp(e3.normalize, e2.map(_.normalize) ++ e4.map(_.normalize))
      case _ => CountComp(e1.normalize, e2.map(_.normalize)) 
    } 
    case _ => self
  }**/

}

trait BagCalc extends AttributeCalc { self =>
  
  def tp: BagType 
  
  override def substitute(e2: Calc, v: VarDef): BagCalc = self match {
    case y if self == e2 => Var(v).asInstanceOf[BagCalc]
    case IfStmt(cond, e3, e4) => e4 match {
      case Some(s) => IfStmt(cond.substitute(e2, v), e3.substitute(e2, v), Option(s.substitute(e2, v)))
      case None => IfStmt(cond.substitute(e2, v), e3.substitute(e2, v), None)
    }
    case Generator(x, y) => Generator(x, y.substitute(e2, v))
    case BagVar(x, p @ Some(a), tp) => (v.n == x, e2) match {
      case (true, Tup(fs)) => fs.get(a).get.asInstanceOf[BagCalc]
      case _ => self
    }
    case Sng(e1) => Sng(e1.substitute(e2, v))
    case Merge(e1, e3) => Merge(e1.substitute(e2, v), e3.substitute(e2, v))
    case BagComp(e1, qs) => BagComp(e1.substitute(e2, v), qs.map(_.substitute(e2, v)))
    case _ => self 
  }

  /**
    * Normalization of comprehension types
    */
  override def normalize: BagCalc = self match {
    case BagComp(e, qs) => 
      val b = BagComp(e, qs.map(_.qualifiers).flatten) // N6, N8
      if (b.hasEmptyGenerator) Zero() // N5
      else if (self.hasMergeGenerator) {
        val mg = qs.filter(_.isMergeGenerator)
        val qsn = qs.filterNot(mg.toSet)
        mg.head match {
          case Generator(x, Merge(e1, e2)) => // N7 
            Merge(BagComp(e, Generator(x, e1) +: qsn), BagComp(e, Generator(x, e2) +: qsn)).normalize
          case _ => self
        }
      }
      else if (self.hasIfGenerator){
        val ifs = qs.filter(_.isIfGenerator)
        val qsn = qs.filterNot(ifs.toSet)
        ifs.head match {
          case Generator(x, IfStmt(ps, e1, e2 @ Some(a))) => // N4
            Merge(BagComp(e, List(ps, Generator(x, e1)) ++ qsn), 
                  BagComp(e, List(NotCondition(ps), Generator(x, a)) ++ qsn)).normalize
          case _ => self
        }
      }
      else b
    case Sng(Tup(e)) => 
      if (e.isEmpty) Zero()
      else self 
    case Merge(e1, e2) => Merge(e1.normalize, e2.normalize)
    case IfStmt(e1, e2, e3 @ Some(a)) => IfStmt(e1, e2.normalize, Some(a.normalize))
    case IfStmt(e1, e2, e3 @ None) => IfStmt(e1, e2.normalize, None)
    case _ => self
  }
}

trait TupleCalc extends Calc { self => 
  def tp: TupleType 
  override def substitute(e2: Calc, v: VarDef): TupleCalc = self match {
    case y if self == e2 => Var(v).asInstanceOf[TupleCalc]
    case BindTuple(x,y) => BindTuple(x, y.substitute(e2, v))
    case t:Tup => Tup(t.fields.map(f => f._1 -> f._2.substitute(e2, v)))
    case _ => self 
  }

  // note tuple projection normalization is
  // handled in the transformation since 
  // projection handles on a tuple bound to a variable
  // only currently (i think this is changed in the
  // updated grammar)
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
  * Condition types
  */
case class Conditional(op: OpCmp, e1: AttributeCalc, e2: AttributeCalc) extends PrimitiveCalc{ 
  val tp: PrimitiveType = BoolType
}
case class NotCondition(e1: PrimitiveCalc) extends PrimitiveCalc { 
  assert(e1.tp == BoolType)
  val tp: PrimitiveType = BoolType 
}
case class AndCondition(e1: PrimitiveCalc, e2: PrimitiveCalc) extends PrimitiveCalc { 
  assert(e1.tp == e2.tp)
  assert(e1.tp == BoolType)
  val tp: PrimitiveType = BoolType
}
case class OrCondition(e1: PrimitiveCalc, e2: PrimitiveCalc) extends PrimitiveCalc {
  assert(e1.tp == e2.tp)
  assert(e1.tp == BoolType)
  val tp: PrimitiveType = BoolType
}

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
case class IfStmt(cond: PrimitiveCalc, e1: BagCalc, e2: Option[BagCalc] = None) extends BagCalc {
  assert(cond.tp == BoolType)
  assert(e2.isEmpty || e1.tp == e2.get.tp) 
  val tp: BagType = e1.tp
}

/**
  * Primitive monoid - count
  */
case class CountComp(e: PrimitiveCalc, qs: List[Calc]) extends PrimitiveCalc{
  // enforce e to not be a bag type
  val tp: PrimitiveType = IntType 
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

