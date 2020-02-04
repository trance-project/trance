package shredding.nrc

import shredding.core._

/**
  * Base NRC expressions
  */
trait BaseExpr {

  trait Expr extends Serializable {
    def tp: Type
  }

  trait TupleAttributeExpr extends Expr {
    def tp: TupleAttributeType
  }

  trait PrimitiveExpr extends TupleAttributeExpr {
    def tp: PrimitiveType
  }

  trait NumericExpr extends PrimitiveExpr {
    def tp: NumericType
  }

  trait CondExpr extends PrimitiveExpr {
    def tp: PrimitiveType = BoolType
  }

  trait BagExpr extends TupleAttributeExpr {
    def tp: BagType
  }

  trait TupleExpr extends Expr {
    def tp: TupleType
  }

}

/**
  * NRC constructs
  */
trait NRC extends BaseExpr {

  sealed trait Const {
    def v: Any

    def tp: PrimitiveType
  }

  final case class NumericConst(v: AnyVal, tp: NumericType) extends NumericExpr with Const

  final case class PrimitiveConst(v: Any, tp: PrimitiveType) extends PrimitiveExpr with Const

  trait VarRef {
    def varDef: VarDef

    def name: String = varDef.name

    def tp: Type = varDef.tp
  }

  final case class NumericVarRef(varDef: VarDef) extends NumericExpr with VarRef {
    override def tp: NumericType = super.tp.asInstanceOf[NumericType]
  }

  final case class PrimitiveVarRef(varDef: VarDef) extends PrimitiveExpr with VarRef {
    override def tp: PrimitiveType = super.tp.asInstanceOf[PrimitiveType]
  }

  final case class BagVarRef(varDef: VarDef) extends BagExpr with VarRef {
    override def tp: BagType = super.tp.asInstanceOf[BagType]
  }

  final case class TupleVarRef(varDef: VarDef) extends TupleExpr with VarRef {
    override def tp: TupleType = super.tp.asInstanceOf[TupleType]
  }

  trait Project {
    def tuple: TupleVarRef

    def field: String

    def tp: TupleAttributeType = tuple.tp(field)
  }

  final case class NumericProject(tuple: TupleVarRef, field: String) extends NumericExpr with Project {
    override def tp: NumericType = super.tp.asInstanceOf[NumericType]
  }

  final case class PrimitiveProject(tuple: TupleVarRef, field: String) extends PrimitiveExpr with Project {
    override def tp: PrimitiveType = super.tp.asInstanceOf[PrimitiveType]
  }

  final case class BagProject(tuple: TupleVarRef, field: String) extends BagExpr with Project {
    override def tp: BagType = super.tp.asInstanceOf[BagType]
  }

  final case class ForeachUnion(x: VarDef, e1: BagExpr, e2: BagExpr) extends BagExpr {
    assert(x.tp == e1.tp.tp)

    val tp: BagType = e2.tp
  }

  final case class Union(e1: BagExpr, e2: BagExpr) extends BagExpr {
    assert(e1.tp == e2.tp)

    val tp: BagType = e1.tp
  }

  final case class Singleton(e: TupleExpr) extends BagExpr {
    val tp: BagType = BagType(e.tp)
  }

  final case class Tuple(fields: Map[String, TupleAttributeExpr]) extends TupleExpr {
    val tp: TupleType = TupleType(fields.map(f => f._1 -> f._2.tp))
  }

  object Tuple {
    def apply(fs: (String, TupleAttributeExpr)*): Tuple = Tuple(Map(fs: _*))
  }

  trait Let {
    def x: VarDef

    def e1: Expr

    def e2: Expr
  }

  final case class NumericLet(x: VarDef, e1: Expr, e2: NumericExpr) extends NumericExpr with Let {
    assert(x.tp == e1.tp)

    val tp: NumericType = e2.tp
  }

  final case class PrimitiveLet(x: VarDef, e1: Expr, e2: PrimitiveExpr) extends PrimitiveExpr with Let {
    assert(x.tp == e1.tp)

    val tp: PrimitiveType = e2.tp
  }

  final case class BagLet(x: VarDef, e1: Expr, e2: BagExpr) extends BagExpr with Let {
    assert(x.tp == e1.tp)

    val tp: BagType = e2.tp
  }

  final case class TupleLet(x: VarDef, e1: Expr, e2: TupleExpr) extends TupleExpr with Let {
    assert(x.tp == e1.tp)

    val tp: TupleType = e2.tp
  }

  final case class Total(e: BagExpr) extends NumericExpr {
    val tp: NumericType = IntType
  }

  final case class DeDup(e: BagExpr) extends BagExpr {
    val tp: BagType = e.tp
  }
  
  sealed trait Cmp {
    def op: OpCmp

    def e1: Expr

    def e2: Expr
  }

  final case class PrimitiveCmp(op: OpCmp, e1: PrimitiveExpr, e2: PrimitiveExpr) extends CondExpr with Cmp {
    assert(e1.tp == e2.tp || (e1.tp.isInstanceOf[NumericType] && e2.tp.isInstanceOf[NumericType]))
  }

  final case class And(e1: CondExpr, e2: CondExpr) extends CondExpr

  final case class Or(e1: CondExpr, e2: CondExpr) extends CondExpr

  final case class Not(c: CondExpr) extends CondExpr

  trait IfThenElse {
    def cond: CondExpr

    def e1: Expr

    def e2: Option[Expr]
  }

  final case class NumericIfThenElse(cond: CondExpr, e1: NumericExpr, p2: NumericExpr) extends NumericExpr with IfThenElse {
    assert(e1.tp == p2.tp)

    val tp: NumericType = e1.tp

    def e2: Option[NumericExpr] = Some(p2)
  }

  final case class PrimitiveIfThenElse(cond: CondExpr, e1: PrimitiveExpr, p2: PrimitiveExpr) extends PrimitiveExpr with IfThenElse {
    assert(e1.tp == p2.tp)

    val tp: PrimitiveType = e1.tp

    def e2: Option[PrimitiveExpr] = Some(p2)
  }

  final case class BagIfThenElse(cond: CondExpr, e1: BagExpr, e2: Option[BagExpr]) extends BagExpr with IfThenElse {
    assert(e2.isEmpty || e1.tp == e2.get.tp)

    val tp: BagType = e1.tp
  }

  final case class TupleIfThenElse(cond: CondExpr, e1: TupleExpr, t2: TupleExpr) extends TupleExpr with IfThenElse {
    assert(e1.tp == t2.tp)

    val tp: TupleType = e1.tp

    def e2: Option[TupleExpr] = Some(t2)
  }

  final case class ArithmeticExpr(op: OpArithmetic, e1: NumericExpr, e2: NumericExpr) extends NumericExpr {
    val tp: NumericType = (e1.tp, e2.tp) match {
      case (DoubleType, _) | (_, DoubleType) => DoubleType
      case (LongType, _) | (_, LongType) => LongType
      case (IntType, _) | (_, IntType) => IntType
      case (tp1, tp2) if tp1 == tp2 => tp1
      case _ => sys.error("Cannot create arithmetic expression between types " + e1.tp + " and " + e2.tp)
    }
  }

  final case class Assignment(name: String, rhs: Expr)

  final case class Program(statements: List[Assignment]) {
    assert(statements.map(_.name).distinct.size == statements.map(_.name).size)

    def ++(p: Program): Program = Program(statements ++ p.statements)

    def append(a: Assignment): Program = Program(statements :+ a)

    def get(n: String): Option[Assignment] = statements.filter(_.name == n) match {
      case Nil => None
      case hd :: Nil => Some(hd)
      case _ => sys.error("Multiple assignments with the same name")
    }

    def apply(n: String): Assignment = get(n) match {
      case Some(a) => a
      case None => throw new NoSuchElementException
    }
  }

  object Program {
    def apply(statements: Assignment*): Program = Program(List(statements: _*))
  }

  /////////////////
  //
  //
  // UNSTABLE BELOW
  //
  //
  /////////////////

  final case class WeightedSingleton(e: TupleExpr, w: NumericExpr) extends BagExpr {
    val tp: BagType = BagType(e.tp)
  }

//  final case class GroupByKey(e: BagExpr, keyFn: TupleExpr => TupleExpr, valueFn: TupleExpr => TupleExpr = identity) extends BagExpr {
//    val tp: BagType = {
//      val t = TupleVarRef(VarDef.fresh(e.tp.tp))
//      BagType(TupleType(keyFn(t).tp.attrTps + "_2" -> BagType(valueFn(t).tp)))
//    }
//  }
//
//  final case class ReduceByKey(e: BagExpr, keyFn: TupleExpr => TupleExpr, valueFn: TupleExpr => PrimitiveExpr) extends BagExpr {
//    val tp: BagType = {
//      val t = TupleVarRef(VarDef.fresh(e.tp.tp))
//      BagType(TupleType(keyFn(t).tp.attrTps + "_2" -> valueFn(t).tp))
//    }
//  }

  trait GroupBy extends BagExpr {
    def bag: BagExpr
    def grp: Expr
    def value: Expr
    def v: VarDef
  }

  final case class BagGroupBy(bag: BagExpr, v: VarDef, grp: TupleExpr, value: TupleExpr) extends GroupBy {
    val tp: BagType = BagType(TupleType(grp.tp.attrTps + ("_2" -> BagType(value.tp))))
  }

  final case class PlusGroupBy(bag: BagExpr, v: VarDef, grp: TupleExpr, value: PrimitiveExpr) extends GroupBy {
    val tp: BagType = BagType(TupleType(grp.tp.attrTps + ("_2" -> value.tp)))
  }

}
