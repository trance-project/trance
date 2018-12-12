package shredding.nrc

import reflect.runtime.universe.{ Symbol => _, _ }

/**
  * Grammar for NRC 
  */

trait NRCExprs {

  type TTuple1[A] = Tuple1[A]
  type TTuple2[A, B] = Tuple2[A, B]
  type TTuple3[A, B, C] = Tuple3[A, B, C]
  type TBag[A] = List[A]

  /**
    * Expr[A] is the type of expressions that evaluate to a value of type A
    */
  abstract class Expr[A]

  /**
    * A statically known constant of type T (e.g., String and Int)
    */
  case class Const[A](x: A) extends Expr[A]

  /**
    * A Sym is a symbolic reference used internally to refer to expressions.
    */
  object Sym { private var currId = 0 }
  case class Sym[A](x: Symbol, id: Int = { Sym.currId += 1; Sym.currId }) extends Expr[A]

  /**
    * NRC constructs
    */
  case class ForeachUnion[A, B](x: Sym[A], e1: Expr[TBag[A]], e2: Expr[TBag[B]]) extends Expr[TBag[B]]

  case class Union[A](e1: Expr[TBag[A]], e2: Expr[TBag[A]]) extends Expr[TBag[A]]

  case class Eq[A,B](e1: Expr[A], e2: Expr[B]) extends Expr[Boolean]

  // this may be too strict
  case class IfThenElse[A,B,C](e1: Expr[A], e2: Expr[B], e3: Expr[C]) extends Expr[TBag[B]]
  
  case class Singleton[A](e: Expr[A]) extends Expr[TBag[A]]

  case class TupleStruct1[A](e1: Expr[A]) extends Expr[TTuple1[A]]

  case class TupleStruct2[A, B](e1: Expr[A], e2: Expr[B]) extends Expr[TTuple2[A, B]]

  case class TupleStruct3[A, B, C](e1: Expr[A], e2: Expr[B], e3: Expr[C]) extends Expr[TTuple3[A, B, C]]

  case class Project[A, B](target: Expr[A], pos: Int) extends Expr[B]

  case class Relation[A](r: Sym[A], b: TBag[A]) extends Expr[TBag[A]]

  case class Label[A](l: Sym[A], e: Expr[A]) extends Expr[A]

  /**
    * Extension methods for NRC expressions
    */
  implicit class TraversalOps[A: TypeTag](e: Expr[A]) {

    def collect[B](f: PartialFunction[Expr[_], List[B]]): List[B] =
      f.applyOrElse(e, (ex: Expr[_]) => ex match {
        case ForeachUnion(_, e1, e2) => e1.collect(f) ++ e2.collect(f)
        case Union(e1, e2) => e1.collect(f) ++ e2.collect(f)
        case Singleton(e1) => e1.collect(f)
        case TupleStruct1(e1) => e1.collect(f)
        case TupleStruct2(e1, e2) => e1.collect(f) ++ e2.collect(f)
        case TupleStruct3(e1, e2, e3) => e1.collect(f) ++ e2.collect(f) ++ e3.collect(f)
        case Project(e1, _) => e1.collect(f)
        case _ => List()
      })

    def containsRelation: Boolean =
      e.collect { case Relation(_, _) => List(true) }.exists(x => x)

    def isBagType: Boolean = typeOf[A] <:< typeOf[TBag[_]]

    def isTupleType: Boolean =
      typeOf[A] <:< typeOf[TTuple1[_]] ||
        typeOf[A] <:< typeOf[TTuple2[_, _]] ||
        typeOf[A] <:< typeOf[TTuple3[_, _, _]]

    def vars: List[Sym[_]] =
      collect {
        case ForeachUnion(x, e1, e2) => x :: e1.vars ++ e2.vars
        case Relation(n, _) => List(n)
      }
  }
}


