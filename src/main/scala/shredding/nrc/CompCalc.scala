package shredding.nrc

import reflect.runtime.universe.{ Symbol => _, _ }

/**
  * Comprehension calculus
  */

trait CompCalc {

  type CBag[A] = List[A]
  
  abstract class Calc[+A]
  
  case class Null[A]() extends Calc[A]
  case class Constant[A](x: A) extends Calc[A]
  // maintain id from NRCexpr 
  case class Symb[A](x: Symbol, id: Int) extends Calc[A]
  case class Record[A](e: Calc[_]*) extends Calc[A] {
    override def productArity: Int = e.size
    override def productElement(i: Int) = e(i)
  }
  case class RProject[A,B](e1: Calc[A], pos: Int) extends Calc[B]
  case class IfStmt[A](e1: Calc[Boolean], e2: Calc[A], e3: Calc[A]) extends Calc[A]
  case class Zero[A]() extends Calc[CBag[A]]
  case class Sng[A](e: Calc[A]) extends Calc[CBag[A]]
  case class Merge[A](e1: Calc[CBag[A]], e2: Calc[CBag[A]]) extends Calc[CBag[A]]
  // U { e | q1, ..., qn }
  case class Generator[A](x: Calc[A], e: Calc[A]) extends Calc[CBag[A]]
  case class Bind[A](x: Calc[A], v: Calc[A]) extends Calc[A]
  case class Pred[A](op: Calc[Boolean]) extends Calc[Boolean]
  // qualifier is a tuple of generator or predicate types
  case class Comprehension[A](e: Calc[A], q: Calc[_]*) extends Calc[CBag[A]]
  case class InputR[A](e: Calc[A], b:CBag[A]) extends Calc[CBag[A]]
  case class CLabel[A](l: Calc[A], e: List[(Any,Any)]) extends Calc[A]
  
  // e1 op e2 
  case class OpEq[A,B](e1: Calc[A], e2: Calc[B]) extends Calc[Boolean]
  case class OpLeq[A,B](e1: Calc[A], e2: Calc[B]) extends Calc[Boolean]
  case class OpLt[A,B](e1: Calc[A], e2: Calc[B]) extends Calc[Boolean]
  case class OpAnd(e1: Calc[Boolean], e2: Calc[Boolean]) extends Calc[Boolean]  
  
  // "terms are tree like data structures that represent both calculus and algebraic terms)
  case class Term[A](e1: Calc[A], e2: Calc[A]) extends Calc[A]

  implicit def symbOp[A,B](self: Symb[A]) = new {
    def equals(s: Symb[B]): Boolean = self.x == s.x && self.id == s.id
  }

}
