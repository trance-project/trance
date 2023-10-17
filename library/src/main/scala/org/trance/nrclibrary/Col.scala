package org.trance.nrclibrary

trait Col[T] extends Rep[T]

trait CompCol[T] extends Col[T] {
  val lhs: Rep[T]
  val rhs: Rep[T]
}

trait MathCol[T] extends Col[T] {
  val lhs: Rep[T]
  val rhs: Rep[T]
}
case class BaseCol[T](df: String, n: String) extends Col[T]
case class Literal[T](v: Any) extends Col[T]
case class Equality[T](lhs: Rep[T],rhs: Rep[T]) extends CompCol[T]
case class Inequality[T](lhs: Rep[T], rhs: Rep[T]) extends  CompCol[T]
case class GreaterThan[T](lhs: Rep[T], rhs: Rep[T]) extends  CompCol[T]
case class GreaterThanOrEqual[T](lhs: Rep[T], rhs: Rep[T]) extends  CompCol[T]
case class LessThan[T](lhs: Rep[T], rhs: Rep[T]) extends  CompCol[T]
case class LessThanOrEqual[T](lhs: Rep[T], rhs: Rep[T]) extends  CompCol[T]
case class OrRep[T](lhs: Rep[T], rhs: Rep[T]) extends  CompCol[T]
case class AndRep[T](lhs: Rep[T], rhs: Rep[T]) extends  CompCol[T]




