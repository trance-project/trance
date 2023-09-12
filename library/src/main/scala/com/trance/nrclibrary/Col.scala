package com.trance.nrclibrary

trait Col[T] extends Rep[T]
case class BaseCol[T](df: String, n: String) extends Col[T]

trait CompCol[T] extends Col[T] {

  def lhs: Rep[T]
  def rhs: Rep[T]
}
case class Equality[T](e1: Rep[T], e2: Rep[T]) extends CompCol[T] {
  override def lhs: Rep[T] = e1
  override def rhs: Rep[T] = e2
}

case class Inequality[T](e1: Rep[T], e2: Rep[T]) extends CompCol[T] {
  override def lhs: Rep[T] = e1

  override def rhs: Rep[T] = e2
}

case class GreaterThan[T](e1: Rep[T], e2: Rep[T]) extends CompCol[T] {
  override def lhs: Rep[T] = e1

  override def rhs: Rep[T] = e2
}

case class GreaterThanOrEqual[T](e1: Rep[T], e2: Rep[T]) extends CompCol[T] {
  override def lhs: Rep[T] = e1

  override def rhs: Rep[T] = e2
}

case class LessThan[T](e1: Rep[T], e2: Rep[T]) extends CompCol[T] {
  override def lhs: Rep[T] = e1

  override def rhs: Rep[T] = e2
}

case class LessThanOrEqual[T](e1: Rep[T], e2: Rep[T]) extends CompCol[T] {
  override def lhs: Rep[T] = e1

  override def rhs: Rep[T] = e2
}

case class OrRep[T](e1: Rep[T], e2: Rep[T]) extends CompCol[T] {
  override def lhs: Rep[T] = e1

  override def rhs: Rep[T] = e2
}




