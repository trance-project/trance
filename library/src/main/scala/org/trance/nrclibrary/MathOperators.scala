package org.trance.nrclibrary

case class Mult[T](e1: Rep[T], e2: Rep[T]) extends CompCol[T] {
  override def lhs: Rep[T] = e1

  override def rhs: Rep[T] = e2
}

case class Add[T](e1: Rep[T], e2: Rep[T]) extends Rep[T]

case class Sub[T](e1: Rep[T], e2: Rep[T]) extends Rep[T]

