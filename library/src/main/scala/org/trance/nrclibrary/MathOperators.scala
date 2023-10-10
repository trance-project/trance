package org.trance.nrclibrary

case class Mult[T](override val lhs: Rep[T], override val rhs: Rep[T]) extends CompCol[T](lhs, rhs)
case class Add[T](override val lhs: Rep[T], override val rhs: Rep[T]) extends CompCol[T](lhs, rhs)
case class Sub[T](override val lhs: Rep[T], override val rhs: Rep[T]) extends CompCol[T](lhs, rhs)
case class Divide[T](override val lhs: Rep[T], override val rhs: Rep[T]) extends CompCol[T](lhs, rhs)
case class Mod[T](override val lhs: Rep[T], override val rhs: Rep[T]) extends CompCol[T](lhs, rhs)