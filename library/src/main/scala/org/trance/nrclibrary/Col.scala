package org.trance.nrclibrary

trait Col[T] extends Rep[T]
case class BaseCol[T](df: String, n: String) extends Col[T]

// CompCol is not a case class because of case-to-case inheritance being prohibited in Scala
class CompCol[T](val lhs: Rep[T], val rhs: Rep[T]) extends Col[T] with Serializable

case class Equality[T](override val lhs: Rep[T], override val rhs: Rep[T]) extends CompCol[T](lhs, rhs)
case class Inequality[T](override val lhs: Rep[T], override val rhs: Rep[T]) extends CompCol[T](lhs, rhs)
case class GreaterThan[T](override val lhs: Rep[T], override val rhs: Rep[T]) extends CompCol[T](lhs, rhs)
case class GreaterThanOrEqual[T](override val lhs: Rep[T], override val rhs: Rep[T]) extends CompCol[T](lhs, rhs)
case class LessThan[T](override val lhs: Rep[T], override val rhs: Rep[T]) extends CompCol[T](lhs, rhs)
case class LessThanOrEqual[T](override val lhs: Rep[T], override val rhs: Rep[T]) extends CompCol[T](lhs, rhs)
case class OrRep[T](override val lhs: Rep[T], override val rhs: Rep[T]) extends CompCol[T](lhs, rhs)
case class AndRep[T](override val lhs: Rep[T], override val rhs: Rep[T]) extends CompCol[T](lhs, rhs)




