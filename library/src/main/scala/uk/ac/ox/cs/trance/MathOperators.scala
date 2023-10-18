package uk.ac.ox.cs.trance

case class Mult[T](lhs: Rep[T], rhs: Rep[T]) extends MathCol[T]
case class Add[T](lhs: Rep[T], rhs: Rep[T]) extends MathCol[T]
case class Sub[T](lhs: Rep[T], rhs: Rep[T]) extends MathCol[T]
case class Divide[T](lhs: Rep[T], rhs: Rep[T]) extends MathCol[T]
case class Mod[T](lhs: Rep[T], rhs: Rep[T]) extends MathCol[T]