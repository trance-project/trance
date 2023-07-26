package framework.library.intermediary

case class Mult[T](e1: Rep[T], e2: Rep[T]) extends Rep[T]

case class Add[T](e1: Rep[T], e2: Rep[T]) extends Rep[T]

case class Sub[T](e1: Rep[T], e2: Rep[T]) extends Rep[T]