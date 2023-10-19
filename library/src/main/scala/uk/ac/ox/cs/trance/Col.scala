package uk.ac.ox.cs.trance

/**
 * A extension of [[Rep]] that imitates Spark's [[org.apache.spark.sql.Column]]
 * <br>
 * [[Col]] is split into 4 types:
 * <br>
 * [[BaseCol]] is the innermost definition of a column - it contains reference to the dataset the column is a part of 'dfId' created during [[Wrapper]] initialisation and the name of the column.
 * <br>
 * [[Literal]] is for holding constant values in column form. For comparison or math operations where one or more values are constant.
 * <br>
 * [[CompCol]] are columns containing comparison between columns. They have a left and right argument that can be BaseCols or nested CompCols.
 * <br>
 * [[MathCol]] work the same as CompCols but are defined separately so they can be handled differently during conversion to NRC Expression in [[NRCConverter]]
 */
trait Col[T] extends Rep[T]

trait CompCol[T] extends Col[T] {
  val lhs: Rep[T]
  val rhs: Rep[T]
}

case class BaseCol[T](dfId: String, name: String) extends Col[T]
case class Literal[T](v: Any) extends Col[T]
case class Equality[T](lhs: Rep[T],rhs: Rep[T]) extends CompCol[T]
case class Inequality[T](lhs: Rep[T], rhs: Rep[T]) extends  CompCol[T]
case class GreaterThan[T](lhs: Rep[T], rhs: Rep[T]) extends  CompCol[T]
case class GreaterThanOrEqual[T](lhs: Rep[T], rhs: Rep[T]) extends  CompCol[T]
case class LessThan[T](lhs: Rep[T], rhs: Rep[T]) extends  CompCol[T]
case class LessThanOrEqual[T](lhs: Rep[T], rhs: Rep[T]) extends  CompCol[T]
case class OrRep[T](lhs: Rep[T], rhs: Rep[T]) extends  CompCol[T]
case class AndRep[T](lhs: Rep[T], rhs: Rep[T]) extends  CompCol[T]

trait MathCol[T] extends Col[T] {
  val lhs: Rep[T]
  val rhs: Rep[T]
}

case class Mult[T](lhs: Rep[T], rhs: Rep[T]) extends MathCol[T]
case class Add[T](lhs: Rep[T], rhs: Rep[T]) extends MathCol[T]
case class Sub[T](lhs: Rep[T], rhs: Rep[T]) extends MathCol[T]
case class Divide[T](lhs: Rep[T], rhs: Rep[T]) extends MathCol[T]
case class Mod[T](lhs: Rep[T], rhs: Rep[T]) extends MathCol[T]


