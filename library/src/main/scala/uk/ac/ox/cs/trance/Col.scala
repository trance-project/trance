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
trait Col extends Rep

trait CompCol extends Col {
  val lhs: Rep
  val rhs: Rep
}

case class BaseCol(dfId: String, name: String) extends Col

case class EquiJoinCol(dfId: String, dfId2: String, n: String*) extends Col
case class Literal(v: Any) extends Col
case class Equality(lhs: Rep,rhs: Rep) extends CompCol
case class Inequality(lhs: Rep, rhs: Rep) extends  CompCol
case class GreaterThan(lhs: Rep, rhs: Rep) extends  CompCol
case class GreaterThanOrEqual(lhs: Rep, rhs: Rep) extends  CompCol
case class LessThan(lhs: Rep, rhs: Rep) extends  CompCol
case class LessThanOrEqual(lhs: Rep, rhs: Rep) extends  CompCol
case class OrRep(lhs: Rep, rhs: Rep) extends  CompCol
case class AndRep(lhs: Rep, rhs: Rep) extends  CompCol

trait MathCol extends Col {
  val lhs: Rep
  val rhs: Rep
}

case class Mult(lhs: Rep, rhs: Rep) extends MathCol
case class Add(lhs: Rep, rhs: Rep) extends MathCol
case class Sub(lhs: Rep, rhs: Rep) extends MathCol
case class Divide(lhs: Rep, rhs: Rep) extends MathCol
case class Mod(lhs: Rep, rhs: Rep) extends MathCol


