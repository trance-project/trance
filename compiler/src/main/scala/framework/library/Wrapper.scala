package framework.library

import org.apache.spark.sql.DataFrame

import scala.language.implicitConversions

case class Wrapper[T](in: T) extends WrappedDataframe[T] {

  def wrap(): WrappedDataframe[T] = {
    new Wrapper[T](in)
  }
}

object Wrapper {
  implicit def wrap[T](in: T): Wrapper[T] = new Wrapper[T](in)
}
