package framework.library

import org.apache.spark.sql.DataFrame

import scala.language.implicitConversions

case class Dataset[T](in: T) extends WrappedDataframe[T] {

  def wrap(): WrappedDataframe[T] = {
    new Dataset[T](in)
  }
}

object Dataset {
  implicit def wrap[T](in: T): Dataset[T] = new Dataset[T](in)
}
