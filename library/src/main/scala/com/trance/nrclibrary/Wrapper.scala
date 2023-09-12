package com.trance.nrclibrary

import org.apache.spark.sql.DataFrame

import scala.language.implicitConversions

case class Wrapper[T](in: T, str: String) extends WrappedDataframe[T]

object Wrapper {
  implicit def wrap(in: DataFrame): Wrapper[DataFrame] = {
    val str: String = utilities.Symbol.fresh()
    new Wrapper[DataFrame](in, str)
  }

  implicit class DataFrameImplicit(df: DataFrame) {
    def wrap(): Wrapper[DataFrame] = Wrapper.wrap(df)
  }
}
