package framework.library.intermediary

import org.apache.spark.sql.{DataFrame, Dataset}

case class FlatMap[T, S](self: WrappedDataframe[T], f: Fun[T, DataFrame]) extends WrappedDataframe[S]

