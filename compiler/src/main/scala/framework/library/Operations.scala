package framework.library

import org.apache.spark.sql.DataFrame

case class FlatMap[T, S](self: WrappedDataframe[T], f: Fun[T, DataFrame]) extends WrappedDataframe[S]

case class Merge[T, S](d1: WrappedDataframe[T], f: Fun[T, DataFrame]) extends WrappedDataframe[S]

case class DropDuplicates[S](self: WrappedDataframe[S]) extends WrappedDataframe[S]

case class Drop[T, S](self: WrappedDataframe[T], col: S) extends WrappedDataframe[T]

case class Select[T, S](self: WrappedDataframe[T], col: String) extends WrappedDataframe[T]
