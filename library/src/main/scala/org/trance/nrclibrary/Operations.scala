package org.trance.nrclibrary

import org.apache.spark.sql.DataFrame

case class FlatMap[T, S](self: WrappedDataframe[T], f: Fun[T, DataFrame]) extends WrappedDataframe[S]

case class Merge[T, S](d1: WrappedDataframe[T], d2: WrappedDataframe[S]) extends WrappedDataframe[S]

case class Join[T, S](self: WrappedDataframe[T], d2: WrappedDataframe[S], joinCond: Rep[T], joinType: String) extends WrappedDataframe[S]

case class DropDuplicates[S](self: WrappedDataframe[S]) extends WrappedDataframe[S]

case class Drop[T](self: WrappedDataframe[T], cols: Seq[String]) extends WrappedDataframe[T]

case class Select[T](self: WrappedDataframe[T], cols: Seq[Rep[T]]) extends WrappedDataframe[T]

case class GroupBy[S](self: WrappedDataframe[S], col: List[String]) extends WrappedDataframe[S] {
  def sum(fields: String*): WrappedDataframe[S] = {
    Reduce(self, col, fields.toList)
  }
}

case class Reduce[T](self: WrappedDataframe[T], col: List[String], values: List[String]) extends WrappedDataframe[T]