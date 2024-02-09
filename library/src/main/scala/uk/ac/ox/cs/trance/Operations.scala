package uk.ac.ox.cs.trance

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait Operation extends WrappedCollection

case class Map(self: WrappedCollection, f: Fun) extends Operation

// FLatMap Fun output -> WrappedCollection
case class FlatMap(self: WrappedCollection, f: Fun) extends Operation

case class Merge(d1: WrappedCollection, d2: WrappedCollection) extends Operation

case class Join(self: WrappedCollection, d2: WrappedCollection, joinCond: Option[Rep]) extends Operation

case class DropDuplicates(self: WrappedCollection) extends Operation

case class Drop(self: WrappedCollection, cols: Seq[String]) extends Operation

case class Select(self: WrappedCollection, cols: Seq[Col]) extends Operation

/**
 * Trance only currently supports GroupBy -> Sum. This is represented as a ReduceByKey in NRC.
 */
case class GroupBy(self: WrappedCollection, col: List[String]) extends Operation {
  def sum(fields: String*): WrappedCollection = {
    Reduce(self, col, fields.toList)
  }
}

case class Reduce(self: WrappedCollection, col: List[String], values: List[String]) extends Operation

// TODO: Operations below need to be implemented

case class Filter(self: WrappedCollection, cols: Col) extends Operation // Also Where


//case class As(self: WrappedDataframe, alias: String) extends Operation
