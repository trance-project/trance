package uk.ac.ox.cs.trance

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait Operation extends WrappedDataframe

case class Map(self: WrappedDataframe, f: Fun, output: RepRowEncoder) extends Operation

case class FlatMap(self: WrappedDataframe, f: Fun, output: RepRowEncoder) extends Operation

case class Merge(d1: WrappedDataframe, d2: WrappedDataframe) extends Operation

case class Join(self: WrappedDataframe, d2: WrappedDataframe, joinCond: Option[Rep]) extends Operation

case class DropDuplicates(self: WrappedDataframe) extends Operation

case class Drop(self: WrappedDataframe, cols: Seq[String]) extends Operation

case class Select(self: WrappedDataframe, cols: Seq[Col]) extends Operation

/**
 * Trance only currently supports GroupBy -> Sum. This is represented as a ReduceByKey in NRC.
 */
case class GroupBy(self: WrappedDataframe, col: List[String]) extends Operation {
  def sum(fields: String*): WrappedDataframe = {
    Reduce(self, col, fields.toList)
  }
}

case class Reduce(self: WrappedDataframe, col: List[String], values: List[String]) extends Operation

// TODO: Operations below need to be implemented

case class Filter(self: WrappedDataframe, cols: Col) extends Operation // Also Where


//case class As(self: WrappedDataframe, alias: String) extends Operation
