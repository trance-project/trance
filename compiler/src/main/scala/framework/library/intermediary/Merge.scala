package framework.library.intermediary

import org.apache.spark.sql.DataFrame

// Using merge rather than Union due to clash with nrc
case class Merge[T, S](self: WrappedDataframe[T], f: Fun[T, DataFrame]) extends WrappedDataframe[S]