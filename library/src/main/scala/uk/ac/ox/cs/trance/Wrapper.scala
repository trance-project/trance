package uk.ac.ox.cs.trance

import org.apache.spark.sql.DataFrame
import uk.ac.ox.cs.trance.utilities.JoinCondContext

import scala.language.implicitConversions

/**
 * This is the class & companion object that can be imported into a workspace allowing entry into trance from a Spark Dataframe.
 * <br>
 * Usage:
 * <br>
 * import [[uk.ac.ox.cs.trance.Wrapper.DataFrameImplicit]]
 *
 * Call .wrap() on a Dataframe object eg.
 * Dataframe df -> df.wrap()
 * <br>
 * Which will result in a [[WrappedDataframe]]
 */
case class Wrapper[T](in: T, str: String) extends WrappedDataframe[T]

object Wrapper {
  implicit def wrap(in: DataFrame): Wrapper[DataFrame] = {
    val str: String = utilities.Symbol.fresh()
    JoinCondContext.addField(str -> in.columns)

    new Wrapper[DataFrame](in, str)
  }

  implicit class DataFrameImplicit(df: DataFrame) {
    def wrap(): Wrapper[DataFrame] = Wrapper.wrap(df)
  }
}
