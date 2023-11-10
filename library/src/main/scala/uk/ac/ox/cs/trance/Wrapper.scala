package uk.ac.ox.cs.trance

import org.apache.spark.sql.DataFrame
import uk.ac.ox.cs.trance.utilities.JoinContext

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
case class Wrapper(in: DataFrame, str: String) extends WrappedDataframe

object Wrapper {
  implicit def wrap(in: DataFrame): Wrapper = {
    val str: String = utilities.Symbol.fresh()
    JoinContext.addField(str -> in.columns)

    new Wrapper(in, str)
  }

  implicit class DataFrameImplicit(df: DataFrame) {
    def wrap(): Wrapper = Wrapper.wrap(df)
  }
}
