package uk.ac.ox.cs.trance.utilities
import uk.ac.ox.cs.trance.utilities.Symbol.counter

import scala.collection.immutable.{Map => IMap}

object JoinCondContext {
    var ctx: IMap[String, Seq[String]] = IMap.empty

  def addField(f: (String, Seq[String])*): Unit = {
    f.foreach { case (key, newValues) =>
      var counter = 2
      var values = newValues

      while (ctx.exists { case (_, existingValues) => existingValues.intersect(values).nonEmpty }) {
        values = newValues.map {
          case value if ctx.values.exists(_.contains(value)) => s"${value}_$counter"
          case value => value
        }
        counter += 1
      }

      ctx += (key -> values)
    }
  }

  def getMappingsForStr(s: String): Seq[String] = {
    ctx.getOrElse(s, sys.error("no mappings for: " + s))
  }

  def getAllMappings(): IMap[String, Seq[String]] = ctx

  def freshClear(): Unit = ctx = IMap.empty
}
