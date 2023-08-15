package framework.library.utilities

import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable

object Context {

  val ctx: mutable.Map[String, Dataset[Row]] = collection.mutable.Map[String, Dataset[Row]]()

  def addMapping(s: (String, Dataset[Row])): Unit = {
    ctx += s
    println(ctx)
  }

  def getMapping(s: String): Dataset[Row] = {
    ctx.getOrElse(s, null)
  }
}
