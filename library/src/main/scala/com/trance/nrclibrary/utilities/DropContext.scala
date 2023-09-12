package com.trance.nrclibrary.utilities

import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable

object DropContext {

  var ctx: Seq[String] = Seq.empty[String]

  def addField(cols: String*): Unit = {
    ctx ++= cols
  }

  def getDropFields: Seq[String] = ctx
}
