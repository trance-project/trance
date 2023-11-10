package uk.ac.ox.cs.trance.utilities


/**
 * This object handles duplicate column names by adding a suffix in the form of _(int)
 * that corresponds to how many times that column name has occurred in the join
 * It stores a context that is used in WrappedDataframe to update Dataframe column names and Join Condition Columns.
 *
 * [[counters]] is a mapping between Column names and an integer count of how many occurrences there are of them in the join.
 * In [[addField]] each new Dataframe's column names are check when it is being wrapped. If they already exist in the [[ctx]]
 * that column name's counter is incremented and a suffix is appended to it.
 */
object JoinContext {
    var ctx: Map[String, Seq[String]] = Map.empty
    private var counters: Map[String, Int] = Map.empty

  def addField(f: (String, Seq[String])*): Unit = {
    f.foreach { case (key, newValues) =>
      var values = newValues
      counters ++= values.map(f => f -> (counters.getOrElse(f, 0) + 1)).toMap

      while (ctx.exists { case (_, existingValues) => existingValues.intersect(values).nonEmpty }) {
        values = newValues.map {
          case value if ctx.values.exists(_.contains(value)) => s"${value}_${counters.getOrElse(value, 1)}"
          case value => value
        }
      }

      ctx += (key -> values)
    }
  }

  def getMappingsForStr(s: String): Seq[String] = {
    ctx.getOrElse(s, sys.error("no mappings for: " + s))
  }

  def freshClear(): Unit = {
    ctx = Map.empty
    counters = Map.empty
  }
}
