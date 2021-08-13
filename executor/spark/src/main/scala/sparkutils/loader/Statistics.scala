package sparkutils.loader

import org.apache.spark.sql.SparkSession

class Statistics(spark: SparkSession){

	var tableCols: Map[String, Array[String]] = Map.empty[String, Array[String]]
	// var columnStats: Map[String, Double] = Map.empty[String, Double]


	def activate(tname: String, columns: Boolean = false, ignore: Set[String] = Set()): Unit = {

		val table = spark.table(tname)
		val statsCboOn = table.queryExecution.analyzed.stats

		if (columns){
			val tcols = table.columns.filter(s => !ignore(s))
			val allCols = tcols.mkString(",")
			tableCols = tableCols + (tname -> tcols)
			spark.sql(s"ANALYZE TABLE $tname COMPUTE STATISTICS FOR COLUMNS $allCols")
		}else{
			spark.sql(s"ANALYZE TABLE $tname COMPUTE STATISTICS")
		}

		table.queryExecution.analyzed.invalidateStatsCache
		val statsCboOn2 = table.queryExecution.analyzed.stats
		val tableStats = table.queryExecution.optimizedPlan.stats
		println(s"Calculated: $tableStats")
		println(s"Status: ${table.queryExecution.logical.fastEquals(table.queryExecution.logical)}")

	}

	def writeColStats(tname: String): Unit = {

		val db = spark.catalog.currentDatabase
		val metadata = spark.sharedState.externalCatalog.getTable(db, tname)
		val stats = metadata.stats.get

		val colStats = stats.colStats
		for (c <- tableCols(tname)){
			colStats(c).toMap(c).foreach{
				case (key, value) =>
		    		if (!key.contains("histogram")) 
		    			println(s"ColumnStat(${tname}.${key},${value.toDouble})")
			}
		}

	}

	// def writeColStats(): String = columnStats.map(c => s"ColumnStat(${c._1},${c._2})").mkString("\n")

}