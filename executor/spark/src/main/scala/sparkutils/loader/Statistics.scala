package sparkutils.loader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.ArrayType

case class Stat(name: String, sizeInBytes: String, rowCount: String)
case class ColumnStat(name: String, value: Double)

class StatsCollector(spark: SparkSession){

	var tableCols: Map[String, Array[String]] = Map.empty[String, Array[String]]
	// var columnStats: Map[String, Double] = Map.empty[String, Double]
  	def genStat(n: String, s: Statistics): Stat = s.rowCount match {
      case Some(rc) => 
       Stat(n, s.sizeInBytes.toString, rc.toString)
      case _ => Stat(n, s.sizeInBytes.toString, "-1")
    }

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

	def getColumns(table: Dataset[_]): Seq[String] = {
		table.schema.fields.toSeq.flatMap(f => if 
			(!f.dataType.isInstanceOf[ArrayType]) List(f.name) else Nil)
	}

	def writeColStats(tname: String, cols: Seq[String] = Seq(), withShred: Boolean = true): Unit = {

		val db = spark.catalog.currentDatabase
		val metadata = spark.sharedState.externalCatalog.getTable(db, tname)
		val stats = metadata.stats.get

		val colStats = stats.colStats
		val columns = if (cols.nonEmpty) cols else tableCols(tname).toSeq
		for (c <- columns){
			colStats(c).toMap(c).foreach{
				case (key, value) =>
		    		if (!key.contains("histogram")){
		    			val name = s"${tname}.${key}"
		    			println(s"ColumnStat($name,${value.toDouble})")
		    			if (withShred) println(s"ColumnStat(IBag_${name}__D,${value.toDouble})")
		    		}

		    			
			}
		}

	}

	// def writeColStats(): String = columnStats.map(c => s"ColumnStat(${c._1},${c._2})").mkString("\n")

}