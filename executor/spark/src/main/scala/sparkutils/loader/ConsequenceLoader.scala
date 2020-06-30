package sparkutils.loader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._

case class ConseqCalc(so_term: String, so_description: String, display_term: String, impact: String)

class ConsequenceLoader(spark: SparkSession) {

	val schema = StructType(Array(StructField("so_term", StringType,
		StructField("so_description", StringType),
		StructField("display_term", StringType),
		StructField("impact", StringType))))

	def load(path: String): Dataset[ConseqCalc] = {
		spark.read.schema(schema)
			.option("header", header)
			.option("delimiter", "\t")
			.csv(path).as[ConseqCalc]
	}

	

}
