package sparkutils.loader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, DoubleType, StructField, StructType}

case class Gene(name: String, description: String, chrom: String, g_type: String, start_hg19: Int, end_hg19: Int, 
  strand: String, ts_id: String, gene_type: String, gene_status: String, loci_level: Int, 
  alias_symbol: String, official_name: String)

class GeneLoader(spark: SparkSession, path: String) extends Serializable {

  import spark.implicits._

  val schema = StructType(Array(
    StructField("name", StringType),
    StructField("description", StringType),
    StructField("chrom", StringType),
    StructField("g_type", StringType),
    StructField("start_hg19", IntegerType),
    StructField("end_hg19", IntegerType),
    StructField("strand", StringType),
    StructField("ts_id", StringType),
    StructField("gene_type", StringType),
    StructField("gene_status", StringType),
    StructField("loci_level", IntegerType),
    StructField("alias_symbol", StringType),
    StructField("official_name", StringType)))

  val table = spark.read.schema(schema)
    .option("header", true)
    .option("delimiter", "\t")
    .csv(path)
    .as[Gene]

}