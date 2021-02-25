package sparkutils.loader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, DoubleType, StructField, StructType}
import sparkutils.Config

/** Gene information table provided from All_human_genes 
  * file from iRiGs supplementary.
  *
  */


case class Gene(name: String, description: String, chrom: String, g_type: String, start_hg19: Int, end_hg19: Int, 
  strand: String, ts_id: String, gene_type: String, gene_status: String, loci_level: Int, 
  alias_symbol: String, official_name: String)

class GeneLoader(spark: SparkSession) extends Table[Gene] {

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

  val schema2 = StructType(Array(
    StructField("g_gene_id", StringType),
    StructField("g_gene_name", StringType),
    StructField("g_contig", StringType),
    StructField("g_start", IntegerType),
    StructField("g_end", IntegerType)))

  val header: Boolean = true
  val delimiter: String = "\t"

  def load(path: String) = spark.read.schema(schema)
    .option("header", header)
    .option("delimiter", delimiter)
    .csv(path)
    .as[Gene].repartition(Config.minPartitions)

  def loadGTF(path: String) = spark.read.schema(schema2)
    .option("header", false)
    .option("delimiter", "|")
    .csv(path)
    .as[GTF].repartition(Config.minPartitions)

}
