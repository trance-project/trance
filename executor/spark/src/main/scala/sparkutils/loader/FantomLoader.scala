package sparkutils.loader
/** Generated Code **/
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

case class Fantom(chrom: String, chrom_start: Int, chrom_end: Int, chr: String, pos_range: String, ncbi_id: String, gene_name: String, r: String, r_stat: Double, fdr: String, fdr_stat: Int, score: Int, strand: String, thick_start: Int, thick_end: Int, item_rgb: String, block_count: Int, block_sizes: String, chrom_starts: String)

class FantomLoader(spark: SparkSession) extends Table[Fantom] {
 
   import spark.implicits._
   val schema = StructType(Array(StructField("chrom", StringType),
StructField("chrom_start", IntegerType),
StructField("chrom_end", IntegerType),
StructField("chr", StringType),
StructField("pos_range", StringType),
StructField("ncbi_id", StringType),
StructField("gene_name", StringType),
StructField("r", StringType),
StructField("r_stat", DoubleType),
StructField("fdr", StringType),
StructField("fdr_stat", IntegerType),
StructField("score", IntegerType),
StructField("strand", StringType),
StructField("thick_start", IntegerType),
StructField("thick_end", IntegerType),
StructField("item_rgb", StringType),
StructField("block_count", IntegerType),
StructField("block_sizes", StringType),
StructField("chrom_starts", StringType)))
   val header: Boolean = true
   val delimiter: String = "\t"
   
   def load(path: String): Dataset[Fantom] = {
     spark.read.schema(schema)
       .option("header", header)
       .option("delimiter", delimiter)
       .csv(path)
       .as[Fantom]        
   }
}

