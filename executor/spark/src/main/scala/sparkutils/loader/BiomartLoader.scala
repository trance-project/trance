package sparkutils.loader
/** Generated Code **/
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

case class Biomart(gene_stable_id: String, gene_stable_id_version: String, transcript_stable_id_version: String, protein_stable_id: String, protein_stable_id_version: String, gene_start_bp: Int, gene_end_bp: Int, transcript_start_bp: Int, transcript_end_bp: Int, biomart_gene_name: String)

class BiomartLoader(spark: SparkSession) extends Table[Biomart] {
 
   import spark.implicits._
   val schema = StructType(Array(StructField("gene_stable_id", StringType),
    StructField("gene_stable_id_version", StringType),
    StructField("transcript_stable_id", StringType),
	StructField("transcript_stable_id_version", StringType),
    StructField("protein_stable_id", StringType),
    StructField("protein_stable_id_version", StringType),
    StructField("gene_start_bp", IntegerType),
    StructField("gene_end_bp", IntegerType),
    StructField("transcript_start_bp", IntegerType),
    StructField("transcript_end_bp", IntegerType),
    StructField("biomart_gene_name", StringType)))
   val header: Boolean = true
   val delimiter: String = "\t"
   
   def load(path: String): Dataset[Biomart] = {
     spark.read.schema(schema)
       .option("header", header)
       .option("delimiter", delimiter)
       .csv(path)
       .as[Biomart]        
   }
}

