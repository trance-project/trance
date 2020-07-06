package sparkutils.loader
/** Generated Code **/
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

case class CopyNumber(cn_gene_id: String, cn_gene_name: String, cn_chromosome: String, cn_start: Int, cn_end: Int, cn_copy_number: Int, min_copy_number: Int, max_copy_number: Int, cn_aliquot_uuid: String)

class CopyNumberLoader(spark: SparkSession) extends Table[CopyNumber] {
 
   import spark.implicits._
   val schema = StructType(Array(StructField("cn_gene_id", StringType),
StructField("cn_gene_name", StringType),
StructField("cn_chromosome", StringType),
StructField("cn_start", IntegerType),
StructField("cn_end", IntegerType),
StructField("cn_copy_number", IntegerType),
StructField("min_copy_number", IntegerType),
StructField("max_copy_number", IntegerType),
StructField("cn_aliquot_uuid", StringType)))
   val header: Boolean = true
   val delimiter: String = "\	"
   
   def load(path: String): Dataset[CopyNumber] = {
     spark.read.schema(schema)
       .option("header", header)
       .option("delimiter", delimiter)
       .csv(path)
       .as[CopyNumber]        
   }
}

