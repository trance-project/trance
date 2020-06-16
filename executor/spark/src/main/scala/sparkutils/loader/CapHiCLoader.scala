package sparkutils.loader
/** Generated Code **/
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

case class CapHiC(gene_id: String, official_name: String, cap4_enhancer_no: Int)

class CapHiCLoader(spark: SparkSession) extends Table[CapHiC] {
 
   import spark.implicits._
   val schema = StructType(Array(StructField("gene_id", StringType),
StructField("official_name", StringType),
StructField("cap4_enhancer_no", IntegerType)))
   val header: Boolean = true
   val delimiter: String = "\t"
   
   def load(path: String): Dataset[CapHiC] = {
     spark.read.schema(schema)
       .option("header", header)
       .option("delimiter", delimiter)
       .csv(path)
       .as[CapHiC]        
   }
}

