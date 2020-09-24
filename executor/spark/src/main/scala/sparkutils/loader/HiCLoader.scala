package sparkutils.loader
/** Generated Code **/
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/** Loader used for HiC files provided in iRiGs 
  * supplementary material. 
  *
  */

case class HiC(chr: String, tss_bin_start: Int, tss_bin_end: Int, interacting_bin_start: Int, interacting_bin_end: Int, fdr: String, gene_id: String)

class HiCLoader(spark: SparkSession) extends Table[HiC] {
 
   import spark.implicits._
   val schema = StructType(Array(StructField("chr", StringType),
StructField("tss_bin_start", IntegerType),
StructField("tss_bin_end", IntegerType),
StructField("interacting_bin_start", IntegerType),
StructField("interacting_bin_end", IntegerType),
StructField("fdr", StringType),
StructField("gene_id", StringType)))
   val header: Boolean = true
   val delimiter: String = "\t"
   
   def load(path: String): Dataset[HiC] = {
     spark.read.schema(schema)
       .option("header", header)
       .option("delimiter", delimiter)
       .csv(path)
       .as[HiC]        
   }
}

