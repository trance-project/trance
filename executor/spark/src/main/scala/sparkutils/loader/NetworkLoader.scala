package sparkutils.loader
/** Generated Code **/
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

case class Network(protein1: String, protein2: String, neighborhood: Int, neighborhood_transferred: Int, fusion: Int, cooccurence: Int, homology: Int, coexpression: Int, coexpression_transferred: Int, experiments: Int, experiments_transferred: Int, database: Int, database_transferred: Int, textmining: Int, textmining_transferred: Int, combined_score: Int)

case class StringEdge(edge_gene: String, neighborhood: Int, neighborhood_transferred: Int, fusion: Int, cooccurence: Int, homology: Int, coexpression: Int, coexpression_transferred: Int, experiments: Int, experiments_transferred: Int, database: Int, database_transferred: Int, textmining: Int, textmining_transferred: Int, combined_score: Int)


class NetworkLoader(spark: SparkSession) extends Table[Network] {
 
   import spark.implicits._
   val schema = StructType(Array(
      StructField("protein1", StringType),
      StructField("protein2", StringType),
      StructField("neighborhood", IntegerType),
      StructField("neighborhood_transferred", IntegerType),
      StructField("fusion", IntegerType),
      StructField("cooccurence", IntegerType),
      StructField("homology", IntegerType),
      StructField("coexpression", IntegerType),
      StructField("coexpression_transferred", IntegerType),
      StructField("experiments", IntegerType),
      StructField("experiments_transferred", IntegerType),
      StructField("database", IntegerType),
      StructField("database_transferred", IntegerType),
      StructField("textmining", IntegerType),
      StructField("textmining_transferred", IntegerType),
      StructField("combined_score", IntegerType)))

   val header: Boolean = true
   val delimiter: String = " "
   
   def load(path: String): Dataset[Network] = {
     spark.read.schema(schema)
       .option("header", header)
       .option("delimiter", delimiter)
       .csv(path)
       .as[Network]        
   }


}

