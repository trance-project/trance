package sparkutils.loader

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.broadcast.Broadcast
import sparkutils.skew.SkewDataset._

/** Network loader based on files from String. Found here:
  * https://string-db.org/cgi/download.pl?sessionId=07OYSmYt20Mf&species_text=Homo+sapiens
  *
  */

case class Network(protein1: String, protein2: String, neighborhood: Int, neighborhood_transferred: Int, fusion: Int, cooccurence: Int, homology: Int, coexpression: Int, coexpression_transferred: Int, experiments: Int, experiments_transferred: Int, database: Int, database_transferred: Int, textmining: Int, textmining_transferred: Int, combined_score: Int)

case class StringEdge(edge_protein: String, neighborhood: Int, neighborhood_transferred: Int, fusion: Int, cooccurence: Int, homology: Int, coexpression: Int, coexpression_transferred: Int, experiments: Int, experiments_transferred: Int, database: Int, database_transferred: Int, textmining: Int, textmining_transferred: Int, combined_score: Int)

case class StringEdgeDict2(_1: String, edge_protein: String, neighborhood: Int, neighborhood_transferred: Int, fusion: Int, cooccurence: Int, homology: Int, coexpression: Int, coexpression_transferred: Int, experiments: Int, experiments_transferred: Int, database: Int, database_transferred: Int, textmining: Int, textmining_transferred: Int, combined_score: Int)

case class StringNode(node_protein: String, edges: Seq[StringEdge])

case class StringNodeDict1(node_protein: String, edges: String)

class NetworkLoader(spark: SparkSession) extends Table[StringNode] {
 
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
   val delimiter: String = ","

   /**
     * Loads the network as a relational table (flattened)
     *
     **/
   def loadFlat(path: String): Dataset[Network] = {
      spark.read.schema(schema)
         .option("header", header)
         .option("delimiter", delimiter)
         .csv(path)
         .as[Network]
   }
   
   /**
     * Loads the network as a one-level nested object 
     *
     **/
   def load(path: String): Dataset[StringNode] = {
     loadFlat(path).groupByKey(x => x.protein1).mapGroups{
         case (key, grps) => 
            val edges = grps.map(g => StringEdge(g.protein2, g.neighborhood, g.neighborhood_transferred, g.fusion, g.cooccurence, g.homology, g.coexpression, 
                g.coexpression_transferred, g.experiments, g.experiments_transferred, g.database, g.database_transferred, g.textmining, g.textmining_transferred, 
                g.combined_score)).toSeq
            StringNode(key, edges) 
       }.as[StringNode]        
   }

   def loadShred(path: String): (Dataset[StringNodeDict1], Dataset[StringEdgeDict2]) = {
     val indexed = loadFlat(path).rdd.zipWithIndex
     val dict1 = indexed.map{ case (pair, id) => StringNodeDict1(pair.protein1, s"$id") }.toDF.as[StringNodeDict1]
     val dict2 = indexed.map{ case (g, id) => 
        StringEdgeDict2(s"$id", g.protein2, g.neighborhood, g.neighborhood_transferred, g.fusion, g.cooccurence, g.homology, g.coexpression, 
                g.coexpression_transferred, g.experiments, g.experiments_transferred, g.database, g.database_transferred, g.textmining, g.textmining_transferred, 
                g.combined_score)
        }.toDF.as[StringEdgeDict2]   
     (dict1, dict2)     
   } 

   def loadSkew(path: String): ((Dataset[StringNodeDict1], Dataset[StringNodeDict1]), 
      (Dataset[StringEdgeDict2], Dataset[StringEdgeDict2], Option[String], Broadcast[Set[String]])) = {

      val (dict1, dict2) = loadShred(path)
      val skew_dict1 = (dict1, dict1.empty)
      val skew_dict2 = (dict2, dict2.empty).repartition[String](col("_1"))
      (skew_dict1, skew_dict2)

   }  


}

