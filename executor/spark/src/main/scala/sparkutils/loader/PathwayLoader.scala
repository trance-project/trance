package sparkutils.loader
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD

case class Name(name: String)
case class Pathway(name: String, url: String, gene_set: List[Name])
case class FlatPathway(name: String, p_gene: String)
case class VariantPathway(contig: String, start: Int, name: String, gene: String, p_gene: String)


class PathwayLoader(spark: SparkSession, path: String) extends Serializable {

    import spark.implicits._

    def loadDS: Dataset[Pathway] = {
        val pathways = spark.sparkContext.textFile(path).map(
            line => {
                val sline = line.split("\t")
                Pathway(sline(0), sline(1), sline.drop(2).map(g => Name(g)).toList)
            }
        ).toDF.as[Pathway]
        pathways
    }
}