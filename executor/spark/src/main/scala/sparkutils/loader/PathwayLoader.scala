package sparkutils.loader
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

case class Name(name: String)
case class Pathway(p_name: String, url: String, gene_set: Seq[Name])

case class IPathway(index:Long, p_name: String, url: String, gene_set: Seq[Name])
case class SPathway( p_name: String, url: String, gene_set:Long)

case class SName(_1: Long, name: String)

case class FlatPathway(p_name: String, p_gene: String)
case class VariantPathway(contig: String, start: Int, p_name: String, gene: String, p_gene: String)


class PathwayLoader(spark: SparkSession, path: String) extends Serializable {

    import spark.implicits._

    def loadDS: Dataset[Pathway] = {
        val pathways = spark.sparkContext.textFile(path).map(
            line => {
                val sline = line.split("\t")
                Pathway(sline(0), sline(1), sline.drop(2).map(g => Name(g)).toSeq)
            }
        ).toDF.as[Pathway]
        pathways
    }

    def shredDS: (Dataset[SPathway], Dataset[SName]) = {
        val input = loadDS.withColumn("index", monotonically_increasing_id()).as[IPathway]
        val pathways = input.drop("gene_set").withColumnRenamed("index", "gene_set").as[SPathway].repartition($"_1")
        val gene_set = input.flatMap{
            p => p.gene_set.map{
                gene => SName(p.index, gene.name)
            }
        }.as[SName].repartition($"_1")
        (pathways, gene_set)
    }
}