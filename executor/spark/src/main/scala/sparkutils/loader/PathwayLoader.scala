package sparkutils.loader
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

case class Name(name: String)
case class Pathway(name: String, url: String, gene_set: List[Name])

case class IPathway(index:Long, name: String, url: String, gene_set: List[Name])
case class SPathway(index:Long, name: String, url: String)

case class SName(_1: Long, name: String)

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

    def shredDS = {
        val input = loadDS.withColumn("index", monotonically_increasing_id()).as[IPathway]
        val pathways = input.drop("gene_set").withColumnRenamed("index", "gene_set").as[SVariant]
        val gene_set = input.flatMap{
            p => p.gene_set.map{
                gene_set => SName(p.index, gene_set.name)
            }
        }.as[SName]
        (pathways, gene_set.repartition($"_1"))
    }
}