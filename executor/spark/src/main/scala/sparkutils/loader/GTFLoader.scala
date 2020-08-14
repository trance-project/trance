package sparkutils.loader
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD

case class GTF(g_contig: String, g_start: Int, g_end: Int, g_gene_name: String, g_gene_id: String)

class GTFLoader(spark: SparkSession, path: String) extends Serializable {
    // Indices of the columns of the gencode file
    private final val COL_CONTIG = 0
    private final val COL_START = 3
    private final val COL_END = 4
    private final val COL_DATA = 8
    //    private final val COL_FEATURE_TYPE = 2
    import spark.implicits._

    def loadDS: Dataset[GTF] = {
        val homo_sapiens_filtered: RDD[String] = spark.sparkContext.textFile(path)
                .mapPartitionsWithIndex { (id_x, iter) => if (id_x == 0) iter.drop(5) else iter }

        val homo_sapiens: Dataset[GTF] = homo_sapiens_filtered.map(
            line => {
                val sline = line.split("\t")
                val geneData = sline(COL_DATA)
                val index1 = geneData.indexOf("gene_name")
				val geneName = geneData.substring(index1).split("\"")(1)
				val index2 = geneData.indexOf("gene_id")
				val geneId = geneData.substring(index2).split("\"")(1)
                GTF(sline(COL_CONTIG), sline(COL_START).toInt, sline(COL_END).toInt, geneName, geneId)
            }
        ).toDF.as[GTF]
        homo_sapiens
    }
}
