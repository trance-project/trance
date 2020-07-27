package sparkutils.loader

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import java.io.File
import org.apache.spark.sql.functions._

case class GeneExpression(ge_aliquot: String, ge_gene_id: String, ge_fpkm: Double)
case class GeneExpr(expr_gene: String, fpkm: Double)
case class GeneExprTop(expr_sample: String, gene_expression: Seq[GeneExpr])

class GeneExpressionLoader(spark: SparkSession) {
 
  import spark.implicits._
  val schema = StructType(Array(
    StructField("ge_gene_id", StringType),
    StructField("ge_fpkm", DoubleType)))

  val header: Boolean = false
  val delimiter: String = "\t"
  

  val aliquotUdf = udf { s: String => s.split("\\/").last.split("\\.").head }

  def load(path: String, dir: Boolean = true): Dataset[GeneExpression] = {
    val files = if (dir) (new File(path)).listFiles().toSeq.map(f => s"$path/${f.getName}")
      else Seq(path)
    spark.read.schema(schema)
      .format("csv")
      .option("header", header)
      .option("delimiter", delimiter)
      .load(files:_*)
      .withColumn("filename", input_file_name)
      .withColumn("ge_aliquot", aliquotUdf($"filename"))
      .drop("filename")
      .as[GeneExpression]
  }

}

