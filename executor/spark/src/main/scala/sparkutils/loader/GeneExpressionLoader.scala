package sparkutils.loader

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import java.io.File
import org.apache.spark.sql.functions._

case class AliquotMap(case_id: String, ge_aliquot: String, file_name: String)
case class GeneExpression(ge_aliquot: String, ge_gene_id: String, ge_fpkm: Double)
case class Tmp(filename: String, case_id: String, ge_aliquot: String, file_name: String, ge_gene_id: String, ge_fpkm: Double)
case class GeneExpr(expr_gene: String, fpkm: Double)
case class GeneExprTop(expr_sample: String, gene_expression: Seq[GeneExpr])

class GeneExpressionLoader(spark: SparkSession) {
 
  import spark.implicits._
  val schema = StructType(Array(
    StructField("ge_gene_id", StringType),
    StructField("ge_fpkm", DoubleType)))

  val schemaMap = StructType(Array(
    StructField("case_id", StringType),
    StructField("ge_aliquot", StringType),
	StructField("file_name", StringType)))

  val header: Boolean = false
  val delimiter: String = "\t"
  

  val aliquotUdf = udf { s: String => s.split("\\/").last.split("\\.").head }

  def loadNoAliquot(path: String, dir: Boolean = true): Dataset[GeneExpression] = {
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

  def load(path: String, dir: Boolean = true, aliquotFile: String = "/nfs_qc4/genomics/gdc/biospecimen/fpkm_uq_case_aliquot.txt"): Dataset[GeneExpression] = {
    val files = if (dir) (new File(path)).listFiles().toSeq.map(f => s"$path/${f.getName}")
      else Seq(path)
    val aliquotMap = spark.read.schema(schemaMap)
		.format("csv")
      	.option("header", header)
      	.option("delimiter", delimiter)
      	.load(aliquotFile)
		.withColumn("file_name", aliquotUdf($"file_name"))
		.as[AliquotMap]		
	spark.read.schema(schema)
      .format("csv")
      .option("header", header)
      .option("delimiter", delimiter)
      .load(files:_*)
      .withColumn("filename", input_file_name)
      .withColumn("filename", aliquotUdf($"filename"))
	  .join(aliquotMap, $"filename" === $"file_name", "inner")
	  .drop("filename", "file_name", "case_id")
      .as[GeneExpression]
  }

}

