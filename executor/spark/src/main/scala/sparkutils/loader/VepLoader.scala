package sparkutils.loader

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import htsjdk.variant.variantcontext.VariantContext
import sparkutils.Config
import org.apache.spark.rdd.RDD
import java.io.ByteArrayInputStream

class VepLoader(spark: SparkSession) extends Serializable {

  val vepCommandLine = s"${Config.vepHome} --cache -o STDOUT --assembly GRCh38 --json --everything --offline --no_stats --format vcf --dir_cache ${Config.vepCache}"

  def formatInput(input: List[VariantContext]): String = {
    input.flatMap{
      v => List(v.getAlternateAlleles).map(a => 
        s"${v.getContig}\t${v.getStart}\t${v.getID}\t${v.getReference}\t$a\t.\tPASS\t.")
    }.mkString("\n")
  }

  def vcfToVep(input: List[VariantContext], verbose: Boolean = false): Stream[String] = {

    import scala.sys.process._

    val formatted = formatInput(input)
    if (verbose) println("\n\nFormatted input: \n" + formatted + "\n")

    val is = new ByteArrayInputStream(formatted.getBytes("UTF-8"))

    val cmd = Process(vepCommandLine)

    val p = cmd #< is
    p.run
    return p.lineStream

  }

  def loadAnnotations(variants: RDD[VariantContext]): RDD[String] = {
    variants.mapPartitions(it => vcfToVep(it.toList).iterator)
  }

}