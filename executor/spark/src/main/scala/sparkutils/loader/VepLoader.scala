package sparkutils.loader

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import htsjdk.variant.variantcontext.{Genotype, VariantContext}
import sparkutils.Config
import org.apache.spark.rdd.RDD
import java.io.ByteArrayInputStream
import scala.sys.process._
import scala.collection.JavaConverters._

case class VariantVep(id: String, contig: String, start: Int, reference: String, genotypes: Seq[Call], vepCall: String)

class VepLoader(spark: SparkSession) extends Serializable {

  import spark.implicits._

  val vepCommandLine = s"${Config.vepHome} --cache -o STDOUT --assembly GRCh38 --json --everything --offline --no_stats --format vcf --dir_cache ${Config.vepCache}"

  def formatVariantContext(v: VariantContext): String = {
    List(v.getAlternateAlleles).map(a => 
        s"${v.getContig}\t${v.getStart}\t${v.getID}\t${v.getReference}\t$a\t.\tPASS\t.").mkString("\n")
  }

  def formatVariant(v: Variant): String = {
    s"${v.contig}\t${v.start}\t${v.id}\t${v.reference}\t${v.alternate}\t.\tPASS\t."
  }

  // compare against process logger?
  def callVep(formatted: List[String]): Stream[String] = {
    val is = new ByteArrayInputStream(formatted.mkString("\n").getBytes("UTF-8"))

    val cmd = Process(vepCommandLine)

    val p = cmd #< is
    p.run
    return p.lineStream

  }

  //def loadAnnotations(variants: RDD[VariantContext]) = {
  //  spark.read.json(variants.mapPartitions(it => variantContextToVep(it.toList).iterator).toDF().as[String])
  //}

  def genotypesJSON(genotypes: Seq[Genotype]): String =
    genotypes.map(s => s"""{"sample": s.getSampleName, "call": ${callCategory(s)} }""").mkString("[",",","]")

  def variantJSON(v: VariantContext): String = {
    val (i, c, s, r, g) = (v.getID, v.getContig, v.getStart, v.getReference, 
      genotypesJSON(v.getGenotypes.iterator.asScala.toSeq))
    s"""{"id": $i, "contig": $c, "start": $s, "reference": $r, "genotypes": $g, "vepCall": ${formatVariantContext(v)}}"""    
  }

  def seqloadAnnotations(variants: RDD[VariantContext]) = {
    val vindexed = variants.map(v => {
        val genotypes = v.getGenotypes.iterator.asScala.toSeq.map(s => Call(s.getSampleName, callCategory(s)))
        VariantVep(v.getID, v.getContig, v.getStart, v.getReference.toString, genotypes, formatVariantContext(v))
    }).toDF().as[VariantVep]
    val annots = spark.read.json(vindexed.mapPartitions(it => callVep(it.map(i => i.vepCall).toList).iterator).toDF().as[String])
    vindexed.join(annots, $"vepCall" === $"input") 
  }

  def loadOccurences(variants: RDD[VariantContext]) = {
    val setup = seqloadAnnotations(variants)
    setup.flatMap{v => v.genotypes} 
  }

  private def callCategory(g: Genotype): Int = g match {
    case c if g.isHet => 1
    case c if g.isHomVar => 2
    case _ => 0
  }
}
