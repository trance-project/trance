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

case class Element(element: String)
case class Motif0(bp_overlap: Long, percentage_overlap: Double, impact: String, motif_feature_id: String, consequence_terms: Seq[String], variant_allele: String)
case class Motif(bp_overlap: Long, percentage_overlap: Double, impact: String, motif_feature_id: String, consequence_terms: Seq[Element], variant_allele: String)
case class Domains(db: String, name: String)
case class Transcript0(bp_overlap: Long, percentage_overlap: Double, gene_symbol_source: String, biotype: String, protein_end: Long, impact: String, gene_id: String, mane: String, 
  domains: Seq[Domains], cdna_start: Long, transcript_id: String, cdna_end: Long, exon: String, cds_end: Long, cds_start: Long, uniparc: Seq[String], 
  ccds: String, canonical: Long, intron: String, protein_id: String, consequence_terms: Seq[String], tsl: Long, strand: Long, swissprot: Seq[String], 
  hgnc_id: String, protein_start: Long, appris: String, variant_allele: String, distance: Long, gene_symbol: String)
case class Transcript(bp_overlap: Long, percentage_overlap: Double, gene_symbol_source: String, biotype: String, protein_end: Long, impact: String, gene_id: String, mane: String, 
  domains: Seq[Domains], cdna_start: Long, transcript_id: String, cdna_end: Long, exon: String, cds_end: Long, cds_start: Long, uniparc: Seq[Element], 
  ccds: String, canonical: Long, intron: String, protein_id: String, consequence_terms: Seq[Element], tsl: Long, strand: Long, swissprot: Seq[Element], 
  hgnc_id: String, protein_start: Long, appris: String, variant_allele: String, distance: Long, gene_symbol: String)
case class Regulatory0(bp_overlap: Long, percentage_overlap: Double, regulatory_feature_id: String, impact: String, consequence_terms: Seq[String], variant_allele: String)
case class Regulatory(bp_overlap: Long, percentage_overlap: Double, regulatory_feature_id: String, impact: String, consequence_terms: Seq[Element], variant_allele: String)
case class Intergenic0(consequence_terms: Seq[String], impact: String, variant_allele: String)
case class Intergenic(consequence_terms: Seq[Element], impact: String, variant_allele: String)
case class AnnotatedVariant0(vreference: String, motif_feature_consequences: Seq[Motif0], allele_string: String, genotypes: Seq[Call], seq_region_name: String, 
  transcript_consequences: Seq[Transcript0], vid: String, regulatory_feature_consequences: Seq[Regulatory0], vcontig: String, assembly_name: String, 
  intergenic_consequences: Seq[Intergenic0], end: Long, variant_class: String, vstart: Int, input: String, most_severe_consequence: String)
case class AnnotatedVariant(vid: String, vreference: String, motif_feature_consequences: Seq[Motif], allele_string: String, genotypes: Seq[Call], seq_region_name: String, 
  transcript_consequences: Seq[Transcript], regulatory_feature_consequences: Seq[Regulatory], vcontig: String, assembly_name: String, 
  intergenic_consequences: Seq[Intergenic], end: Long, variant_class: String, vstart: Int, input: String, most_severe_consequence: String)


case class VariantVep(vid: String, vcontig: String, vstart: Int, vreference: String, genotypes: Seq[Call], vepCall: String)

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

  def loadAnnotations(variants: RDD[VariantContext]) = {
    val vindexed = variants.map(v => {
        val genotypes = v.getGenotypes.iterator.asScala.toSeq.map(s => Call(s.getSampleName, callCategory(s)))
        VariantVep(v.getID, v.getContig, v.getStart, v.getReference.toString, genotypes, formatVariantContext(v))
    }).toDF().as[VariantVep]
    val annots = spark.read.json(vindexed.mapPartitions(it => callVep(it.map(i => i.vepCall).toList).iterator).toDF().as[String])
    vindexed.join(annots, $"vepCall" === $"input").drop("vepCall").as[AnnotatedVariant0]
  }

  def loadOccurences(variants: RDD[VariantContext]) = {

  }

  private def callCategory(g: Genotype): Int = g match {
    case c if g.isHet => 1
    case c if g.isHomVar => 2
    case _ => 0
  }
}
