package sparkutils.loader

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row,DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import htsjdk.variant.variantcontext.{Genotype, VariantContext}
import sparkutils.Config
import org.apache.spark.rdd.RDD
import java.io.ByteArrayInputStream
import scala.sys.process._
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, ArrayType, LongType, DoubleType, StringType, IntegerType, StructField, StructType}


case class Element(element: String)
case class Motif0(bp_overlap: Long, percentage_overlap: Double, impact: String, motif_feature_id: String, consequence_terms: Seq[String], variant_allele: String)
case class Motif(bp_overlap: Long, percentage_overlap: Double, impact: String, motif_feature_id: String, consequence_terms: Seq[Element], variant_allele: String)
case class Domains(db: String, name: String)
case class Transcript0(bp_overlap: Long, percentage_overlap: Double, gene_symbol_source: String, biotype: String, protein_end: Long, impact: String, gene_id: String, mane: String, 
  domains: Seq[Domains], cdna_start: Long, transcript_id: String, cdna_end: Long, exon: String, cds_end: Long, cds_start: Long, uniparc: Seq[String], 
  ccds: String, canonical: Long, intron: String, protein_id: String, consequence_terms: Seq[String], tsl: Long, strand: Long, swissprot: Seq[String], 
  hgnc_id: String, protein_start: Long, appris: String, variant_allele: String, distance: Long, gene_symbol: String)

case class Regulatory0(bp_overlap: Long, percentage_overlap: Double, regulatory_feature_id: String, impact: String, consequence_terms: Seq[String], variant_allele: String)
case class Regulatory(bp_overlap: Long, percentage_overlap: Double, regulatory_feature_id: String, impact: String, consequence_terms: Seq[Element], variant_allele: String)
case class Intergenic0(consequence_terms: Seq[String], impact: String, variant_allele: String)
case class Intergenic(consequence_terms: Seq[Element], impact: String, variant_allele: String)
case class AnnotatedVariant0(vreference: String, motif_feature_consequences: Seq[Motif0], allele_string: String, genotypes: Seq[Call], seq_region_name: String, 
  transcript_consequences: Seq[Transcript0], vid: String, regulatory_feature_consequences: Seq[Regulatory0], vcontig: String, assembly_name: String, 
  intergenic_consequences: Seq[Intergenic0], end: Long, variant_class: String, vstart: Int, input: String, most_severe_consequence: String)
case class AnnotatedVariant(vid: String, vreference: String, motif_feature_consequences: Seq[Motif0], allele_string: String, genotypes: Seq[Call], seq_region_name: String, 
  transcript_consequences: Seq[Transcript], regulatory_feature_consequences: Seq[Regulatory0], vcontig: String, assembly_name: String, 
  intergenic_consequences: Seq[Intergenic0], end: Long, variant_class: String, vstart: Int, input: String, most_severe_consequence: String)

case class VepAnnotation(motif_feature_consequences: Seq[Motif0], allele_string: String, seq_region_name: String, 
  transcript_consequences: Seq[Transcript0], regulatory_feature_consequences: Seq[Regulatory0], assembly_name: String, 
  intergenic_consequences: Seq[Intergenic0], end: Long, variant_class: String, input: String, most_severe_consequence: String)

case class Transcript(amino_acids: String, distance: Option[Long], cdna_end: Option[Long], cdna_start: Option[Long], cds_end: Option[Long], cds_start: Option[Long], codons: String, consequence_terms: List[String], flags: List[String], gene_id: String, impact: String, protein_end: Option[Long], protein_start: Option[Long], strand: Option[Long], transcript_id: String, variant_allele: String)

case class Transcript2(impact: String, consequence_terms: Seq[String])

case class VepAnnotTrunc(allele_string: String, assembly_name: String, end: Long, id: String, input: String, most_severe_consequence: String, seq_region_name: String, start: Long, strand: Long, transcript_consequences: Seq[Transcript])

case class VepAnnotTrunc2(input: String, transcript_consequences: Seq[Transcript2])

case class VariantVep(vid: String, vcontig: String, vstart: Int, vreference: String, genotypes: Seq[Call], vepCall: String)
case class VepOccurrence(donorId: String, vend: Int, projectId: String, start: Int, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, vepCall:String)

case class Occurrence(donorId: String, vend: Int, projectId: String, start: Int, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, motif_feature_consequences: Seq[Motif0], allele_string: String, seq_region_name: String,
  transcript_consequences: Seq[Transcript], regulatory_feature_consequences: Seq[Regulatory0], assembly_name: String,
    intergenic_consequences: Seq[Intergenic0], end: Long, variant_class: String, input: String, most_severe_consequence: String)

class VepLoader(spark: SparkSession) extends Serializable {

  import spark.implicits._

  val options = "--distance 100000" //--no_check_variants_order --distance 100000"
  val vepCommandLine = s"${Config.vepHome} --cache -o STDOUT --assembly GRCh38 --json $options --offline --no_stats --format vcf --dir_cache ${Config.vepCache}"

  val formatOccurrenceUdf = udf { 
	(c: String, s: Int, r: String, a1: String, a2: String) => s"$c\t$s\t.\t$r\t$a1\t.\tPASS\t.\n$c\t$s\t.\t$r\t$a2\t.\tPASS\t."
  }

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

  def testVep = {
  	val calls = spark.sparkContext.parallelize(
		List("1\t881907\t881906\t-/C\t+","1\t881907\t881906\t-/C\t+","1\t881907\t881906\t-/C\t+",
			 "1\t881907\t881906\t-/C\t+","1\t881907\t881906\t-/C\t+","1\t881907\t881906\t-/C\t+"),6)
	spark.read.json(calls.mapPartitions(it => callVep(it.toList).iterator).toDF().as[String])
  }

  def genotypesJSON(genotypes: Seq[Genotype]): String =
    genotypes.map(s => s"""{"sample": s.getSampleName, "call": ${callCategory(s)} }""").mkString("[",",","]")

  def variantJSON(v: VariantContext): String = {
    val (i, c, s, r, g) = (v.getID, v.getContig, v.getStart, v.getReference, 
      genotypesJSON(v.getGenotypes.iterator.asScala.toSeq))
    s"""{"id": $i, "contig": $c, "start": $s, "reference": $r, "genotypes": $g, "vepCall": ${formatVariantContext(v)}}"""    
  }

  def loadAnnotations(variants: RDD[VariantContext]): (Dataset[VariantVep], Dataset[VepAnnotation]) = {
    val vindexed = variants.map(v => {
        val genotypes = v.getGenotypes.iterator.asScala.toSeq.map(s => Call(s.getSampleName, callCategory(s)))
        VariantVep(v.getID, v.getContig, v.getStart, v.getReference.toString, genotypes, formatVariantContext(v))
    }).toDF().as[VariantVep]
    val annots = spark.read.json(vindexed.mapPartitions(it => 
		callVep(it.map(i => i.vepCall).toList).iterator).toDF().as[String])
		.as[VepAnnotation]
	(vindexed, annots)
	//vindexed.join(annots, $"vepCall" === $"input").drop("vepCall").as[AnnotatedVariant0]
  }

  def loadOccurrences(variants: Dataset[OccurrencesTop]): (Dataset[VepOccurrence], Dataset[VepAnnotTrunc]) = {
	val vindex = variants.withColumn("vepCall", 
		formatOccurrenceUdf(col("chromosome"), col("start"), col("Reference_Allele"), col("Tumor_Seq_Allele1"), col("Tumor_Seq_Allele2"))).as[VepOccurrence]
  	val annots = spark.read.json(vindex.sort($"chromosome", $"start").mapPartitions(it => 
		callVep(it.map(i => i.vepCall).toList).iterator).toDF().as[String])
	
	//vindex.join(annots, $"vepCall" === $"input").drop("vepCall").as[Occurrence]
  	(vindex, annots.as[VepAnnotTrunc])
  }

  private def callCategory(g: Genotype): Int = g match {
    case c if g.isHet => 1
    case c if g.isHomVar => 2
    case _ => 0
  }
}
