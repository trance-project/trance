package sparkutils.loader

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row,DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import htsjdk.variant.variantcontext.{Genotype, VariantContext}
import sparkutils.Config
import org.apache.spark.rdd.RDD
import java.io.ByteArrayInputStream
import java.math.BigDecimal
import scala.sys.process._
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, ArrayType, LongType, DoubleType, StringType, IntegerType, StructField, StructType}
import sparkutils.skew.SkewDataset._
import sparkutils.Config
import org.apache.spark.broadcast.Broadcast

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

case class TranscriptQuant(amino_acids: String, distance: Option[Long], cdna_end: Option[Long], cdna_start: Option[Long], cds_end: Option[Long], cds_start: Option[Long], codons: String, consequence_terms: List[Double], flags: List[String], gene_id: String, impact: Double, protein_end: Option[Long], protein_start: Option[Long], strand: Option[Long], transcript_id: String, variant_allele: String)

case class Transcript3(amino_acids: String, distance: Long, cdna_end: Long, cdna_start: Long, cds_end: Long, cds_start: Long, codons: String, consequence_terms: List[String], flags: List[String], gene_id: String, impact: String, protein_end: Long, protein_start: Long, strand: Long, transcript_id: String, variant_allele: String)

case class Transcript2(impact: String, consequence_terms: Seq[String])

case class VepAnnotTrunc(vid: String, allele_string: String, assembly_name: String, end: Long, input: String, most_severe_consequence: String, seq_region_name: String, start: Long, strand: Long, transcript_consequences: Seq[Transcript])

case class VepAnnotTrunc2(input: String, transcript_consequences: Seq[Transcript2])

case class VariantVep(vid: String, vcontig: String, vstart: Int, vreference: String, genotypes: Seq[Call], vepCall: String)
case class VepOccurrence(donorId: String, oid: String, vend: Int, projectId: String, vstart: Int, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, vepCall:String)

case class OccurrenceNulls(oid: String, donorId: String, vend: Int, projectId: String, vstart: Int, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, allele_string: Option[String], assembly_name: Option[String], end: Option[Long], vid: Option[String], input: Option[String], most_severe_consequence: Option[String], seq_region_name: Option[String], start: Option[Long], strand: Option[Long], transcript_consequences: Option[Seq[Transcript]])

case class Occurrence(oid: String, donorId: String, vend: Long, projectId: String, vstart: Long, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, allele_string: String, assembly_name: String, end: Long, vid: String, input: String, most_severe_consequence: String, seq_region_name: String, start: Long, strand: Long, transcript_consequences: Option[Seq[Transcript]])

case class OccurrenceMapped(aliquot_id: String, oid: String, donorId: String, vend: Long, projectId: String, vstart: Long, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, allele_string: String, assembly_name: String, end: Long, vid: String, input: String, most_severe_consequence: String, seq_region_name: String, start: Long, strand: Long, transcript_consequences: Option[Seq[Transcript]])

case class OccurrQuant(oid: String, donorId: String, vend: Long, projectId: String, vstart: Long, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, allele_string: String, assembly_name: String, end: Long, vid: String, input: String, most_severe_consequence: String, seq_region_name: String, start: Long, strand: Long, transcript_consequences: Option[Seq[TranscriptQuant]])

case class Occurrence2(oid: String, donorId: String, vend: Long, projectId: String, vstart: Long, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, allele_string: String, assembly_name: String, end: Long, vid: String, input: String, most_severe_consequence: String, seq_region_name: String, start: Long, strand: Long, transcript_consequences: Seq[Transcript3])

case class OccurrDict1(oid: String, donorId: String, vend: Long, projectId: String, vstart: Long, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, allele_string: String, assembly_name: String, end: Long, vid: String, input: String, most_severe_consequence: String, seq_region_name: String, start: Long, strand: Long, transcript_consequences: String)

case class OccurrTransDict2(_1: String, amino_acids: String, distance: Long, cdna_end: Long, cdna_start: Long, cds_end: Long, cds_start: Long, codons: String, consequence_terms: String, flags: List[String], gene_id: String, impact: String, protein_end: Long, protein_start: Long, strand: Long, transcript_id: String, variant_allele: String)

case class OccurrTransConseqDict3(_1: String, element: String)

class VepLoader(spark: SparkSession) extends Serializable {

  import spark.implicits._

  val options = "--distance 100000" //--no_check_variants_order --distance 100000"
  val vepCommandLine = s"${Config.vepHome} --cache -o STDOUT --assembly GRCh38 --json $options --offline --no_stats --format vcf --dir_cache ${Config.vepCache}"

  val formatOccurrenceUdf = udf { 
	(c: String, s: Int, i: String, r: String, a1: String, a2: String) => s"$c\t$s\t$i\t$r\t$a1\t.\tPASS\t.\n$c\t$s\t$i\t$r\t$a2\t.\tPASS\t."
  }

  val extractIdUdf = udf {
	(s: String) => s.split("\t")(2) 
  }

  val quantifyConsequence = udf { s: String => s match {
    case "HIGH" => .8
    case "MODERATE" => .5
    case "LOW" => .3
    case "MODIFIER" => .1
  }}

  val castBigDecimal = udf { s: String => new BigDecimal(s) }

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
	val vindex = variants.withColumn("oid", monotonically_increasing_id().cast(StringType)).withColumn("vepCall", 
		formatOccurrenceUdf(col("chromosome"), col("start"), col("oid"), col("Reference_Allele"), col("Tumor_Seq_Allele1"), col("Tumor_Seq_Allele2")))
	 .withColumnRenamed("start", "vstart").as[VepOccurrence]
 	
	val annots = spark.read.json(vindex.sort($"chromosome", $"start").mapPartitions(it => 
		callVep(it.map(i => i.vepCall).toList).iterator).toDF().as[String]).withColumn("vid", extractIdUdf(col("input"))).as[VepAnnotTrunc]
	
	//vindex.join(annots, $"vepCall" === $"input").drop("vepCall").as[Occurrence]
  	(vindex, annots)
  }

  def buildOccurrences(inputvariants: Dataset[VepOccurrence], annots: Dataset[VepAnnotTrunc]): Dataset[Occurrence] = {
	val nullFill = Map("allele_string" -> "null", "assembly_name" -> "null", "end" -> -1, "vid" -> "null", 
		"input" -> "null", "most_severe_consequence" -> "null", "seq_region_name" -> "null", "start" -> -1, 
		"strand" -> -1)

	//val annots = spark.read.json(path).as[VepAnnotTrunc]
	inputvariants.join(annots, $"oid" === $"vid", "left_outer").drop("vepCall")
		.as[OccurrenceNulls].na.fill(nullFill).as[Occurrence]
  }

  def finalize(occur: Dataset[Occurrence]): Dataset[Occurrence2] = {
  	occur.map{ o => Occurrence2(o.oid, o.donorId, o.vend, o.projectId, o.vstart, o.Reference_Allele, o.Tumor_Seq_Allele1, o.Tumor_Seq_Allele2, 
  		o.chromosome, o.allele_string, o.assembly_name, o.end, o.vid, o.input, o.most_severe_consequence, o.seq_region_name, o.start, o.strand, 
  		o.transcript_consequences match {
  		  case Some(tc) => tc.map(t => Transcript3(t.amino_acids, t.distance match { case Some(l:Long) => l; case _ => -1 },
  		t.cdna_end match { case Some(l:Long) => l; case _ => -1 }, t.cdna_start match { case Some(l:Long) => l; case _ => -1 }, 
  		t.cds_end match { case Some(l:Long) => l; case _ => -1 }, t.cds_start match { case Some(l:Long) => l; case _ => -1 }, 
  		t.codons, t.consequence_terms, t.flags, t.gene_id, t.impact, t.protein_end match { case Some(l:Long) => l; case _ => -1 }, 
  		t.protein_start match { case Some(l:Long) => l; case _ => -1 }, t.strand match { case Some(l:Long) => l; case _ => -1 }, 
  		t.transcript_id, t.variant_allele))
  		  case _ => Seq()
  		  })
  		
  	}.as[Occurrence2].repartition(Config.maxPartitions)

  }

  
      //   val (dfull, hk) = heavyKeys[K](nkey)
      // if (hk.nonEmpty){
      //   val hkeys = dfull.sparkSession.sparkContext.broadcast(hk)
      //   (dfull.lfilter[K](col(nkey), hkeys), dfull.hfilter[K](col(nkey), hkeys), Some(nkey), hkeys).equiJoin[S,K](right, usingColumns, joinType)

  def shred(occur: Dataset[Occurrence]): (Dataset[OccurrDict1], Dataset[OccurrTransDict2], Dataset[OccurrTransConseqDict3]) = {

    val occurReparts = occur.repartition(Config.maxPartitions)
	
  	val dict1 = occurReparts.map{ o => OccurrDict1(o.oid, o.donorId, o.vend, o.projectId, o.vstart, o.Reference_Allele, o.Tumor_Seq_Allele1, o.Tumor_Seq_Allele2, 
  		o.chromosome, o.allele_string, o.assembly_name, o.end, o.vid, o.input, o.most_severe_consequence, o.seq_region_name, o.start, o.strand, o.oid)
  	}.as[OccurrDict1]
  	
  	val tmp2 = occurReparts.flatMap{ o => o.transcript_consequences match {
  	  case Some(tc) => tc.zipWithIndex.map{ case (t, id) => (o.oid, id, t) }
  	  case _ => Seq()
  	}}

  	val dict2 = tmp2.map{ case (id1, id2, t) => OccurrTransDict2(id1, t.amino_acids, t.distance match { case Some(l:Long) => l; case _ => -1 },
  		t.cdna_end match { case Some(l:Long) => l; case _ => -1 }, t.cdna_start match { case Some(l:Long) => l; case _ => -1 }, 
  		t.cds_end match { case Some(l:Long) => l; case _ => -1 }, t.cds_start match { case Some(l:Long) => l; case _ => -1 }, 
  		t.codons, s"${id1}_${id2}", t.flags, t.gene_id, t.impact, t.protein_end match { case Some(l:Long) => l; case _ => -1 }, 
  		t.protein_start match { case Some(l:Long) => l; case _ => -1 }, t.strand match { case Some(l:Long) => l; case _ => -1 }, 
  		t.transcript_id, t.variant_allele)}.as[OccurrTransDict2]

  	val dict3 = tmp2.flatMap{ case (id1, id2, t) => 
  		t.consequence_terms.map(c => OccurrTransConseqDict3(s"${id1}_${id2}", c)) }.as[OccurrTransConseqDict3]

    (dict1, dict2, dict3)

  }

  def shredSkew(occur: Dataset[Occurrence]): ((Dataset[OccurrDict1], Dataset[OccurrDict1]), 
    (Dataset[OccurrTransDict2], Dataset[OccurrTransDict2], Option[String], Broadcast[Set[String]]),
    (Dataset[OccurrTransConseqDict3], Dataset[OccurrTransConseqDict3], Option[String], Broadcast[Set[String]])) = {

    val (dict1, dict2, dict3) = shred(occur)

    val skew_dict1 = (dict1, dict1.empty)
    val skew_dict2 = (dict2, dict2.empty).repartition[String](col("_1"))
    val skew_dict3 = (dict3, dict3.empty).repartition[String](col("_1"))
    (skew_dict1, skew_dict2, skew_dict3)

  }

  private def callCategory(g: Genotype): Int = g match {
    case c if g.isHet => 1
    case c if g.isHomVar => 2
    case _ => 0
  }

}
