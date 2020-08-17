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


class VepLoader(spark: SparkSession) extends Serializable {

  import spark.implicits._

  val options = "--distance 500000 --sift b --polyphen b --numbers "//--check_existing --clin_sig_allele 0 --no_check_alleles --af --af_1kg --af_esp --af_gnomad --failed 1"
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

  def getString(e: Option[String]): String = e match { case Some(s) => s; case _ => "null"; }
  def getLong(e: Option[Long]): Long = e match { case Some(l) => l; case _ => 0; }
  def getDouble(e: Option[Double]): Double = e match { case Some(d) => d; case _ => 0.0 }

  def loadOccurrencesMid(variants: Dataset[OccurrencesTop]): Dataset[OccurrenceMid] = {
	val nullFill = Map("allele_string" -> "null", "assembly_name" -> "null", "end" -> -1, "vid" -> "null", 
		"input" -> "null", "most_severe_consequence" -> "null", "seq_region_name" -> "null", "start" -> -1, 
		"strand" -> -1)

	val vindex = variants.withColumn("oid", monotonically_increasing_id().cast(StringType)).withColumn("vepCall", 
		formatOccurrenceUdf(col("chromosome"), col("start"), col("oid"), col("Reference_Allele"), col("Tumor_Seq_Allele1"), col("Tumor_Seq_Allele2")))
	 .withColumnRenamed("start", "vstart").as[VepOccurrence]
 	
	val annots = spark.read.json(vindex.sort($"chromosome", $"start").mapPartitions(it => 
		callVep(it.map(i => i.vepCall).toList).iterator).toDF().as[String])
		.withColumn("vid", extractIdUdf(col("input")))
		.as[VepAnnotMid]
	
	vindex.join(annots, $"oid" === $"vid", "left_outer").drop("vepCall")
		.as[OccurrenceMidNulls].na.fill(nullFill).as[OccurrenceMidPartialNulls].map(o => 
			OccurrenceMid(o.oid, o.donorId, o.vend, o.projectId, o.vstart, o.Reference_Allele, o.Tumor_Seq_Allele1, 
				o.Tumor_Seq_Allele2, o.chromosome, o.allele_string,
			    o.assembly_name, o.end, o.vid, o.input, o.most_severe_consequence, o.seq_region_name, o.start, o.strand,
				o.transcript_consequences match { case Some(tc) => tc.map(t => TranscriptFull2(o.donorId, getString(t.amino_acids), getLong(t.cdna_end), 
				getLong(t.cdna_start), getLong(t.cds_end), getLong(t.cds_start), getString(t.codons), 
				t.consequence_terms match { case Some(ct) => ct.map(x => x match { case null => "null"; case _ => x }); case _ => Seq() },
				getLong(t.distance), getString(t.exon), t.flags match { case Some(tf) => tf; case _ => Seq() }, 
				getString(t.gene_id), getString(t.impact), getString(t.intron), getString(t.polyphen_prediction),
				getDouble(t.polyphen_score), getLong(t.protein_end), getLong(t.protein_start), getString(t.sift_prediction), 
				getDouble(t.sift_score), getLong(t.strand), 
				getString(t.transcript_id), getString(t.variant_allele))); case _ => Seq()})
		).as[OccurrenceMid]
  }

  def loadOccurrencesFull(variants: Dataset[OccurrencesTop]): Dataset[OccurrenceFull] = {
	//(Dataset[VepOccurrence], Dataset[VepAnnotFull2]) = {//Dataset[OccurrenceEverything]) = {
	val nullFill = Map("allele_string" -> "null", "assembly_name" -> "null", "end" -> -1, "vid" -> "null", 
		"input" -> "null", "most_severe_consequence" -> "null", "seq_region_name" -> "null", "start" -> -1, 
		"strand" -> -1)

	val vindex = variants.withColumn("oid", monotonically_increasing_id().cast(StringType)).withColumn("vepCall", 
		formatOccurrenceUdf(col("chromosome"), col("start"), col("oid"), col("Reference_Allele"), col("Tumor_Seq_Allele1"), col("Tumor_Seq_Allele2")))
	 .withColumnRenamed("start", "vstart").as[VepOccurrence]
 	
	val annots = spark.read.json(vindex.sort($"chromosome", $"start").mapPartitions(it => 
		callVep(it.map(i => i.vepCall).toList).iterator).toDF().as[String])
		.withColumn("vid", extractIdUdf(col("input")))
		.as[VepAnnotFull]

	vindex.join(annots, $"oid" === $"vid", "left_outer").drop("vepCall")
		.as[OccurrenceFullNulls].na.fill(nullFill).as[OccurrenceFullPartialNulls].map(o => 
			OccurrenceFull(o.oid, o.donorId, o.vend, o.projectId, o.vstart, o.Reference_Allele, o.Tumor_Seq_Allele1, o.Tumor_Seq_Allele2, o.chromosome, o.allele_string,
			    o.assembly_name, o.end, o.vid, o.input, o.most_severe_consequence, o.seq_region_name, o.start, o.strand,
				o.colocated_variants match { case Some(cv) => cv.map(c => CoLocated2(o.donorId, getString(c.AA), getString(c.AFR), getString(c.AMR), getString(c.EA), 
				getString(c.EAS), getString(c.EUR), getString(c.SAS), getString(c.allele_string), 
				c.clin_sig match { case Some(cg) => cg.map(x => x match { case null => "null"; case _ => x}); case _ => Seq()},
				getString(c.clin_sig_allele), getLong(c.end), getString(c.gnomAD), getString(c.gnomAD_AFR), getString(c.gnomAD_AMR), getString(c.gnomAD_ASJ), 
				getString(c.gnomAD_EAS), getString(c.gnomAD_FIN), getString(c.gnomAD_NFE), getString(c.gnomAD_OTH), getString(c.gnomAD_SAS),
				getString(c.id), getString(c.minor_allele), getDouble(c.minor_allele_freq), getLong(c.phenotype_or_disease), 
				c.pubmed match { case Some(ps) => ps; case _ => Seq() }, getString(c.seq_region_name), getLong(c.somatic), getLong(c.start), getLong(c.strand))); 
					case _ => Seq() }, 
				o.transcript_consequences match { case Some(tc) => tc.map(t => TranscriptFull2(o.donorId, getString(t.amino_acids), getLong(t.cdna_end), 
				getLong(t.cdna_start), getLong(t.cds_end), getLong(t.cds_start), getString(t.codons), 
				t.consequence_terms match { case Some(ct) => ct.map(x => x match { case null => "null"; case _ => x }); case _ => Seq() },
				getLong(t.distance), getString(t.exon), t.flags match { case Some(tf) => tf; case _ => Seq() }, 
				getString(t.gene_id), getString(t.impact), getString(t.intron), getString(t.polyphen_prediction),
				getDouble(t.polyphen_score), getLong(t.protein_end), getLong(t.protein_start), getString(t.sift_prediction), getDouble(t.sift_score), getLong(t.strand), 
				getString(t.transcript_id), getString(t.variant_allele))); case _ => Seq()})
		).as[OccurrenceFull]
	//(vindex, annots)
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
  	occur.map{ o => Occurrence2(o.oid, o.donorId, o.donorId, o.vend, o.projectId, o.vstart, o.Reference_Allele, o.Tumor_Seq_Allele1, o.Tumor_Seq_Allele2, 
  		o.chromosome, o.allele_string, o.assembly_name, o.end, o.vid, o.input, o.most_severe_consequence, o.seq_region_name, o.start, o.strand, 
  		o.transcript_consequences match {
  		  case Some(tc) => tc.map(t => Transcript3(o.donorId, t.amino_acids, t.distance match { case Some(l:Long) => l; case _ => -1 },
  		t.cdna_end match { case Some(l:Long) => l; case _ => -1 }, t.cdna_start match { case Some(l:Long) => l; case _ => -1 }, 
  		t.cds_end match { case Some(l:Long) => l; case _ => -1 }, t.cds_start match { case Some(l:Long) => l; case _ => -1 }, 
  		t.codons, t.consequence_terms, t.flags, t.gene_id, t.impact, t.protein_end match { case Some(l:Long) => l; case _ => -1 }, 
  		t.protein_start match { case Some(l:Long) => l; case _ => -1 }, t.strand match { case Some(l:Long) => l; case _ => -1 }, 
  		t.transcript_id, t.variant_allele))
  		  case _ => Seq()
  		  })
  		
  	}.as[Occurrence2].repartition(Config.maxPartitions)

  }

  def finalize(occur: Dataset[Occurrence], biospec: Dataset[Biospec]): Dataset[Occurrence2] = {
    val biooccur = occur.join(biospec, $"donorId" === $"bcr_patient_uuid").as[OccurrenceBiospec]
    biooccur.map{ o => Occurrence2(o.oid, o.bcr_aliquot_uuid, o.donorId, o.vend, o.projectId, o.vstart, o.Reference_Allele, o.Tumor_Seq_Allele1, o.Tumor_Seq_Allele2, 
      o.chromosome, o.allele_string, o.assembly_name, o.end, o.vid, o.input, o.most_severe_consequence, o.seq_region_name, o.start, o.strand, 
      o.transcript_consequences match {
        case Some(tc) => tc.map(t => Transcript3(o.bcr_aliquot_uuid, t.amino_acids, t.distance match { case Some(l:Long) => l; case _ => -1 },
      t.cdna_end match { case Some(l:Long) => l; case _ => -1 }, t.cdna_start match { case Some(l:Long) => l; case _ => -1 }, 
      t.cds_end match { case Some(l:Long) => l; case _ => -1 }, t.cds_start match { case Some(l:Long) => l; case _ => -1 }, 
      t.codons, t.consequence_terms, t.flags, t.gene_id, t.impact, t.protein_end match { case Some(l:Long) => l; case _ => -1 }, 
      t.protein_start match { case Some(l:Long) => l; case _ => -1 }, t.strand match { case Some(l:Long) => l; case _ => -1 }, 
      t.transcript_id, t.variant_allele))
        case _ => Seq()
        })
      
    }.as[Occurrence2].repartition(Config.maxPartitions)
  }

  
  def shred(occur: Dataset[Occurrence2]): (Dataset[OccurrDict1], Dataset[OccurrTransDict2], Dataset[OccurrTransConseqDict3]) = {
	
  	val dict1 = occur.map{ o => OccurrDict1(o.oid, o.aliquotId, o.donorId, o.vend, o.projectId, o.vstart, o.Reference_Allele, o.Tumor_Seq_Allele1, o.Tumor_Seq_Allele2, 
  		o.chromosome, o.allele_string, o.assembly_name, o.end, o.vid, o.input, o.most_severe_consequence, o.seq_region_name, o.start, o.strand, o.oid)
  	}.as[OccurrDict1]
  	
  	val tmp2 = occur.flatMap{ o => o.transcript_consequences.zipWithIndex.map{ case (t, id) => (o.oid, id, t) } }

  	val dict2 = tmp2.map{ case (id1, id2, t) => OccurrTransDict2(id1, t.aliquot_id, t.amino_acids, 
		t.distance, t.cdna_end, t.cdna_start, t.cds_end, t.cds_start, t.codons, s"${id1}_${id2}", t.flags, 
    t.gene_id, t.impact, t.protein_end, t.protein_start, t.ts_strand,
  	t.transcript_id, t.variant_allele)}.as[OccurrTransDict2].repartition(col("_1"))

  	val dict3 = tmp2.flatMap{ case (id1, id2, t) => 
  		t.consequence_terms.map(c => OccurrTransConseqDict3(s"${id1}_${id2}", c)) }.as[OccurrTransConseqDict3]
      .repartition(col("_1"))
      
    (dict1, dict2, dict3)

  }

  def shredMid(occur: Dataset[OccurrenceMid]): (Dataset[OccurrDict1], Dataset[OccurTransDict2Mid], Dataset[OccurrTransConseqDict3]) = {
  
    val dict1 = occur.map{ o => OccurrDict1(o.oid, o.donorId, o.donorId, o.vend, o.projectId, o.vstart, o.Reference_Allele, o.Tumor_Seq_Allele1, o.Tumor_Seq_Allele2, 
      o.chromosome, o.allele_string, o.assembly_name, o.end, o.vid, o.input, o.most_severe_consequence, o.seq_region_name, o.start, o.strand, o.oid)
    }.as[OccurrDict1]
    
    val tmp2 = occur.flatMap{ o => o.transcript_consequences.zipWithIndex.map{ case (t, id) => (o.oid, id, t) } }

    val dict2 = tmp2.map{ case (id1, id2, t) => OccurTransDict2Mid(id1, t.case_id, t.amino_acids, 
    t.cdna_end, t.cdna_start, t.cds_end, t.cds_start, t.codons, s"${id1}_${id2}", t.distance, t.exon, t.flags, 
    t.gene_id, t.impact, t.intron, t.polyphen_prediction, t.polyphen_score, t.protein_end, t.protein_start, 
    t.sift_prediction, t.sift_score, t.ts_strand,
    t.transcript_id, t.variant_allele)}.as[OccurTransDict2Mid].repartition(col("_1"))

    val dict3 = tmp2.flatMap{ case (id1, id2, t) => 
      t.consequence_terms.map(c => OccurrTransConseqDict3(s"${id1}_${id2}", c)) }.as[OccurrTransConseqDict3]
      .repartition(col("_1"))
      
    (dict1, dict2, dict3)

  }

  def shredSkew(occur: Dataset[Occurrence2]): ((Dataset[OccurrDict1], Dataset[OccurrDict1]), 
    (Dataset[OccurrTransDict2], Dataset[OccurrTransDict2], Option[String], Broadcast[Set[String]]),
    (Dataset[OccurrTransConseqDict3], Dataset[OccurrTransConseqDict3], Option[String], Broadcast[Set[String]])) = {

    val (dict1, dict2, dict3) = shred(occur)

    val skew_dict1 = (dict1, dict1.empty)
    val skew_dict2 = (dict2, dict2.empty).repartition[String](col("_1"))
    val skew_dict3 = (dict3, dict3.empty).repartition[String](col("_1"))
    (skew_dict1, skew_dict2, skew_dict3)

  }

  def shredSkewMid(occur: Dataset[OccurrenceMid]): ((Dataset[OccurrDict1], Dataset[OccurrDict1]), 
    (Dataset[OccurTransDict2Mid], Dataset[OccurTransDict2Mid], Option[String], Broadcast[Set[String]]),
    (Dataset[OccurrTransConseqDict3], Dataset[OccurrTransConseqDict3], Option[String], Broadcast[Set[String]])) = {

    val (dict1, dict2, dict3) = shredMid(occur)

    val skew_dict1 = (dict1, dict1.empty)
    val skew_dict2 = (dict2, dict2.empty, Some("_1"), spark.sparkContext.broadcast(Set.empty[String]))//.repartition[String](col("_1"))
    val skew_dict3 = (dict3, dict3.empty, Some("_1"), spark.sparkContext.broadcast(Set.empty[String]))//.repartition[String](col("_1"))
    (skew_dict1, skew_dict2, skew_dict3)

  }

  private def callCategory(g: Genotype): Int = g match {
    case c if g.isHet => 1
    case c if g.isHomVar => 2
    case _ => 0
  }

}
