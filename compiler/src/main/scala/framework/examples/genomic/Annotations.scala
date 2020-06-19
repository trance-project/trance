package framework.examples.genomic

import framework.common._
import framework.examples.Query

trait Vep {

  val element = TupleType("element" -> StringType)
  
  val intergenic = TupleType("consequence_terms" -> BagType(element),
    "impact" -> StringType, "variant_allele" -> StringType)
  
  val motif = TupleType("bp_overlap" -> LongType,
    "consequence_terms" -> BagType(element),
    "impact" -> StringType, "variant_allele" -> StringType,
    "motif_feature_id" -> StringType, "percentage_overlap" -> DoubleType)
  
  val regulatory = TupleType("bp_overlap" -> LongType,
    "consequence_terms" -> BagType(element),
    "impact" -> StringType, "variant_allele" -> StringType,
    "regulatory_feature_id" -> StringType, "percentage_overlap" -> DoubleType)
  
  val transcript = TupleType(
    "appris" -> StringType,
    "biotype" -> StringType,
    "bp_overlap" -> LongType,
    "canonical" -> LongType,
    "ccds" -> StringType,
    "cdna_end" -> LongType,
    "cdna_start" -> LongType,
    "cds_end" -> LongType,
    "cds_start" -> LongType,
    "consequence_terms" -> BagType(element),
    "distance" -> LongType,
    "domains" -> BagType(TupleType("db" -> StringType, "name" -> StringType)),
    "exon" -> StringType,
    "gene_id" -> StringType,
    "gene_symbol" -> StringType,
    "gene_symbol_source" -> StringType,
    "hgnc_id" -> StringType,
    "impact" -> StringType,
    "intron" -> StringType,
    "mane" -> StringType,
    "percentage_overlap" -> DoubleType,
    "protein_end" -> LongType,
    "protein_id" -> StringType,
    "protein_start" -> LongType,
    "strand" -> LongType,
    "swissprot" -> BagType(element),
    "transcript_id" -> StringType,
    "tsl" -> LongType,
    "uniparc" -> BagType(element),
    "variant_allele" -> StringType)

  val vep_type = TupleType("allele_string" -> StringType, 
    "assembly_name" -> StringType, "end" -> LongType, "id" -> StringType,
    "input" -> StringType, "intergenic_consequences" -> BagType(intergenic),
    "most_severe_consequence" -> StringType,
    "motif_feature_consequences" -> BagType(motif),
    "regulatory_feature_consequences" -> BagType(regulatory),
    "seq_region_name" -> StringType,
    "start" -> LongType, 
    "transcript_consequences" -> BagType(transcript),
    "variant_class" -> StringType)

  val vcf_vep_type = TupleType(
    "id" -> StringType, "contig" -> StringType, "start" -> IntType,
    "reference" -> StringType, "genotypes" -> 
      BagType(TupleType("g_sample" -> StringType, "call" -> IntType)),
    "allele_string" -> StringType, 
    "assembly_name" -> StringType, "end" -> LongType, "id" -> StringType,
    "input" -> StringType, "intergenic_consequences" -> BagType(intergenic),
    "most_severe_consequence" -> StringType,
    "motif_feature_consequences" -> BagType(motif),
    "regulatory_feature_consequences" -> BagType(regulatory),
    "seq_region_name" -> StringType,
    "start" -> LongType, 
    "transcript_consequences" -> BagType(transcript),
    "variant_class" -> StringType
  )

  val occurence_type = TupleType((vcf_vep_type.attrTps - "genotypes") ++
    Map("donorId" -> StringType, "mutation" -> IntType))

}