package framework.examples.genomic

import framework.common._
import framework.examples.Query

trait GenomicSchema extends Query {

  def loadTables(shred: Boolean = false, skew: Boolean = false): String = {
    if (shred)
      s"""|val vloader = new VariantLoader(spark, "/mnt/app_hdd/data/Data/Variants/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf")
          |val (variants, genotypes) = vloader.shredDS
          |val IBag_variants__D = variants
          |val IDict_variants__D_genotypes = genotypes
          |//IBag_variants__D.cache
          |//IBag_variants__D.count
          |//IDict_variants__D_genotypes.cache
          |//IDict_variants__D_genotypes.count
          |
          |val gtfLoader = new GTFLoader(spark, "/mnt/app_hdd/data/Data/Map/Homo_sapiens.GRCh37.87.chr.gtf")
          |val Gtfs = gtfLoader.loadDS
          |val IBag_Gtfs__D = Gtfs
          |//IBag_Gtfs__D.cache
          |//IBag_Gtfs__D.count
          |""".stripMargin

    else
      s"""|val vloader = new VariantLoader(spark, "/mnt/app_hdd/data/Data/Variants/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf")
          |val vcf = vloader.loadDS
          |//variants.cache
          |//variants.count
          |
          |val gtfLoader = new GTFLoader(spark, "/mnt/app_hdd/data/Data/Map/Homo_sapiens.GRCh37.87.chr.gtf")
          |val genes = gtfLoader.loadDS
          |//genes.cache
          |//genes.count
          |""".stripMargin
  }

  // define the types, which would reflect the case classes from your variant loader
  val genoType = TupleType("g_sample" -> StringType, "call" -> DoubleType)

  val variantType = TupleType(
    "contig" -> StringType,
    "start" -> IntType,
    "reference" -> StringType,
    "alternate" -> StringType,
    "genotypes" -> BagType(genoType))

  val gtfType = TupleType(
    "g_contig" -> StringType,
    "g_start" -> IntType,
    "g_end" -> IntType,
    "g_gene_name" -> StringType, 
    "g_gene_id" -> StringType
  )

  val geneType = TupleType("name" -> StringType)
  val pathwayType = TupleType("p_name" -> StringType, "url" -> StringType, "gene_set" -> BagType(geneType))

  val metaType = TupleType("m_sample" -> StringType, "family_id" -> StringType, "population" -> StringType, "gender" -> StringType)

  // in replacement of gtf file but this one is smaller
  val genesType = TupleType(
    "name"->StringType,
    "description"-> StringType,
    "chrom"-> StringType,
    "g_type" -> StringType,
    "start_hg19" -> IntType,
    "end_hg19" -> IntType,
    "strand" -> StringType,
    "ts_id" -> StringType,
    "gene_type" -> StringType,
    "gene_status" -> StringType,
    "loci_level" -> IntType,
    "alias_symbol" -> StringType,
    "official_name"-> StringType)


  // define references to these types
  val variants = BagVarRef("variants", BagType(variantType))
  val vr = TupleVarRef("v", variantType)
  val gr = TupleVarRef("g", genoType)

  val gtfs = BagVarRef("Gtfs", BagType(gtfType))
  val gtfr = TupleVarRef("gtf", gtfType)

  val pathways = BagVarRef("pathways", BagType(pathwayType))
  val pr = TupleVarRef("p", pathwayType)
  val ger = TupleVarRef("ge", geneType)

  val metadata = BagVarRef("metadata", BagType(metaType))
  val mr = TupleVarRef("m", metaType)

  val gtfss = BagVarRef("genes", BagType(genesType))
  val gtfrr = TupleVarRef("gtff", genesType)


}
    