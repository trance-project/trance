package framework.examples

import framework.common._

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
          |val cloader = new TGenomesLoader(spark)
          |val IBag_metadata__D = cloader.load("/mnt/app_hdd/data/Data/Phenotype/1000g.csv")
          |//IBag_metadata__D.cache
          |//IBag_metadata__D.count
          |
          |val gtfLoader = new GTFLoader(spark, "/mnt/app_hdd/data/Data/Map/Homo_sapiens.GRCh37.87.chr.gtf")
          |val Gtfs = gtfLoader.loadDS
          |val IBag_Gtfs__D = Gtfs
          |//IBag_Gtfs__D.cache
          |//IBag_Gtfs__D.count
          |
          |val pathwayLoader = new PathwayLoader(spark, "/mnt/app_hdd/data/Data/Pathway/c2.cp.v7.1.symbols.gmt")
          |val (pathways, geneSet) = pathwayLoader.shredDS
          |val IBag_pathways__D = pathways
          |val IDict_pathways__D_gene_set = geneSet
          |//IBag_pathways__D.cache
          |//IBag_pathways__D.count
          |
          |val geneLoader = new GeneLoader(spark)
          |val genes = geneLoader.load("/mnt/app_hdd/data/Data/Map/All_human_genes")
          |val IBag_genes__D = genes
          |
          |""".stripMargin
    else
      s"""|val vloader = new VariantLoader(spark, "/mnt/app_hdd/data/Data/Variants/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf")
          |val variants = vloader.loadDS
          |//variants.cache
          |//variants.count
          |
          |val cloader = new TGenomesLoader(spark)
          |val metadata = cloader.load("/mnt/app_hdd/data/Data/Phenotype/1000g.csv")
          |//metadata.cache
          |//metadata.count
          |
          |val gtfLoader = new GTFLoader(spark, "/mnt/app_hdd/data/Data/Map/Homo_sapiens.GRCh37.87.chr.gtf")
          |val Gtfs = gtfLoader.loadDS
          |//Gtfs.cache
          |//Gtfs.count
          |
          |val pathwayLoader = new PathwayLoader(spark, "/mnt/app_hdd/data/Data/Pathway/c2.cp.v7.1.symbols.gmt")
          |val pathways = pathwayLoader.loadDS
          |//pathways.cache
          |//pathways.count
          |
          |val geneLoader = new GeneLoader(spark)
          |val genes = geneLoader.load("/mnt/app_hdd/data/Data/Map/All_human_genes")
          |
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
    "gene_name" -> StringType
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
    