package framework.examples.genomic

import framework.common._
import framework.nrc.MaterializeNRC

trait VCF extends MaterializeNRC {

  def loadVCF(path: String, shred: Boolean = false, skew: Boolean = false): String = {
    
    if (shred){
      val (variants, genotypes) = 
        if (skew) ("(variants, variants.empty)", "(genotypes, genotypes.empty)")
        else ("variants", "genotypes")
    
      s"""|val vloader = new VariantLoader(spark, "$path")
          |val (variants, genotypes) = vloader.shredDS
          |val IBag_Variants__D = $variants
          |//IBag_Variants__D.cache
          |//IBag_Variants__D.count
          |val IDict_Variants__D_genotypes = $genotypes
          |//IDict_Variants__D_genotypes.cache
          |//IDict_Variants__D_genotypes.count
          |""".stripMargin
    
    }else{
      val variants = if (!skew) s"""|val Variants = vloader.loadDS"""
        else s"""|val Variants_L = vloader.loadDS
                 |val Variants = (Variants_L, Variants_L.empty)
                 |"""
      s"""|val vloader = new VariantLoader(spark, "$path")
          $variants
          |//Variants.cache
          |//Variants.count
          |""".stripMargin
    }
  }

  val genoType = TupleType("g_sample" -> StringType, "call" -> IntType)
  val variantType = TupleType("contig" -> StringType, "start" -> IntType, 
    "reference" -> StringType, "alternate" -> StringType, "genotypes" -> BagType(genoType))
  
  val variants = BagVarRef("Variants", BagType(variantType))
  val vr = TupleVarRef("variant", variantType)
  val gr = TupleVarRef("genotype", genoType)

}

trait TGenomesMetadata extends MaterializeNRC {

  def loadTGenomes(path: String, shred: Boolean = false, skew: Boolean = true): String = {
    val metadata = if (skew)
        s"""|val Metadata_L = tgenomesLoader.load("$path")
            |val Metadata = (Metadata_L, Metadata_L.empty)"""
      else s"""|val Metadata = tgenomesLoader.load("$path")"""
    
    if (shred)
      s"""|val tgenomesLoader = new TGenomesLoader(spark)
          $metadata
          |val IBag_Metadata__D = Metadata
          |//IBag_Metadata__D.cache
          |//IBag_Metadata__D.count
          |""".stripMargin
    else
      s"""|val tgenomesLoader = new TGenomesLoader(spark)
          $metadata
          |//Metadata.cache
          |//Metadata.count
          |""".stripMargin

  }

  val metaType = TupleType("m_sample" -> StringType, "family_id" -> StringType, 
    "population" -> StringType, "gender" -> StringType)
  val metadata = BagVarRef("Metadata", BagType(metaType))
  val mr = TupleVarRef("metadata", metaType)

}


trait GeneLoader extends MaterializeNRC {

  def loadGene(path: String, shred: Boolean = false, skew: Boolean = true): String = {
    val genes = if (skew)
        s"""|val Gene_L = geneLoader.load("$path")
            |val Gene = (Gene_L, Gene_L.empty)"""
      else s"""|val Gene = geneLoader.load("$path")"""
    
    if (shred)
      s"""|val geneLoader = new GeneLoader(spark)
          $genes
          |val IBag_Gene__D = Gene
          |//IBag_Gene__D.cache
          |//IBag_Gene__D.count
          |""".stripMargin
    else
      s"""|val geneLoader = new GeneLoader(spark)
          $genes
          |//Gene.cache
          |//Gene.count
          |""".stripMargin

  }

  val geneType = TupleType("name" -> StringType,
    "description" -> StringType,
    "chrom" -> StringType,
    "g_type" -> StringType,
    "start_hg19" -> IntType,
    "end_hg19" -> IntType,
    "strand" -> StringType,
    "ts_id" -> StringType,
    "gene_type" -> StringType,
    "gene_status" -> StringType,
    "loci_level" -> IntType,
    "alias_symbol" -> StringType,
    "official_name" -> StringType)
  val genes = BagVarRef("Gene", BagType(geneType))
  val gene = TupleVarRef("gene", geneType)

}






